require "yaml"
require "log"

module Croupier
  alias TaskProc = -> String? | Array(String)

  # Length of the SHA1 prefix used for generated task ids (tasks with
  # outputs and no explicit id): long enough to avoid realistic
  # collisions, short enough to stay readable in logs
  ID_HASH_LENGTH = 12

  # A Task is an object that may generate output
  #
  # It has a `Proc` which is executed when the task is run
  # It can have zero or more inputs
  # It has zero or more outputs
  # Tasks are connected by dependencies, where one task's output is another's input
  class Task
    include YAML::Serializable
    include YAML::Serializable::Strict

    # Staleness values: unknown (compute on demand), stale or fresh.
    enum Staleness
      Unknown
      Stale
      Fresh
    end

    property id : String = ""
    # The task's inputs: files, task ids or kv:// keys it depends on.
    #
    # Don't mutate the returned set: runs read it without locks. Add
    # inputs with `TaskManager.add_input`, which locks, defers the
    # change during parallel waves and invalidates the graph cache.
    getter inputs : Set(String) = Set(String).new
    property outputs : Array(String) = [] of String
    # Tri-state staleness in one atomic field, safe to read from
    # parallel workers. stale, stale= and stale? are views over it.
    @[YAML::Field(ignore: true)]
    @staleness : Atomic(Staleness) = Atomic.new(Staleness::Unknown)
    property? always_run : Bool = false
    property? no_save : Bool = false
    @[YAML::Field(ignore: true)]
    property procs : Array(TaskProc) = [] of TaskProc
    property? mergeable : Bool = true
    property mutex : String? = nil

    # Setting a mutex also registers it with the manager, which is
    # where Task#run looks it up
    def mutex=(name : String?)
      @mutex = name
      TaskManager.add_mutex(name) if name
    end

    @[YAML::Field(ignore: true)]
    property? outputs_changed : Bool = false # Whether the last run changed any output

    # Under what keys should this task be registered with TaskManager
    def keys
      @outputs.empty? ? [@id] : @outputs
    end

    # Create a task with zero or more outputs.
    #
    # `outputs` is an array of files or k/v store keys that the task
    #   generates (the `output:` overloads take a single one)
    # `inputs` is an array of filesystem paths, task ids or k/v store
    #   keys that the task depends on
    # The block (or `proc:`) is executed when the task is run
    # `no_save` tells croupier that the task saves its outputs itself
    # `id` is a unique identifier for the task. If the task has no
    #   outputs, it *must* have an id. If not given, it's a hash of the
    #   outputs.
    # `always_run` makes the task stale regardless of its inputs
    # `mergeable`: if true, the task can be merged with others that
    #   share an output. Tasks with different `mergeable` values can
    #   NOT be merged together.
    # `mutex` names a lock held while the task's procs run, so tasks
    #   sharing it never run at the same time
    #
    # k/v store keys are of the form `kv://key`, and are used to store
    # intermediate data in a key/value store (in memory, or a file via
    # `TaskManager.use_persistent_store`).
    #
    # To access k/v data in your proc, use `TaskManager.get(key)`.
    #
    # Important: tasks are registered in TaskManager on creation, and
    # creating one while a run is in progress raises `UsageError`. If
    # the new task conflicts in id/outputs with others, it is merged
    # into the existing one and the new object is NOT registered, so
    # keeping references to Task objects you create is probably
    # pointless.
    def initialize(
      outputs : Array(String) = [] of String,
      inputs : Array(String) = [] of String,
      no_save : Bool = false,
      id : String? = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      mutex : String? = nil,
      &block : TaskProc
    )
      # Set before delegating: the inner initialize may merge this
      # task with others, and the merge checks mutex compatibility
      @mutex = mutex
      initialize(outputs, inputs, block, no_save, id, always_run, mergeable)
      TaskManager.add_mutex(mutex) if mutex
    end

    def initialize(
      outputs : Array(String) = [] of String,
      inputs : Array(String) = [] of String,
      proc : TaskProc? = nil,
      no_save : Bool = false,
      id : String? = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
    )
      # An empty kv:// key can never be satisfied (get("") on a store
      # that never holds it): better to fail at declaration
      raise TaskDefinitionError.new("Task has an empty kv:// key") if outputs.includes?("kv://") || inputs.includes?("kv://")

      inputs = normalize_paths(inputs)
      outputs = normalize_paths(outputs)

      overlap = inputs.to_set & outputs.to_set
      unless overlap.empty?
        raise CycleError.new("Cycle detected: #{overlap.to_a.sort.join(", ")} is both an input and an output of the task")
      end
      @always_run = always_run
      @procs << proc unless proc.nil?
      @outputs = outputs.uniq
      raise TaskDefinitionError.new("Task has no outputs and no id") if id.nil? && @outputs.empty?
      @id = id ? id : Digest::SHA1.hexdigest(@outputs.join(","))[0, ID_HASH_LENGTH]
      @inputs = Set.new inputs
      @no_save = no_save
      @mergeable = mergeable

      # Raises if a run is in progress (see TaskManager.register_task)
      TaskManager.register_task(self, id)
    end

    # Register this task in the TaskManager: merge every task it has
    # an output/id collision with into one, and register the survivor
    # on every output/id of the merged set. Called by
    # TaskManager.register_task. Not part of the public API.
    # :nodoc:
    def register_with_manager(explicit_id : String?) : Nil
      to_merge = colliding_tasks
      raise TaskDefinitionError.new("Can't merge task #{self} with #{to_merge[..-2].map(&.to_s)}") \
        if to_merge.size > 1 && to_merge.any? { |t| !t.mergeable? }
      check_explicit_id_conflict(explicit_id, to_merge)
      check_merge_flag_compatibility(to_merge)
      register_merged(to_merge)

      TaskManager.invalidate_graph_cache
    end

    # kv:// entries keep their prefix; everything else is a path and
    # gets normalized, so "./x", "dir/../x" and "x" are the same
    # graph vertex (and match the watcher's normalized event paths)
    private def normalize_paths(paths : Array(String)) : Array(String)
      paths.map { |path| path.starts_with?("kv://") ? path : Path[path].normalize.to_s }
    end

    # Every registered task this one collides with (by id or output),
    # plus this task itself (the merge set always includes it).
    private def colliding_tasks : Array(Task)
      fetched = (keys.map { |k|
        TaskManager.tasks.fetch(k, nil)
      }).select(Task).uniq!
      fetched << self
      fetched
    end

    # An explicit id on a task with outputs must be unique among tasks
    # that stay separate: the id index assumes one task per id.
    # (Output-less tasks may still merge under a shared id, and a
    # collision with a merge target is fine: one task, one id.)
    private def check_explicit_id_conflict(id : String?, to_merge : Array(Task))
      return if id.nil? || @outputs.empty?
      conflict = TaskManager.tasks_by_id[id]?
      return if conflict.nil? || to_merge.includes?(conflict)
      raise TaskDefinitionError.new("Task id #{id} is already used by #{conflict}")
    end

    # Check flag compatibility across the WHOLE set before the first
    # merge: merge mutates the first task in place, so a reduce that
    # failed partway (3+ colliding tasks) would leave earlier merges
    # applied. Same checks and messages as Task#merge.
    private def check_merge_flag_compatibility(to_merge : Array(Task))
      return unless to_merge.size > 1
      first = to_merge.first
      to_merge.each do |task|
        raise TaskDefinitionError.new("Cannot merge tasks with different no_save settings") unless task.no_save? == first.no_save?
        raise TaskDefinitionError.new("Cannot merge tasks with different always_run settings") unless task.always_run? == first.always_run?
        raise TaskDefinitionError.new("Cannot merge tasks with different mutexes") unless task.mutex == first.mutex
      end
    end

    private def register_merged(to_merge : Array(Task))
      reduced = to_merge.reduce { |t1, t2| t1.merge t2 }
      reduced.keys.each { |k| TaskManager.tasks[k] = reduced }
      # Keep the id index in step: absorbed tasks leave the registry,
      # so their index entries go too, or a later task reusing such an
      # id would falsely conflict
      to_merge.each { |t| TaskManager.tasks_by_id.delete(t.id) unless t == reduced }
      TaskManager.tasks_by_id[reduced.id] = reduced
    end

    def initialize(
      output : String? = nil,
      inputs : Array(String) = [] of String,
      no_save : Bool = false,
      id : String? = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      mutex : String? = nil,
      &block : TaskProc
    )
      initialize(
        output ? [output] : [] of String,
        inputs, no_save, id, always_run, mergeable, mutex,
        &block
      )
    end

    # Create a task with zero or one outputs. Overload for convenience.
    def initialize(
      output : String? = nil,
      inputs : Array(String) = [] of String,
      proc : TaskProc? = nil,
      no_save : Bool = false,
      id : String? = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
    )
      initialize(
        outputs: output ? [output] : [] of String,
        inputs: inputs,
        proc: proc,
        no_save: no_save,
        id: id,
        always_run: always_run,
        mergeable: mergeable
      )
    end

    # Executes the proc for the task
    def run
      call_results = call_procs

      # Set by the save/verify steps below; read by early cutoff
      @outputs_changed = false

      if @no_save
        verify_no_save_outputs
      else
        save_outputs(call_results)
      end
      self.stale = false
      TaskManager.progress_callback.call(id)
    end

    # Run every proc, locking the task's mutex (if any) around each
    # call, and collect their results.
    private def call_procs : Array(String?)
      call_results = Array(String?).new
      @procs.each do |proc|
        Fiber.yield
        mtx = mutex
        result = nil
        begin
          TaskManager.lock_mutex(mtx) unless mtx.nil?
          result = proc.call
        rescue ex
          raise TaskFailure.new("Task #{self} failed: #{ex}", cause: ex)
        ensure
          TaskManager.unlock_mutex(mtx) unless mtx.nil?
        end
        if result.nil?
          call_results << nil
        elsif result.is_a?(String)
          call_results << result
        else
          call_results.concat(result)
        end
      end
      call_results
    end

    # no_save tasks write their own outputs: check they exist and
    # record their hashes
    private def verify_no_save_outputs
      @outputs.reject(&.empty?).each do |output|
        # The task sets kv:// outputs itself; nothing to check
        next if output.lchop?("kv://")
        if !File.exists?(output)
          raise TaskVerificationError.new("Task #{self} did not generate #{output}")
        end
        # A directory output gets the same Merkle-tree digest the
        # input scanner uses, so a dependent consuming it as an input
        # compares matching hashes and stays fresh across runs
        new_hash = File.directory?(output) ? TaskManager.hash_directory(output) : Croupier.hash_file(output)
        old_hash = TaskManager.swap_output_hash(output, new_hash)
        @outputs_changed = true if old_hash != new_hash
      end
    end

    # Save the procs' results to the task's outputs, in order
    private def save_outputs(call_results : Array(String?))
      if call_results.size > @outputs.size
        Log.warn { "Task #{self} returned #{call_results.size} results for #{@outputs.size} outputs, discarding the extras" }
      end
      @outputs.zip(call_results) do |output, call_result|
        raise TaskVerificationError.new("Task #{self} did not return any data for output #{output}") if call_result.nil?
        if k = output.lchop?("kv://")
          save_kv_output(k, output, call_result)
        else
          save_file_output(output, call_result)
        end
      end
    rescue IndexError
      raise TaskVerificationError.new("Task #{self} did not return the correct number of outputs")
    end

    # kv:// outputs go to the k/v store; `set` reports whether the
    # value changed, and the value's hash is recorded like a file's
    private def save_kv_output(key : String, output : String, call_result : String)
      @outputs_changed = true if TaskManager.set(key, call_result)
      TaskManager.record_output_hash(output, Digest::SHA1.hexdigest(call_result))
    end

    private def save_file_output(output : String, call_result : String)
      Dir.mkdir_p(File.dirname output)
      File.open(output, "w") do |io|
        io << call_result
      end
      new_hash = Digest::SHA1.hexdigest(call_result)
      old_hash = TaskManager.swap_output_hash(output, new_hash)
      if old_hash != new_hash
        @outputs_changed = true
      else
        Log.debug { "Task #{id} output #{output} unchanged (old=#{old_hash.inspect}, new=#{new_hash.inspect})" }
      end
    end

    # A task is stale if:
    #
    # * it is always_run, or has no inputs
    # * one of its outputs is missing
    # * one of its inputs was modified
    # * one of its inputs is produced by a stale task
    #
    # Staleness is tri-state: unknown, stale, fresh.
    # TaskManager.propagate_staleness sets it for every task before a
    # run, and running a task sets it to fresh. This method trusts an
    # assigned value (dependents rely on a finished task reporting
    # fresh even when it is always_run) and computes only while it is
    # unknown.
    def stale? : Bool
      case @staleness.get
      when Staleness::Stale then true
      when Staleness::Fresh then false
      else
        # Unknown: compute on demand
        return true if @always_run || @inputs.empty?

        computed = compute_staleness
        @staleness.set(computed ? Staleness::Stale : Staleness::Fresh)
        computed
      end
    end

    # Tri-state staleness property: nil=unknown, true=stale, false=fresh.
    def stale : Bool?
      return true if @staleness.get.stale?
      return false if @staleness.get.fresh?
      nil # Unknown
    end

    def stale=(value : Bool?)
      @staleness.set(
        case value
        when nil  then Staleness::Unknown
        when true then Staleness::Stale
        else           Staleness::Fresh
        end
      )
    end

    # Early cutoff: `input` turned out unchanged, so recompute
    # staleness from all inputs (another may still be stale).
    def mark_dependency_fresh(input : String)
      self.stale = compute_staleness(inputless_is_stale: true)
    end

    # Stale unless every output exists (as a file or as a k/v key) and
    # no input is modified or produced by a stale task. Shared by
    # stale? and mark_dependency_fresh; `inputless_is_stale` makes
    # always_run and input-less tasks stale (stale? checks that
    # itself first).
    private def compute_staleness(inputless_is_stale : Bool = false) : Bool
      return true if inputless_is_stale && (@always_run || @inputs.empty?)

      return true if @outputs.any? do |output|
                       if key = output.lchop? "kv://"
                         !TaskManager.get(key)
                       else
                         !File.exists?(output)
                       end
                     end

      return true if @inputs.any? { |input| TaskManager.modified?(input) }

      @inputs.any? do |input|
        task = TaskManager.tasks[input]?
        task && task.stale?
      end
    end

    # Is this input satisfied: a fresh task, an existing file, or a
    # key present in the k/v store?
    private def input_satisfied?(input) : Bool
      if task = TaskManager.tasks[input]?
        !task.stale?
      elsif key = input.lchop? "kv://"
        !TaskManager.get(key).nil?
      else
        TaskManager.file_exists?(input)
      end
    end

    # All inputs that are not satisfied yet
    def waiting_for
      @inputs.reject { |input| input_satisfied?(input) }
    end

    # Early-exit version of `waiting_for.empty?`, used by ready?
    def waiting? : Bool
      @inputs.any? { |input| !input_satisfied?(input) }
    end

    # A task is ready if it needs to run (stale, always_run or
    # run_all) and is not waiting for any input
    def ready?(run_all = false)
      (stale? || always_run? || run_all) &&
        !waiting?
    end

    def to_s(io)
      io << @id << "::" << @outputs.join(", ")
    end

    # Merge two tasks: inputs and outputs are joined, and the second
    # task's procs are appended to the first's.
    def merge(other : Task)
      raise TaskDefinitionError.new("Cannot merge tasks with different no_save settings") unless no_save? == other.no_save?
      raise TaskDefinitionError.new("Cannot merge tasks with different always_run settings") unless always_run? == other.always_run?
      # A merged task runs all procs under one mutex, so keeping only
      # one side's would break the other's mutual exclusion
      raise TaskDefinitionError.new("Cannot merge tasks with different mutexes") unless mutex == other.mutex

      # @outputs may hold duplicates: several procs can write the
      # same output
      @outputs += other.@outputs
      @inputs += other.@inputs
      @procs += other.@procs
      self
    end
  end
end
