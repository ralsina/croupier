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
    @inputs : Set(String) = Set(String).new

    # The task's inputs: files, task ids or kv:// keys it depends on.
    # Adding to it calls `TaskManager.add_input`.
    def inputs : Inputs
      Inputs.new(self)
    end

    # A read-only view of a task's inputs. Runs read input sets without
    # locks, so additions go through `TaskManager.add_input`, which
    # locks and defers them until the run ends; there's no removal.
    struct Inputs
      include Enumerable(String)

      def initialize(@task : Task)
      end

      def each(& : String ->) : Nil
        @task.@inputs.each { |input| yield input }
      end

      def includes?(input : String) : Bool
        @task.@inputs.includes?(input)
      end

      def size : Int32
        @task.@inputs.size
      end

      def empty? : Bool
        @task.@inputs.empty?
      end

      # Same as `TaskManager.add_input` on this task
      def <<(input : String) : self
        add(input)
        self
      end

      # Same as `TaskManager.add_input` on this task: false if the
      # task already had the input
      def add(input : String) : Bool
        TaskManager.add_input(@task.keys.first, input)
      end

      def inspect(io : IO) : Nil
        @task.@inputs.inspect(io)
      end

      def to_s(io : IO) : Nil
        @task.@inputs.to_s(io)
      end
    end

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
    # A task producing one of this task's inputs changed its outputs
    # in the current run. compute_staleness can't see that: the
    # producer is fresh once it ran, and the input scan predates the
    # change. Set and cleared by the runner.
    @[YAML::Field(ignore: true)]
    property? input_changed : Bool = false

    # Under what keys should this task be registered with TaskManager
    def keys
      @outputs.empty? ? [@id] : @outputs
    end

    # Create a task with zero or more outputs.
    #
    # `outputs` is an array of files or k/v store keys that the task
    #   generates. A single output can be passed as a string, or by
    #   name as `output:`.
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
      outputs : Array(String) | String | Nil = nil,
      inputs : Array(String) = [] of String,
      no_save : Bool = false,
      id : String? = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      mutex : String? = nil,
      output : String? = nil,
      &block : TaskProc
    )
      setup(output_list(outputs, output), inputs, block, no_save, id, always_run, mergeable, mutex)
    end

    def initialize(
      outputs : Array(String) | String | Nil = nil,
      inputs : Array(String) = [] of String,
      proc : TaskProc? = nil,
      no_save : Bool = false,
      id : String? = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      mutex : String? = nil,
      output : String? = nil,
    )
      setup(output_list(outputs, output), inputs, proc, no_save, id, always_run, mergeable, mutex)
    end

    # `outputs` (an array, a single string or nil) and the `output:`
    # alias, as one array
    private def output_list(outputs : Array(String) | String | Nil, output : String?) : Array(String)
      raise TaskDefinitionError.new("Pass either output or outputs, not both") if outputs && output
      case outputs
      when Array(String) then outputs
      when String        then [outputs]
      else                    output ? [output] : [] of String
      end
    end

    private def setup(
      outputs : Array(String),
      inputs : Array(String),
      proc : TaskProc?,
      no_save : Bool,
      id : String?,
      always_run : Bool,
      mergeable : Bool,
      mutex : String?,
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
      # Set before registering: registration may merge this task with
      # others, and the merge checks mutex compatibility
      @mutex = mutex

      # Raises if a run is in progress (see TaskManager.register_task)
      TaskManager.register_task(self, id)
      TaskManager.add_mutex(mutex) if mutex
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
      conflict = TaskManager.tasks.by_id?(id)
      return if conflict.nil? || to_merge.includes?(conflict)
      raise TaskDefinitionError.new("Task id #{id} is already used by #{conflict}")
    end

    # Check flag compatibility across the WHOLE set before the first
    # merge: merge mutates the first task in place, so a reduce that
    # failed partway (3+ colliding tasks) would leave earlier merges
    # applied.
    private def check_merge_flag_compatibility(to_merge : Array(Task))
      first = to_merge.first
      to_merge.each do |task|
        if conflict = first.merge_conflict(task)
          raise TaskDefinitionError.new(conflict)
        end
      end
    end

    # Why `self` and `other` can't be merged, or nil if they can. A
    # merged task runs all procs under one mutex, so keeping only one
    # side's would break the other's mutual exclusion.
    protected def merge_conflict(other : Task) : String?
      return "Cannot merge tasks with different no_save settings" unless no_save? == other.no_save?
      return "Cannot merge tasks with different always_run settings" unless always_run? == other.always_run?
      "Cannot merge tasks with different mutexes" unless mutex == other.mutex
    end

    private def register_merged(to_merge : Array(Task))
      reduced = to_merge.reduce { |t1, t2| t1.merge t2 }
      # Absorbed tasks leave the registry, ids included, or a later
      # task reusing such an id would falsely conflict
      to_merge.each { |t| TaskManager.tasks.remove(t) unless t == reduced }
      TaskManager.tasks.put(reduced)
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
      TaskManager.swap_output_hash(output, Digest::SHA1.hexdigest(call_result))
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

    # Early cutoff: one of the task's inputs turned out unchanged, so
    # recompute staleness from all of them (another may still be
    # stale, or may already have changed in this run).
    def recompute_staleness : Nil
      self.stale = @input_changed || compute_staleness
    end

    # Whether the task is stale on its own account, regardless of the
    # tasks producing its inputs: it is always_run or has no inputs,
    # an output is missing (as a file or a k/v key), or an input was
    # modified. TaskManager.propagate_staleness starts from these.
    def stale_on_own? : Bool
      return true if @always_run || @inputs.empty?

      return true if @outputs.any? do |output|
                       if key = output.lchop? "kv://"
                         !TaskManager.get(key)
                       else
                         !File.exists?(output)
                       end
                     end

      @inputs.any? { |input| TaskManager.modified?(input) }
    end

    # Stale on its own account, or because an input is produced by a
    # stale task.
    private def compute_staleness : Bool
      stale_on_own? || @inputs.any? do |input|
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
        File.exists?(input)
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

    # A task is ready if it needs to run (stale, or run_all) and is
    # not waiting for any input. always_run tasks are stale until they
    # run.
    def ready?(run_all = false)
      (stale? || run_all) && !waiting?
    end

    def to_s(io)
      io << @id << "::" << @outputs.join(", ")
    end

    # Merge two tasks: inputs and outputs are joined, and the second
    # task's procs are appended to the first's.
    def merge(other : Task)
      if conflict = merge_conflict(other)
        raise TaskDefinitionError.new(conflict)
      end

      # @outputs may hold duplicates: several procs can write the
      # same output
      @outputs += other.@outputs
      @inputs += other.@inputs
      @procs += other.@procs
      self
    end
  end
end
