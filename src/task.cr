require "yaml"
require "log"

module Croupier
  alias TaskProc = -> String? | Array(String)

  # Error raised when a task's proc raises: it carries the task context
  # in its message and keeps the original exception (with its backtrace)
  # available as `#cause`.
  class TaskFailure < Exception
  end

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
    # Treat as read-only while tasks are running: mutating it from task
    # procs on parallel workers races and, even done safely, cannot
    # affect the current run (wave planning happens before workers
    # start). Use `TaskManager.add_input`, which is guarded and
    # invalidates the caches a later run needs.
    property inputs : Set(String) = Set(String).new
    property outputs : Array(String) = [] of String
    # Tri-state staleness in a single atomic field, so reads from
    # parallel workers are safe without extra locking. The stale,
    # stale= and stale? methods below are views over this field.
    @[YAML::Field(ignore: true)]
    @staleness : Atomic(Staleness) = Atomic.new(Staleness::Unknown)
    property? always_run : Bool = false
    property? no_save : Bool = false
    @[YAML::Field(ignore: true)]
    property procs : Array(TaskProc) = [] of TaskProc
    property? mergeable : Bool = true
    property mutex : String? = nil
    property? master_task : Bool = false
    @[YAML::Field(ignore: true)]
    property subtask_ids : Set(String) = Set(String).new
    @[YAML::Field(ignore: true)]
    property? outputs_changed : Bool = false # Track if outputs actually changed during run

    # Under what keys should this task be registered with TaskManager
    def keys
      @outputs.empty? ? [@id] : @outputs
    end

    # Create a task with zero or more outputs.
    #
    # `output` is an array of files or k/v store keys that the task generates
    # `inputs` is an array of filesystem paths, task ids or k/v store keys that the
    # task depends on.
    # `proc` is a proc that is executed when the task is run
    # `no_save` is a boolean that tells croupier that the task will save the files itself
    # `id` is a unique identifier for the task. If the task has no outputs,
    # it *must* have an id. If not given, it's calculated as a hash of outputs.
    # `always_run` is a boolean that tells croupier that the task is always
    #   stale regardless of its dependencies' state
    # `mergeable` is a boolean. If true, the task can be merged
    #   with others that share an output. Tasks with different
    #   `mergeable` values can NOT be merged together.
    #
    # k/v store keys are of the form `kv://key`, and are used to store
    # intermediate data in a key/value store. They are not saved to disk.
    #
    # To access k/v data in your proc, you can use `TaskManager.store.get(key)`.
    #
    # Important: tasks will be registered in TaskManager. If the new task
    # conflicts in id/outputs with others, it will be merged, and the new
    # object will NOT be registered. For that reason, keeping references
    # to Task objects you create is probably pointless.

    def initialize(
      outputs : Array(String) = [] of String,
      inputs : Array(String) = [] of String,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      mutex : String? = nil,
      master_task : Bool = false,
      &block : TaskProc
    )
      initialize(outputs, inputs, block, no_save, id, always_run, mergeable, master_task)
      TaskManager.add_mutex(mutex) if mutex
    end

    def initialize( # ameba:disable Metrics/CyclomaticComplexity
outputs : Array(String) = [] of String,
                   inputs : Array(String) = [] of String,
                   proc : TaskProc | Nil = nil,
                   no_save : Bool = false,
                   id : String | Nil = nil,
                   always_run : Bool = false,
                   mergeable : Bool = true,
                   master_task : Bool = false,)
      # An empty kv:// key can never be satisfied (get("") on a store
      # that never holds it): better to fail at declaration
      raise "Task has an empty kv:// key" if outputs.includes?("kv://") || inputs.includes?("kv://")

      # kv:// entries keep their prefix; everything else is a path and
      # gets normalized, so "./x", "dir/../x" and "x" are the same
      # graph vertex (and match the watcher's normalized event paths)
      inputs = inputs.map { |path| path.starts_with?("kv://") ? path : Path[path].normalize.to_s }
      outputs = outputs.map { |path| path.starts_with?("kv://") ? path : Path[path].normalize.to_s }

      if !(inputs.to_set & outputs.to_set).empty?
        raise "Cycle detected"
      end
      @always_run = always_run
      @procs << proc unless proc.nil?
      @outputs = outputs.uniq
      raise "Task has no outputs and no id" if id.nil? && @outputs.empty?
      @id = id ? id : Digest::SHA1.hexdigest(@outputs.join(","))[0, 12]
      @inputs = Set.new inputs
      @no_save = no_save
      @mergeable = mergeable
      @master_task = master_task

      # Register with the task manager.
      # We should merge every task we have output/id collision with
      # into one, and register it on every output/id of every one
      # of those tasks
      to_merge = (keys.map { |k|
        TaskManager.tasks.fetch(k, nil)
      }).select(Task).uniq!
      to_merge << self
      # Refuse to merge if this task or any of the colliding ones
      # are not mergeable
      raise "Can't merge task #{self} with #{to_merge[..-2].map(&.to_s)}" \
        if to_merge.size > 1 && to_merge.any? { |t| !t.mergeable? }
      # An explicit id on an output-ful task must be unique among tasks
      # that stay separate: subtask tracking matches tasks BY id, so a
      # duplicate would make remove_subtasks delete unrelated tasks.
      # (Output-less tasks may still merge under a shared id, and a
      # collision with a merge target is fine: one task, one id.)
      if id && !@outputs.empty?
        if conflict = TaskManager.tasks.values.find { |t| t.id == id }
          unless to_merge.includes?(conflict)
            raise "Task id #{id} is already used by #{conflict}"
          end
        end
      end
      # Check flag compatibility across the WHOLE set before the first
      # merge: merge mutates the live first task in place, so a reduce
      # that fails partway (3+ colliding tasks) would leave the earlier
      # merges applied and the registry corrupted. Same checks and
      # messages as Task#merge, which still re-checks pairwise.
      if to_merge.size > 1
        first = to_merge.first
        to_merge.each do |task|
          raise "Cannot merge tasks with different no_save settings" unless task.no_save? == first.no_save?
          raise "Cannot merge tasks with different always_run settings" unless task.always_run? == first.always_run?
          raise "Cannot merge master task with non-master task" unless task.master_task? == first.master_task?
        end
      end
      reduced = to_merge.reduce { |t1, t2| t1.merge t2 }
      reduced.keys.each { |k| TaskManager.tasks[k] = reduced }

      # Invalidate graph cache since we added/modified a task
      TaskManager.invalidate_graph_cache
    end

    def initialize(
      output : String | Nil = nil,
      inputs : Array(String) = [] of String,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      master_task : Bool = false,
      &block : TaskProc
    )
      initialize(output, inputs, block, no_save, id, always_run, mergeable, master_task)
    end

    # Create a task with zero or one outputs. Overload for convenience.
    def initialize(
      output : String | Nil = nil,
      inputs : Array(String) = [] of String,
      proc : TaskProc | Nil = nil,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      master_task : Bool = false,
    )
      initialize(
        outputs: output ? [output] : [] of String,
        inputs: inputs,
        proc: proc,
        no_save: no_save,
        id: id,
        always_run: always_run,
        mergeable: mergeable,
        master_task: master_task
      )
    end

    # Executes the proc for the task
    def run # ameba:disable Metrics/CyclomaticComplexity
      call_results = Array(String | Nil).new
      @procs.each do |proc|
        Fiber.yield
        begin
          TaskManager.lock_mutex(mutex.as(String)) unless mutex.nil?
          result = proc.call
        rescue ex
          raise TaskFailure.new("Task #{self} failed: #{ex}", cause: ex)
        ensure
          TaskManager.unlock_mutex(mutex.as(String)) unless mutex.nil?
        end
        if result.nil?
          call_results << nil
        elsif result.is_a?(String)
          call_results << result
        else
          call_results += result.as(Array(String))
        end
      end

      # Track if any output changed (for early cutoff optimization)
      @outputs_changed = false

      if @no_save
        # The task saved the data so we should not do it
        # but we need to update hashes
        @outputs.reject(&.empty?).each do |output|
          # If the output is a kv:// url, we don't need to check if it exists
          next if output.lchop?("kv://")
          if !File.exists?(output)
            raise "Task #{self} did not generate #{output}"
          end
          new_hash = Digest::SHA1.hexdigest(File.read(output))
          old_hash = TaskManager.swap_output_hash(output, new_hash)
          @outputs_changed = true if old_hash != new_hash
        end
      else
        # We have to save the files ourselves
        begin
          if call_results.size > @outputs.size
            Log.warn { "Task #{self} returned #{call_results.size} results for #{@outputs.size} outputs, discarding the extras" }
          end
          @outputs.zip(call_results) do |output, call_result|
            raise "Task #{self} did not return any data for output #{output}" if call_result.nil?
            if k = output.lchop?("kv://")
              # If the output is a kv:// url, we save it in the k/v
              # store; set reports whether the value actually changed,
              # and the value's hash is recorded for the next run's
              # state file exactly like a file output's
              @outputs_changed = true if TaskManager.set(k, call_result)
              TaskManager.record_output_hash(output, Digest::SHA1.hexdigest(call_result))
            else
              begin
                Dir.mkdir_p(File.dirname output)
              rescue ex : Exception
                # This fails because the directory already exists.
                # If there is a real problem creating it (such as permissions)
                # then the File.open below will fail and we'll catch it there.
              end
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
          end
        rescue IndexError
          raise "Task #{self} did not return the correct number of outputs"
        end
      end
      self.stale = false # Done, not stale anymore (staleness is a single atomic field)
      TaskManager.progress_callback.call(id)
    end

    # Tasks are stale if:
    #
    # * One of their inputs are stale
    # * If one of the output files doesn't exist
    # * If any of the inputs are generated by a stale task
    #
    # Staleness is tri-state: unknown (compute on-demand), stale, fresh.
    # TaskManager.propagate_staleness pre-computes it for O(V+E)
    # performance, and running a task sets it to fresh. This method
    # trusts that assigned value — dependents (waiting_for) rely on a
    # finished task reporting fresh even when it has no inputs or is
    # always_run — and computes on-demand only while it is unknown.

    def stale? : Bool
      case @staleness.get
      when Staleness::Stale then true
      when Staleness::Fresh then false
      else
        # Unknown: compute on demand. Tasks without inputs or flagged
        # always_run are always stale.
        return true if @always_run || @inputs.empty?

        computed = compute_staleness
        @staleness.set(computed ? Staleness::Stale : Staleness::Fresh)
        computed
      end
    end

    # Tri-state staleness property: nil=unknown, true=stale, false=fresh.
    def stale : Bool | Nil
      case @staleness.get
      when Staleness::Stale then true
      when Staleness::Fresh then false
      else                       nil
      end
    end

    def stale=(value : Bool | Nil)
      @staleness.set(
        case value
        when nil  then Staleness::Unknown
        when true then Staleness::Stale
        else           Staleness::Fresh
        end
      )
    end

    # Mark that a dependency (input) is known to be unchanged.
    # Recomputes staleness considering ALL inputs together (thread-safe).
    def mark_dependency_fresh(input : String)
      self.stale = compute_staleness(inputless_is_stale: true)
    end

    # Compute staleness by checking that every output exists (as a file
    # or as a k/v key) and no input is modified or produced by a stale
    # task.
    #
    # Single shared implementation for the on-demand path (stale?) and
    # the early-cutoff recompute (mark_dependency_fresh); they differ
    # only in whether input-less / always_run tasks short-circuit to
    # stale, which is the `inputless_is_stale` flag (stale? checks that
    # itself before descending). The output scan is one early-exit pass
    # instead of separate file/kv partitions, and stops at the first
    # missing output.
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

    # For inputs that are tasks, we check if they are stale
    # For inputs that are not tasks, they should exist as files
    # or as keys in the k/v store
    # If any inputs don't fit those criteria, they are being
    # waited for.

    # Is this input satisfied (a fresh task, an existing file, or a
    # key present in the k/v store)?
    #
    # The store is read through TaskManager.get so the @data_mutex
    # guards against parallel workers writing it from other threads.
    private def input_satisfied?(input) : Bool
      if task = TaskManager.tasks[input]?
        !task.stale?
      elsif key = input.lchop? "kv://"
        !TaskManager.get(key).nil?
      else
        TaskManager.file_exists?(input)
      end
    end

    # All inputs that are not satisfied yet.
    def waiting_for
      @inputs.reject { |input| input_satisfied?(input) }
    end

    # Early-exit version of waiting_for.empty? used by ready?, so
    # readiness checks stop at the first blocked input instead of
    # building the whole array.
    def waiting? : Bool
      @inputs.any? { |input| !input_satisfied?(input) }
    end

    # A task is ready if it is stale and not waiting for anything
    def ready?(run_all = false)
      (stale? || always_run? || run_all) &&
        !waiting?
    end

    def to_s(io)
      io << @id << "::" << @outputs.join(", ")
    end

    # Merge two tasks.
    #
    # inputs and outputs are joined
    # procs of the second task are added to the 1st
    def merge(other : Task)
      raise "Cannot merge tasks with different no_save settings" unless no_save? == other.no_save?
      raise "Cannot merge tasks with different always_run settings" unless always_run? == other.always_run?
      raise "Cannot merge master task with non-master task" unless master_task? == other.master_task?

      # @outputs is NOT unique! We can save multiple times
      # the same file in multiple procs
      @outputs += other.@outputs
      @inputs += other.@inputs
      @procs += other.@procs
      self
    end
  end
end
