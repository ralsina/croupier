require "yaml"

module Croupier
  alias TaskProc = -> String? | Array(String)

  # A Task is an object that may generate output
  #
  # It has a `Proc` which is executed when the task is run
  # It can have zero or more inputs
  # It has zero or more outputs
  # Tasks are connected by dependencies, where one task's output is another's input
  class Task
    include YAML::Serializable
    include YAML::Serializable::Strict

    property id : String = ""
    property inputs : Set(String) = Set(String).new
    property outputs : Array(String) = [] of String
    @[YAML::Field(ignore: true)]
    property stale : Bool | Nil = nil # nil=unknown, true=stale, false=fresh
    property? always_run : Bool = false
    property? no_save : Bool = false
    @[YAML::Field(ignore: true)]
    property procs : Array(TaskProc) = [] of TaskProc
    property? mergeable : Bool = true
    property mutex : String? = nil

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
      &block : TaskProc
    )
      initialize(outputs, inputs, block, no_save, id, always_run, mergeable)
      TaskManager.add_mutex(mutex, self) if mutex
    end

    def initialize(
      outputs : Array(String) = [] of String,
      inputs : Array(String) = [] of String,
      proc : TaskProc | Nil = nil,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
    )
      if !(inputs.to_set & outputs.to_set).empty?
        raise "Cycle detected"
      end
      @always_run = always_run
      @procs << proc unless proc.nil?
      @outputs = outputs.uniq
      raise "Task has no outputs and no id" if id.nil? && @outputs.empty?
      @id = id ? id : Digest::SHA1.hexdigest(@outputs.join(","))[..6]
      @inputs = Set.new inputs
      @no_save = no_save
      @mergeable = mergeable

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
      reduced = to_merge.reduce { |t1, t2| t1.merge t2 }
      reduced.keys.each { |k| TaskManager.tasks[k] = reduced }
    end

    def initialize(
      output : String | Nil = nil,
      inputs : Array(String) = [] of String,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true,
      &block : TaskProc
    )
      initialize(output, inputs, block, no_save, id, always_run, mergeable)
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
    def run # ameba:disable Metrics/CyclomaticComplexity
      call_results = Array(String | Nil).new
      @procs.each do |proc|
        Fiber.yield
        begin
          TaskManager.lock_mutex(mutex.as(String)) unless mutex.nil?
          result = proc.call
        rescue ex
          raise "Task #{self} failed: #{ex}"
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

      if @no_save
        # The task saved the data so we should not do it
        # but we need to update hashes
        @outputs.reject(&.empty?).each do |output|
          # If the output is a kv:// url, we don't need to check if it exists
          next if output.lchop?("kv://")
          if !File.exists?(output)
            raise "Task #{self} did not generate #{output}"
          end
          TaskManager.next_run[output] = Digest::SHA1.hexdigest(File.read(output))
        end
      else
        # We have to save the files ourselves
        begin
          @outputs.zip(call_results) do |output, call_result|
            raise "Task #{self} did not return any data for output #{output}" if call_result.nil?
            if k = output.lchop?("kv://")
              # If the output is a kv:// url, we save it in the k/v store
              TaskManager.set(k, call_result)
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
              TaskManager.next_run[output] = Digest::SHA1.hexdigest(call_result)
            end
          end
        rescue IndexError
          raise "Task #{self} did not return the correct number of outputs"
        end
      end
      @stale = false # Done, not stale anymore
      TaskManager.progress_callback.call(id)
    end

    # Tasks are stale if:
    #
    # * One of their inputs are stale
    # * If one of the output files doesn't exist
    # * If any of the inputs are generated by a stale task
    #
    # Staleness is tri-state: nil=unknown (compute on-demand), true=stale, false=fresh.
    # TaskManager.propagate_staleness pre-computes this for O(V+E) performance.
    # This method computes on-demand if not yet computed.

    def stale?
      # Tasks without inputs or flagged always_run are always stale
      return true if @always_run || @inputs.empty?

      return @stale.as(Bool) unless @stale.nil?

      # Compute on-demand for backward compatibility with tests
      # that call stale? before run_tasks
      @stale = compute_staleness
    end

    # Internal method to compute staleness on-demand
    private def compute_staleness
      file_outputs = @outputs.reject(&.lchop?("kv://"))
      kv_outputs = @outputs.select(&.lchop?("kv://")).map(&.lchop("kv://"))

      # Check if outputs are missing
      missing_outputs = file_outputs.any? { |output| !File.exists?(output) } ||
                        kv_outputs.any? { |output| !TaskManager.get(output) }
      return true if missing_outputs

      # Check if inputs are modified
      modified_inputs = @inputs.any? { |input| TaskManager.modified.includes?(input) }
      return true if modified_inputs

      # Check if any input tasks are stale
      stale_inputs = @inputs.any? do |input|
        TaskManager.tasks.has_key?(input) && TaskManager.tasks[input].stale?
      end
      stale_inputs
    end

    # For inputs that are tasks, we check if they are stale
    # For inputs that are not tasks, they should exist as files
    # If any inputs don't fit those criteria, they are being
    # waited for.
    def waiting_for
      @inputs.reject do |input|
        if TaskManager.tasks.has_key? input
          !TaskManager.tasks[input].stale?
        else
          if input.lchop? "kv://"
            !TaskManager.@_store.get(input.lchop("kv://")).nil?
          else
            File.exists? input
          end
        end
      end
    end

    # A task is ready if it is stale and not waiting for anything
    def ready?(run_all = false)
      (stale? || always_run? || run_all) &&
        waiting_for.empty?
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

      # @outputs is NOT unique! We can save multiple times
      # the same file in multiple procs
      @outputs += other.@outputs
      @inputs += other.@inputs
      @procs += other.@procs
      self
    end
  end
end
