# Croupier describes a task graph and lets you operate on them
require "./topo_sort"
require "crystalline"
require "digest/sha1"
require "inotify"
require "kiwi/file_store"
require "kiwi/memory_store"
require "log"
require "yaml"

module Croupier
  VERSION = "0.4.1"

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
    property stale : Bool = true # ameba:disable Style/QueryBoolMethods
    property? always_run : Bool = false
    property? no_save : Bool = false
    @[YAML::Field(ignore: true)]
    property procs : Array(TaskProc) = [] of TaskProc
    property? mergeable : Bool = true

    # Under what keys should this task be registered with TaskManager
    def keys
      @outputs.empty? ? [@id] : @outputs
    end

    # Create a task with zero or more outputs.
    #
    # `output` is an array of files or k/v store keys that the task generates
    # `inputs` is an array of files, task ids or k/v store keys that the
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
      proc : TaskProc | Nil = nil,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true
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

    # Create a task with zero or one outputs. Overload for convenience.
    def initialize(
      output : String | Nil = nil,
      inputs : Array(String) = [] of String,
      proc : TaskProc | Nil = nil,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false,
      mergeable : Bool = true
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
      call_results = Array(String | Nil).new
      @procs.each do |proc|
        Fiber.yield
        begin
          result = proc.call
        rescue ex
          raise "Task #{self} failed: #{ex}"
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
              Dir.mkdir_p(File.dirname output)
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
    end

    # Tasks are stale if:
    #
    # * One of their inputs are stale
    # * If one of the output files doesn't exist
    # * If any of the inputs are generated by a stale task

    # ameba:disable Metrics/CyclomaticComplexity
    def stale?
      # Tasks without inputs or flagged always_run are always stale
      Log.trace { "#{outputs} is stale because @always_run" } if @always_run
      Log.trace { "#{outputs} is stale because @inputs.empty?" } if @inputs.empty?
      return true if @always_run || @inputs.empty?
      # Tasks don't get stale twice
      return false unless @stale

      file_outputs = @outputs.reject(&.lchop?("kv://"))
      kv_outputs = @outputs.select(&.lchop?("kv://")).map(&.lchop("kv://"))

      result = (missing_file_outputs = file_outputs.any? { |output| !File.exists?(output) }) ||
               (missing_kv_outputs = kv_outputs.any? { |output| !TaskManager.get(output) }) ||
               (modified_inputs = inputs.any? { |input| TaskManager.modified.includes? input }) ||
               (stale_inputs = @inputs.any? { |input| TaskManager.tasks.has_key?(input) && TaskManager.tasks[input].stale? })

      if result
        Log.trace {
          "#{outputs} is stale because of missing_file_outputs"
        } if missing_file_outputs
        Log.trace {
          "#{outputs} is stale because of missing_kv_outputs"
        } if missing_kv_outputs
        Log.trace {
          "#{outputs} is stale because of modified_inputs #{inputs.select { |input| TaskManager.modified.includes? input }}"
        } if modified_inputs
        Log.trace {
          "#{outputs} is stale because of stale_inputs"
        } if stale_inputs
      end
      # p! missing_file_outputs, missing_kv_outputs, modified_inputs, stale_inputs
      result
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
    def ready?
      (stale? || always_run?) &&
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

  # TaskManager is a singleton that keeps track of all tasks
  class TaskManagerType
    # Registry of all tasks
    property tasks = {} of String => Croupier::Task
    # Registry of modified files, which will make tasks stale
    property modified = Set(String).new
    # SHA1 of files from last run
    property last_run = {} of String => String
    # SHA1 of files as of starting this run
    property this_run = {} of String => String
    # SAH1 of input files as of ending this run
    property next_run = {} of String => String
    # If true, only compare file dates
    property fast_mode = false # ameba:disable Style/QueryBoolMethods

    # Files with changes detected in auto_run
    @queued_changes : Set(String) = Set(String).new

    # Key/Value store
    @_store : Kiwi::Store = Kiwi::MemoryStore.new
    @_store_path : String | Nil = nil

    def set(key, value)
      @_store.set(key, value)
      @modified << "kv://#{key}"
    end

    def get(key)
      @_store.get(key)
    end

    # Use a persistent k/v store in this path instead of
    # the default memory store
    def use_persistent_store(path : String)
      return if path == @_store_path
      raise "Can't change persistent k/v store path" unless @_store_path.nil?
      new_store = Kiwi::FileStore.new(path)
      if @_store_path.nil?
        # Convert from MemoryStore to FileStore
        old_store = @_store.as(Kiwi::MemoryStore)
        old_store.@mem.each { |k, v| new_store[k] = v }
        @_store = new_store
      end
      Log.info { "Storing k/v data in #{path}" }
    end

    # Remove all tasks and everything else (good for tests)
    def cleanup
      modified.clear
      tasks.clear
      last_run.clear
      this_run.clear
      next_run.clear
      @all_inputs.clear
      @graph = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new
      @graph_sorted = [] of String
      @queued_changes.clear
      @_store_path = nil
      @_store = Kiwi::MemoryStore.new
      @fast_mode = false
    end

    # Tasks as a dependency graph sorted topologically
    @graph = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new
    @graph_sorted = [] of String

    def sorted_task_graph
      return @graph, @graph_sorted unless @graph.@vertice_dict.empty?
      # First, we create the graph
      @graph = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new

      # Add all tasks and inputs as vertices
      # Add all dependencies as edges

      # The start node is just a convenience
      @graph.add_vertex "start"

      # All inputs are vertices
      all_inputs.each do |input|
        if !tasks.has_key? input
          @graph.add_vertex input
          @graph.add_edge "start", input
        end
      end

      # Tasks without outputs are added as vertices by ID
      tasks.values.each do |task|
        if task.@outputs.empty?
          @graph.add_vertex task.@id
        end
      end

      # Add vertices and edges for inputs
      tasks.each do |output, task|
        @graph.add_vertex output
        if task.@inputs.empty?
          @graph.add_edge "start", output
        end
        task.@inputs.each do |input|
          @graph.add_edge input, output
        end
      end

      # Only return tasks, not inputs in the sorted graph
      @graph_sorted = topological_sort(@graph.@vertice_dict).select { |v| tasks.has_key? v }
      return @graph, @graph_sorted
    end

    # All inputs from all tasks
    @all_inputs = Set(String).new

    def all_inputs
      return @all_inputs unless @all_inputs.empty?
      tasks.values.each do |task|
        @all_inputs.concat task.@inputs
      end
      @all_inputs
    end

    # The set of all inputs for the given tasks
    def inputs(targets : Array(String))
      result = Set(String).new
      targets.each do |target|
        raise "Unknown target #{target}" unless tasks.has_key? target
      end

      dependencies(targets).each do |task|
        result.concat tasks[task].@inputs
      end

      result
    end

    # Get a task list of what tasks need to be done to produce `outputs`
    # The list is sorted so it can be executed in order
    def dependencies(outputs : Array(String))
      outputs.each do |output|
        if !tasks.has_key?(output)
          raise "Unknown output #{output}"
        end
      end
      result = self._dependencies outputs
      sorted_task_graph[1].select(->(v : String) { result.includes? v })
    end

    # Get a task list of what tasks need to be done to produce `output`
    # The list is sorted so it can be executed in order
    # Overloaded to accept a single string for convenience
    def dependencies(output : String)
      dependencies([output])
    end

    # Helper function for dependencies
    def _dependencies(outputs : Array(String))
      result = Set(String).new
      outputs.each do |output|
        if tasks.has_key? output
          result << output
          result.concat _dependencies(tasks[output].@inputs.to_a)
        end
      end
      result
    end

    def depends_on(input : String)
      depends_on [input]
    end

    def depends_on(inputs : Array(String))
      result = Set(String).new
      TaskManager.tasks.values.each do |t|
        if (t.@inputs & Set.new(inputs)).size > 0
          result.concat t.outputs
          result.concat depends_on(t.outputs)
        end
      end
      result
    end

    # Read state of last run, then scan inputs and compare
    def mark_stale_inputs
      if File.exists? ".croupier"
        last_run_date = File.info(".croupier").modification_time
        last_run = File.open(".croupier") do |file|
          YAML.parse(file).as_h.map { |k, v| [k.to_s, v.to_s] }.to_h
        end
      else
        last_run_date = Time.utc # Now
        last_run = {} of String => String
      end
      if @fast_mode
        all_inputs.each do |file|
          if info = File.info?(file)
            @modified << file if last_run_date < info.modification_time
          end
        end
      else
        (@this_run = scan_inputs).each do |file, sha1|
          @modified << file if last_run.fetch(file, "") != sha1
        end
      end
    end

    # Scan all inputs and return a hash with their sha1
    def scan_inputs
      all_inputs.reduce({} of String => String) do |hash, file|
        if File.exists? file
          hash[file] = Digest::SHA1.hexdigest(File.read(file))
        end
        hash
      end
    end

    # We ran all tasks, store the current state
    def save_run
      File.open(".croupier", "w") do |file|
        file << YAML.dump(this_run.merge next_run)
      end
    end

    # Check if all inputs are correct:
    # They should all be either task outputs or existing files
    def check_dependencies
      bad_inputs = all_inputs.select { |input|
        !input.lchop?("kv://") &&
          !tasks.has_key?(input) &&
          !File.exists?(input)
      }
      raise "Can't run: Unknown inputs #{bad_inputs.join(", ")}" \
         unless bad_inputs.empty?
    end

    # Run all stale tasks in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    def run_tasks(
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false
    )
      _, tasks = sorted_task_graph
      check_dependencies
      run_tasks(tasks, run_all, dry_run, parallel, keep_going)
    end

    # Run the tasks needed to create or update the requested targets
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    def run_tasks(
      targets : Array(String),
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false
    )
      tasks = dependencies(targets)
      if parallel
        _run_tasks_parallel(tasks, run_all, dry_run, keep_going)
      else
        _run_tasks(tasks, run_all, dry_run, keep_going)
      end
    end

    # Internal helper to run tasks serially
    def _run_tasks(
      outputs,
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false
    )
      mark_stale_inputs
      finished = Set(Task).new
      outputs.each do |output|
        t = tasks.fetch(output, nil)
        next if t.nil? || finished.includes?(t)
        next unless run_all || t.stale? || t.@always_run
        Log.debug { "Running task for #{output}" }
        raise "Can't run task for #{output}: Waiting for #{t.waiting_for}" unless t.ready? || dry_run
        begin
          t.run unless dry_run
        rescue ex
          Log.error { "Error running task for #{output}: #{ex}" }
          raise ex unless keep_going
        end
        finished << t
      end
      save_run
    end

    # Internal helper to run tasks concurrently
    #
    # Whenever a task is ready, launch it in a separate fiber
    # This is only concurrency, not parallelism, but on tests
    # it seems to be faster than running tasks sequentially.
    def _run_tasks_parallel(
      targets : Array(String) = [] of String,
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false # FIXME: implement
    )
      mark_stale_inputs

      targets = tasks.keys if targets.empty?
      _tasks = dependencies(targets)
      finished_tasks = Set(Task).new
      failed_tasks = Set(Task).new
      errors = [] of String

      loop do
        stale_tasks = (_tasks.map { |t| tasks[t] }).select(&.stale?).reject { |t|
          finished_tasks.includes?(t) || failed_tasks.includes?(t)
        }

        break if stale_tasks.empty?

        # The uniq is because a task may be repeated in the
        # task graph because of multiple outputs. We don't
        # want to run it twice.
        batch = stale_tasks.select(&.ready?).uniq!

        if batch.size == 0
          # No tasks are ready
          raise "Can't run tasks: Waiting for #{stale_tasks.map(&.waiting_for).uniq!.join(", ")}"
        end
        channel = Channel(Nil).new
        batch.each do |t|
          spawn do
            begin
              t.run unless dry_run
            rescue ex
              failed_tasks << t
              errors << ex.message.to_s
            ensure
              # Task is done, do not run again
              t.stale = false
              finished_tasks << t
              channel.send nil
            end
          end
        end
        # Wait for the whole batch to finish
        [..batch.size].each { channel.receive }
      end
      raise errors.join("\n") unless errors.empty? unless keep_going
      save_run
    end

    @autorun_control = Channel(Bool).new

    def auto_stop
      @autorun_control.send true
      @autorun_control.receive?
      @autorun_control = Channel(Bool).new
    end

    def auto_run(targets : Array(String) = [] of String)
      targets = tasks.keys if targets.empty?
      # Only want dependencies that are not tasks
      inputs = inputs(targets)
      raise "No inputs to watch, can't auto_run" if inputs.empty?
      watch(targets)
      spawn do
        loop do
          select
          when @autorun_control.receive
            Log.info { "Stopping automatic run" }
            @autorun_control.close
            @@watcher.close # Stop watchers
            break
          else
            begin
              # Sleep early is better for race conditions in tests
              # If we sleep late, it's likely that we'll get the
              # stop order and break the loop without running, so
              # we can't see the side effects without sleeping in
              # the tests.
              sleep 0.01.seconds
              next if @queued_changes.empty? && @modified.empty?
              Log.info { "Detected changes in #{@queued_changes}" }
              # Mark all targets as stale
              targets.each { |t| tasks[t].stale = true }
              @modified += @queued_changes
              Log.debug { "Modified: #{@modified}" }
              run_tasks(targets: targets)
              # Only clean queued changes after a successful run
              @modified.clear
              @queued_changes.clear
            rescue ex
              # Sometimes we can't run because not all dependencies
              # are there yet or whatever. We'll try again later
              unless ex.message.to_s.starts_with?("Can't run: Unknown inputs")
                Log.warn { "Automatic run failed (will retry): #{ex.message}" }
              end
            end
          end
        end
      end
    end

    # Filesystem watcher
    @@watcher = Inotify::Watcher.new

    # Watch for changes in inputs.
    # If an input has been changed BEFORE calling this method,
    # it will NOT be detected as a change.
    #
    # Changes are added to queued_changes
    def watch(targets : Array(String) = [] of String)
      @@watcher.close
      @@watcher = Inotify::Watcher.new
      targets = tasks.keys if targets.empty?
      target_inputs = inputs(targets)

      @@watcher.on_event do |event|
        # It's a file we care about, add it to the queue
        path = event.name || event.path
        Log.debug { "Detected change in #{path}" }
        Log.trace { "Event: #{event}" }
        @queued_changes << path.to_s if target_inputs.includes? path.to_s
      end

      watch_flags = LibInotify::IN_DELETE |
                    LibInotify::IN_CREATE |
                    LibInotify::IN_MODIFY |
                    LibInotify::IN_CLOSE_WRITE

      target_inputs.each do |input|
        # Don't watch for changes in k/v store
        next if input.lchop?("kv://")
        if File.exists? input
          @@watcher.watch input, watch_flags
        else
          # It's a file that doesn't exist. To detect it
          # being created, we watch the parent directory
          # if we are not already watching it.
          path = (Path[input].parent).to_s
          if !@@watcher.watching.includes?(path)
            @@watcher.watch path, watch_flags
          end
        end
      end
    end
  end

  # The global task manager (singleton)
  TaskManager = TaskManagerType.new
end
