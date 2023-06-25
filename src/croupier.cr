# Croupier describes a task graph and lets you operate on them
require "digest/sha1"
require "yaml"
require "crystalline"
require "log"
require "./topo_sort"

module Croupier
  VERSION = "0.2.4"

  # A Task is an object that may generate output
  #
  # It has a `Proc` which is executed when the task is run
  # It can have zero or more inputs
  # It has zero or more outputs
  # Tasks are connected by dependencies, where one task's output is another's input

  alias TaskProc = -> String? | Array(String)

  class Task
    include YAML::Serializable
    include YAML::Serializable::Strict

    property id : String = ""
    property inputs : Array(String) = [] of String
    property outputs : Array(String) = [] of String
    property stale : Bool = true # ameba:disable Style/QueryBoolMethods
    property? always_run : Bool = false
    property? no_save : Bool = false
    @[YAML::Field(ignore: true)]
    property procs : Array(TaskProc) = [] of TaskProc

    # Under what keys should this task be registered with TaskManager
    def keys
      @outputs.empty? ? [@id] : @outputs
    end

    # Create a task with zero or more outputs.
    #
    # `output` is an array of files that the task generates
    # `inputs` is an array of files or task ids that the task depends on
    # `proc` is a proc that is executed when the task is run
    # `no_save` is a boolean that tells croupier that the task will save the files itself
    # `id` is a unique identifier for the task. If the task has no outputs,
    # it *must* have an id. If not given, it's calculated as a hash of outputs.
    # `always_run` is a boolean that tells croupier that the task is always
    #   stale regardless of its dependencies' state
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
      always_run : Bool = false
    )
      if !(inputs.to_set & outputs.to_set).empty?
        raise "Cycle detected"
      end
      @always_run = always_run
      @procs << proc unless proc.nil?
      @outputs = outputs.uniq
      raise "Task has no outputs and no id" if id.nil? && @outputs.empty?
      @id = id ? id : Digest::SHA1.hexdigest(@outputs.join(","))[..6]
      @inputs = inputs
      @no_save = no_save

      # Register with the task manager.
      # We should merge every task we have output/id collision with
      # into one, and register it on every output/id of every one
      # of those tasks
      to_merge = (keys.map { |k|
        TaskManager.tasks.fetch(k, nil)
      }).select(Task).uniq!
      to_merge << self
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
      always_run : Bool = false
    )
      initialize(
        outputs: output ? [output] : [] of String,
        inputs: inputs,
        proc: proc,
        no_save: no_save,
        id: id,
        always_run: always_run
      )
    end

    # Executes the proc for the task
    def run
      call_results = Array(String | Nil).new
      @procs.each do |proc|
        Fiber.yield
        result = proc.call
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
            Dir.mkdir_p(File.dirname output)
            File.open(output, "w") do |io|
              io << call_result
            end
            TaskManager.next_run[output] = Digest::SHA1.hexdigest(call_result)
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

    def stale?
      # Tasks without inputs or flagged always_run are always stale
      return true if @always_run || @inputs.empty?
      # Tasks don't get stale twice
      return false unless @stale

      @outputs.any? { |output| !File.exists?(output) } ||
        # Any input file is modified
        @inputs.any? { |input| TaskManager.modified.includes? input } ||
        # Any input file is created by a stale task
        @inputs.any? { |input|
          TaskManager.tasks.has_key?(input) &&
            TaskManager.tasks[input].stale?
        }
    end

    # A task is ready if it is stale but all its inputs are not.
    # For inputs that are tasks, we check if they are stale
    # For inputs that are not tasks, they should exist as files
    def ready?
      (
        (stale? || always_run?) &&
          @inputs.all? { |input|
            if TaskManager.tasks.has_key? input
              !TaskManager.tasks[input].stale?
            else
              File.exists? input
            end
          }
      )
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
      @inputs.uniq!
      @procs += other.@procs
      self
    end
  end

  struct TaskManagerType
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
          result.concat _dependencies(tasks[output].@inputs)
        end
      end
      result
    end

    # Read state of last run, then scan inputs and compare
    def mark_stale_inputs
      if File.exists? ".croupier"
        last_run = File.open(".croupier") do |file|
          YAML.parse(file).as_h.map { |k, v| [k.to_s, v.to_s] }.to_h
        end
      else
        last_run = {} of String => String
      end
      (this_run = scan_inputs).each do |file, sha1|
        if last_run.fetch(file, "") != sha1
          modified << file
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
    # FIXME add tests for this
    def save_run
      File.open(".croupier", "w") do |file|
        file << YAML.dump(this_run.merge next_run)
      end
    end

    # Check if all inputs are correct:
    # They should all be either task outputs or existing files
    def check_dependencies
      bad_inputs = all_inputs.select { |input|
        !tasks.has_key?(input) && !File.exists?(input)
      }
      raise "Can't run: Unknown inputs #{bad_inputs.join(", ")}" \
         unless bad_inputs.empty?
    end

    # Run all stale tasks in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    def run_tasks(run_all : Bool = false, dry_run : Bool = false, parallel : Bool = false)
      mark_stale_inputs
      _, tasks = sorted_task_graph
      check_dependencies
      run_tasks(tasks, run_all, dry_run, parallel)
    end

    # Run the tasks needed to create or update the requested targets
    # run_all will run all tasks, not just the ones that are stale
    # dry_run will only log what would be done, but not actually do it
    def run_tasks(
      targets : Array(String),
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false
    )
      mark_stale_inputs
      tasks = dependencies(targets)
      if parallel
        _run_tasks_parallel(tasks, run_all, dry_run)
      else
        _run_tasks(tasks, run_all, dry_run)
      end
    end

    # Helper to run tasks
    def _run_tasks(outputs, run_all : Bool = false, dry_run : Bool = false)
      finished = Set(Task).new
      outputs.each do |output|
        next unless tasks.has_key?(output)
        next if finished.includes?(tasks[output])
        next unless run_all || tasks[output].stale? || tasks[output].@always_run

        Log.debug { "Running task for #{output}" }
        tasks[output].run unless dry_run
        finished << tasks[output]
      end
      save_run
    end

    # Run all stale tasks as concurrently as possible.
    #
    # Whenever a task is ready, launch it in a separate fiber
    # This is only concurrency, not parallelism, but on tests
    # it seems to be faster than running tasks sequentially.
    #
    # However, it's a bit buggy (the .croupier file is not correct)
    def _run_tasks_parallel(
      targets : Array(String) = [] of String,
      run_all : Bool = false,
      dry_run : Bool = false
    )
      mark_stale_inputs

      targets = tasks.keys if targets.empty?
      _tasks = dependencies(targets)
      finished_tasks = Set(Task).new
      errors = [] of String

      loop do
        stale_tasks = (_tasks.map { |t| tasks[t] }).select(&.stale?).reject { |t|
          finished_tasks.includes?(t)
        }

        break if stale_tasks.empty?

        # The uniq is because a task may be repeated in the
        # task graph because of multiple outputs. We don't
        # want to run it twice.
        batch = stale_tasks.select(&.ready?).uniq!
        batch.each do |t|
          spawn do
            begin
              t.run unless dry_run
            rescue ex
              errors << ex.message.to_s
            ensure
              # Task is done, do not run again
              t.stale = false
              finished_tasks << t
            end
          end
        end
        sleep(0.001)
      end
      raise errors.join("\n") unless errors.empty?
      # FIXME It's losing outputs for some reason
      save_run
    end
  end

  # The global task manager (singleton)
  TaskManager = TaskManagerType.new
end
