# Croupier describes a task graph and lets you operate on them
require "digest/sha1"
require "yaml"
require "crystalline"
require "log"
require "./topo_sort"

module Croupier
  VERSION = "0.2.0"

  # A Task is an object that may generate output
  #
  # It has a descriptive `name` which should be understandable to the user
  # It has a `Proc` which is executed when the task is run
  # It can have zero or more inputs
  # It has zero or more outputs
  # Tasks are connected by dependencies, where one task's output is another's input

  alias TaskProc = -> String? | Array(String)

  class Task
    include YAML::Serializable
    include YAML::Serializable::Strict

    property id : String = ""
    property name : String = ""
    property inputs : Array(String) = [] of String
    property outputs : Array(String) = [] of String
    property? always_run : Bool = false
    property? no_save : Bool = false
    @[YAML::Field(ignore: true)]
    property procs : Array(TaskProc) = [] of TaskProc

    @stale : Bool = true

    # Create a task with zero or more outputs.
    #
    # name is a descriptive name for the task
    # output is an array of files that the task generates
    # inputs is an array of files or task ids that the task depends on
    # proc is a proc that is executed when the task is run
    # no_save is a boolean that tells croupier that the task will save the files itself
    # id is a unique identifier for the task. If the task has no outputs, it *must* have an id
    # always_run is a boolean that tells croupier that the task is always
    #   stale regardless of its dependencies' state
    # FIXME: the id/name/output thing is confusing
    def initialize(
      name : String,
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
      @name = name
      @procs << proc unless proc.nil?
      @outputs = outputs.uniq
      @id = id ? id : Digest::SHA1.hexdigest(@outputs.join(","))
      @inputs = inputs
      @no_save = no_save

      raise "Task #{@name} has no outputs and no id" if @id.nil? && @outputs.empty?

      # Register with the task manager
      (@outputs.empty? ? [@id] : @outputs).each do |k|
        if TaskManager.tasks.has_key?(k)
          TaskManager.tasks[k].merge(self)
        else
          TaskManager.tasks[k] = self
        end
      end
    end

    # Create a task with zero or one outputs. Overload for convenience.
    def initialize(
      name : String,
      output : String | Nil = nil,
      inputs : Array(String) = [] of String,
      proc : TaskProc | Nil = nil,
      no_save : Bool = false,
      id : String | Nil = nil,
      always_run : Bool = false
    )
      initialize(
        name: name,
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
      # Tasks don't get stale twice
      return true if @always_run
      return false unless @stale

      @stale = (
        @outputs.any? { |output| !File.exists?(output) } ||
        # Any input file is modified
        @inputs.any? { |input| TaskManager.modified.includes? input } ||
        # Any input file is created by a stale task
        @inputs.any? { |input|
          TaskManager.tasks.has_key?(input) &&
            TaskManager.tasks[input].stale?
        }
      )
    end

    # A task is ready if it is stale but all its dependencies are not
    # For inputs that are tasks, we check if they are stale
    # For inputs that are not tasks, they should exist
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

    # Mark this task as stale. Only use for testing.
    def mark_stale
      @stale = true
    end

    # Mark as not ready. Only use for testing.
    def not_ready
      @stale = false
    end

    def to_s(io)
      io << "#{@name}::(#{@outputs.join(", ")})"
    end

    # Merge two tasks.
    #
    # inputs and outputs are joined
    # procs of the second task are added to the 1st
    def merge(other : Task)
      raise "Cannot merge tasks with different no_save settings" unless no_save? == other.no_save?
      raise "Cannot merge tasks with different always_run settings" unless always_run? == other.always_run?

      # @outputs is NOT unique! We can save multiple times
      @outputs += other.@outputs
      @inputs += other.@inputs
      @inputs.uniq!
      @procs += other.@procs
    end
  end

  struct TaskManager
    # Registry of all tasks
    @@tasks = {} of String => Croupier::Task

    # List of all registered tasks
    def self.tasks
      @@tasks
    end

    # Tasks as a dependency graph sorted topologically
    def self.sorted_task_graph
      # First, we create the graph
      g = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new

      # Add all tasks and inputs as vertices
      # Add all dependencies as edges

      # The start node is just a convenience
      g.add_vertex "start"

      # All inputs are vertices
      all_inputs.each do |input|
        if !@@tasks.has_key? input
          g.add_vertex input
          g.add_edge "start", input
        end
      end

      # Tasks without outputs are added as vertices by ID
      @@tasks.values.each do |task|
        if task.@outputs.empty?
          g.add_vertex task.@id
        end
      end

      # Add vertices and edges for dependencies
      @@tasks.each do |output, task|
        g.add_vertex output
        if task.@inputs.empty?
          g.add_edge "start", output
        end
        task.@inputs.each do |input|
          g.add_edge input, output
        end
      end

      # Only return tasks, not inputs in the sorted graph
      return g, topological_sort(g.@vertice_dict).select { |v| @@tasks.has_key? v }
    end

    # Registry of modified files, which will make tasks stale
    @@modified = Set(String).new

    # List of all modified files
    def self.modified
      @@modified
    end

    # Mark a file as modified
    def self.mark_modified(file)
      @@modified << file
    end

    # Mark all files as unmodified (only meant for testing)
    def self.clear_modified
      @@modified.clear
    end

    # Remove all tasks and everything else (good for tests)
    def self.cleanup
      self.clear_modified
      @@tasks = {} of String => Task
      @@last_run = {} of String => String
      @@this_run = {} of String => String
      @@next_run = {} of String => String
    end

    # SHA1 of files from last run
    @@last_run = {} of String => String
    # SHA1 of files as of starting this run
    @@this_run = {} of String => String
    # SAH1 of input files as of ending this run
    @@next_run = {} of String => String

    def self.next_run
      @@next_run
    end

    # All inputs from all tasks
    def self.all_inputs
      all = Array(String).new
      @@tasks.values.each do |task|
        all += task.@inputs
      end
      all
    end

    # Get a task list of what tasks need to be done to produce `outputs`
    # The list is sorted so it can be executed in order
    def self.dependencies(outputs : Array(String))
      outputs.each do |output|
        if !@@tasks.has_key?(output)
          raise "Unknown output #{output}"
        end
      end
      self._dependencies outputs
    end

    # Get a task list of what tasks need to be done to produce `output`
    # The list is sorted so it can be executed in order
    # Overloaded to accept a single string for convenience
    def self.dependencies(output : String)
      self.dependencies([output])
    end

    # Helper function for dependencies
    def self._dependencies(outputs : Array(String))
      result = Set(String).new
      outputs.each do |output|
        if @@tasks.has_key? output
          result << output
          result += _dependencies(@@tasks[output].@inputs).to_set
        end
      end
      self.sorted_task_graph[1].select(->(v : String) { result.includes? v })
    end

    # Read state of last run, then scan inputs and compare
    def self.mark_stale_inputs
      if File.exists? ".croupier"
        @@last_run = File.open(".croupier") do |file|
          YAML.parse(file).as_h.map { |k, v| [k.to_s, v.to_s] }.to_h
        end
      else
        @@last_run = {} of String => String
      end
      (@@this_run = scan_inputs).each do |file, sha1|
        if @@last_run.fetch(file, "") != sha1
          mark_modified(file)
        end
      end
    end

    # Scan all inputs and return a hash with their sha1
    def self.scan_inputs
      self.all_inputs.reduce({} of String => String) do |hash, file|
        if File.exists? file
          hash[file] = Digest::SHA1.hexdigest(File.read(file))
        end
        hash
      end
    end

    # We ran all tasks, store the current state
    # FIXME add tests for this
    def self.save_run
      File.open(".croupier", "w") do |file|
        file << YAML.dump(@@this_run.merge @@next_run)
      end
    end

    # Check if all inputs are correct:
    # They should all be either task outputs or existing files
    def self.check_dependencies
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
    def self.run_tasks(run_all : Bool = false, dry_run : Bool = false, parallel : Bool = false)
      mark_stale_inputs
      _, tasks = TaskManager.sorted_task_graph
      check_dependencies
      run_tasks(tasks, run_all, dry_run, parallel)
    end

    # Run the tasks needed to create or update the requested targets
    # run_all will run all tasks, not just the ones that are stale
    # dry_run will only log what would be done, but not actually do it
    def self.run_tasks(
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
    def self._run_tasks(outputs, run_all : Bool = false, dry_run : Bool = false)
      outputs.each do |output|
        if @@tasks.has_key?(output) && \
              (run_all || @@tasks[output].stale? ||
             @@tasks[output].@always_run)
          Log.debug { "Running task for #{output}" }
          @@tasks[output].run unless dry_run
        end
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
    def self._run_tasks_parallel(
      targets : Array(String) = [] of String,
      run_all : Bool = false,
      dry_run : Bool = false
    )
      mark_stale_inputs

      if targets.empty?
        targets = @@tasks.keys
      end

      eligible_tasks = @@tasks.select { |k, _|
        targets.includes? k
      }

      finished_tasks = Set(Task).new

      errors = [] of String
      loop do
        stale_tasks = eligible_tasks.values.select { |t|
          (!finished_tasks.includes?(t)) && t.stale?
        }
        if stale_tasks.empty?
          break
        end
        batch = stale_tasks.select(&.ready?)
        batch.each do |t|
          spawn do
            begin
              t.run unless dry_run
            rescue ex
              errors << ex.message.to_s
            ensure
              t.not_ready # FIXME shouldn't need this
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
end
