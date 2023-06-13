# Croupier describes a task graph and lets you operate on them
require "digest/sha1"
require "yaml"
require "crystalline"
require "log"
require "./topo_sort"

module Croupier
  VERSION = "0.1.2"

  # A Task is a code that generates an output file
  #
  # It can have one or more inputs, which may also be outputs of other tasks
  # It has a descriptive `name` which should be understandable to the user
  # It has a `Proc` which is executed when the task is run

  class Task
    # Registry of all tasks
    @@tasks = {} of String => Task

    def self.tasks
      @@tasks
    end

    def self.task(output)
      @@tasks[output]
    end

    # Tasks as a dependency graph sorted topologically
    def self.sorted_task_graph
      # First, we create the graph
      g = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new

      # Add all tasks and inputs as vertices
      # Add all dependencies as edges

      # The start node is just a convenience
      g.add_vertex "start"
      all_inputs.each do |input|
        if !@@tasks.has_key? input
          g.add_vertex input
          g.add_edge "start", input
        end
      end

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

    def self.modified
      @@modified
    end

    # Mark one file as modified
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

    # All inputs from all tasks
    def self.all_inputs
      @@tasks.values.flat_map { |task| task.@inputs }.uniq!
    end

    # Get a task list of what tasks need to be done to produce `output`
    # The list is sorted so it can be executed in order
    def self.dependencies(output : String)
      self.dependencies [output]
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

    # Helper function for dependencies
    def self._dependencies(outputs : Array(String))
      result = Set(String).new
      outputs.each do |output|
        if @@tasks.has_key? output
          result << output
          _dependencies(@@tasks[output].@inputs).each do |dep|
            result << dep
          end
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
    def self.save_run
      File.open(".croupier", "w") do |file|
        file << YAML.dump(@@this_run.merge @@next_run)
      end
    end

    # Run all stale tasks in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    def self.run_tasks(run_all : Bool = false)
      mark_stale_inputs
      _, tasks = Task.sorted_task_graph
      tasks.each do |task|
        if @@tasks.has_key?(task) && (run_all || @@tasks[task].stale?)
          path = @@tasks[task].@output
          data = @@tasks[task].run
          @@next_run[path] = Digest::SHA1.hexdigest(data)
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
    # TODO: support run_all
    def self.run_tasks_parallel
      mark_stale_inputs
      loop do
        stale_tasks = @@tasks.values.select(&.stale?).to_set
        if stale_tasks.empty?
          break
        end
        batch = stale_tasks.select(&.ready?)
        batch.each(&.not_ready)
        batch.each do |t|
          spawn do
            path = t.@output
            # FIXME: most of these are getting lost
            @@next_run[path] = Digest::SHA1.hexdigest(t.run)
          end
        end
        sleep(0.001)
      end
      # FIXME It's losing outputs for some reason
      save_run
    end

    @proc : Proc(String)

    def initialize(name : String, output : String, inputs : Array(String), proc : Proc(String), no_save = false)
      if inputs.includes?(output)
        raise "Cycle detected"
      end
      @name = name
      @proc = proc
      @output = output
      @inputs = inputs
      @stale = true
      @no_save = no_save
      if @@tasks.has_key? output
        # Can't have two tasks generating the same output
        raise "Task conflict: #{name} would generate #{output} which is already generated by #{@@tasks[output]}"
      end
      @@tasks[output] = self
    end

    # Executes the proc for the task and returns the contents of the output file.
    def run : String
      Fiber.yield
      data = @proc.call
      Fiber.yield
      if @no_save
        if !File.exists?(@output)
          raise "Task #{self} did not generate #{@output}"
        end
        data = File.read(@output)
      else # Save the file
        Dir.mkdir_p(File.dirname @output)
        File.open(@output, "w") do |io|
          io << data
        end
      end
      @stale = false # Done, not stale anymore
      data
    end

    # Tasks are stale if any of their inputs are stale, if the
    # output file does not exist, or if any of the inputs are
    # generated by a stale task
    def stale?
      # Tasks don't get stale twice
      if !@stale
        return false
      end

      @stale = (
        !File.exists?(@output) ||
        # Any input file is modified
        @inputs.any? { |input| @@modified.includes? input } ||
        # Any input file is created by a stale task
        @inputs.any? { |input| @@tasks.has_key?(input) && @@tasks[input].stale? }
      )
    end

    # A task is ready if it is stale but all its dependencies are not
    # For inputs that are tasks, we check if they are stale
    # For inputs that are not tasks, they should exist
    def ready?
      (
        stale? &&
          @inputs.all? { |input|
            if @@tasks.has_key? input
              !@@tasks[input].stale?
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

    # Mark as not ready
    def not_ready
      @stale = false
    end

    def to_s(io)
      io << "#{@name}::#{@output}"
    end
  end
end
