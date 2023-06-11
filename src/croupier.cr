# Croupier describes a task graph and lets you operate on them
require "digest/sha1"
require "yaml"
require "crystalline"

module Croupier
  VERSION = "0.1.0"

  # A Task is a block of code that generates an output file
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

    # Tasks as a dependency graph sorted topologically
    def self.sorted_task_graph
      g = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new

      g.add_vertex "root"

      @@tasks.each do |output, task|
        g.add_vertex output
        if task.@inputs.empty?
          g.add_edge "root", output
        else
          task.@inputs.each do |input|
            g.add_edge input, output
          end
        end
      end
      # Connect all subgraphs
      all_inputs.each do |input|
        if !@@tasks.has_key? input
          g.add_vertex input
          g.add_edge "root", input
        end
      end
      # Ensure there are no loops
      dfs = Crystalline::Graph::DFSIterator.new(g, "root")
      dfs.back_edge_event = ->(u : String, v : String) {
        raise "Cycle detected between #{u} and #{v}"
      }
      sorted = [] of String
      dfs.each { |v| sorted << v }
      return g, sorted
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

    # SHA1 of files from last run
    @@last_run = {} of String => String
    # SHA1 of files as of now
    @@this_run = {} of String => String

    # All inputs from all tasks
    def self.all_inputs
      @@tasks.values.flat_map { |task| task.@inputs }.uniq!
    end

    # Read state of last run, then scan inputs and compare
    def self.mark_stale
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
        hash[file] = Digest::SHA1.hexdigest(File.read(file))
        hash
      end
    end

    # Run all stale tasks in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    def self.run_tasks(run_all : Bool = false)
      mark_stale
      _, tasks = Task.sorted_task_graph
      tasks.each do |task|
        if @@tasks.has_key?(task) && (run_all || @@tasks[task].stale?)
          @@tasks[task].run
        end
      end
      # We ran all tasks, store the current state
      File.open(".croupier", "w") do |file|
        file.puts YAML.dump(@@this_run)
      end
    end

    @block : Proc(Nil)

    def initialize(name : String, output : String, inputs : Array(String), block : Proc)
      if inputs.includes?(output)
        raise "Cycle detected"
      end
      @name = name
      @block = block
      @output = output
      @inputs = inputs
      @stale = false
      if @@tasks.has_key? output
        # Can't have two tasks generating the same output
        raise "Task conflict: #{name} would generate #{output} which is already generated by #{@@tasks[output].@name}"
      end
      @@tasks[output] = self
    end

    # This should generate the output file
    def run
      @block.call
    end

    # Tasks are stale if any of their inputs are stale
    def stale?
      @stale = (
        @inputs.any? { |input| @@modified.includes? input } ||
        @inputs.any? { |input| @@tasks.has_key?(input) && @@tasks[input].stale? }
      )
    end

    def to_s(io)
      io.puts "#{@name}::#{@output}"
    end
  end
end
