# Croupier describes a task graph and lets you operate on them
require "digest/sha1"

module Croupier
  VERSION = "0.1.0"

  # A Task is a block of code that generates an output file
  #
  # It can have one or more inputs, which may also be outputs of other tasks
  # It has a descriptive `name` which should be understandable to the user
  # It has a `Proc` which is executed when the task is run

  class Task
    # Registry of all tasks
    @@Tasks = {} of String => Task

    def self.tasks
      @@Tasks
    end

    # Registry of modified files, which will make tasks stale
    @@Modified = Set(String).new

    def self.mark_modified(file)
      @@Modified << file
    end

    # SHA1 of files from last run
    @@LastRun = {} of String => String

    # All inputs from all tasks
    def self.all_inputs
      @@Tasks.values.map { |task| task.@inputs }.flatten.uniq
    end

    # Scan all inputs and return a hash with their sha1
    def self.scan_inputs
      self.all_inputs.reduce({} of String => String) do |hash, file|
        hash[file] = Digest::SHA1.hexdigest(File.read(file))
        hash
      end
    end

    # Run all tasks (inconditionally for now)
    def self.runTasks
      @@Tasks.each do |name, task|
        task.run
      end
    end

    @block : Proc(Nil)

    def initialize(name : String, output : String, inputs : Array(String), block : Proc)
      @name = name
      @block = block
      @output = output
      @inputs = inputs
      @stale = false
      if @@Tasks.has_key? output
        # Can't have two tasks generating the same output
        raise "Task conflict: #{name} would generate #{output} which is already generated by #{@@Tasks[output].@name}"
      end
      @@Tasks[output] = self
    end

    # This should generate the output file
    def run
      @block.call
      # Since we just generated it, output is modified and Task is not stale
      @stale = false
    end

    # Tasks are stale if any of their inputs are stale
    def stale?
      @stale = (
        @inputs.any? { |input| @@Modified.includes? input } ||
        @inputs.any? { |input| @@Tasks.has_key?(input) && @@Tasks[input].stale? }
      )
    end
  end
end
