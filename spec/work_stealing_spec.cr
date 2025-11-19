require "./spec_helper"
require "file_utils"
include Croupier

def with_scenario(
  name,
  keep = [] of String,
  to_create = {} of String => String,
  procs = {} of String => TaskProc, &
)
  # Setup logging, helps coverage
  logs = IO::Memory.new
  Log.setup(:trace, Log::IOBackend.new(io: logs))

  # Library of procs - matching the original croupier_spec
  x = 0
  _procs = {
    "dummy"   => TaskProc.new { "" },
    "counter" => TaskProc.new {
      x += 1
      x.to_s
    },
    "output2" => TaskProc.new {
      x += 1
      File.write("output2", "foo")
    },
  }.merge procs

  Dir.cd("spec/testcases/#{name}") do
    # Clean up
    File.delete?(".croupier")
    Dir.glob("*").each do |f|
      FileUtils.rm_rf(f) unless keep.includes?(f) || f == "tasks.yml"
    end
    TaskManager.cleanup

    # Create files as requested in scenario
    to_create.each do |k, v|
      File.open(k, "w") { |io| io << v }
    end

    # Create tasks from tasks.yml
    if File.exists?("tasks.yml")
      tasks = YAML.parse(File.read("tasks.yml"))
      tasks.as_h.values.each do |t|
        Task.new(
          outputs: t["outputs"].as_a.map(&.to_s),
          inputs: t["inputs"].as_a.map(&.to_s),
          proc: _procs[t["procs"]],
          always_run: t["always_run"].as_bool,
          no_save: t["no_save"].as_bool,
          id: t["id"].to_s,
        )
      end
    end
    begin
      yield
    rescue ex
      puts "Error: #{ex}"
      raise ex
    ensure
      TaskManager.cleanup
    end
  end
end

describe "Work-Stealing Algorithm" do
  describe "vs Static Chunking" do
    it "should handle tasks with varying execution times better" do
      with_scenario("empty") do
        # Create tasks with different execution times
        slow_tasks = [] of String
        fast_tasks = [] of String

        # Create 10 fast tasks (short execution)
        10.times do |i|
          task_name = "fast_#{i}"
          fast_tasks << task_name
          Task.new(output: task_name, inputs: [] of String) {
            File.write(task_name, "fast_#{i}")
            "fast_#{i}"
          }
        end

        # Create 5 slow tasks (longer execution)
        5.times do |i|
          task_name = "slow_#{i}"
          slow_tasks << task_name
          Task.new(output: task_name, inputs: [] of String) do
            # Simulate longer processing time
            sleep 1.milliseconds
            File.write(task_name, "slow_#{i}")
            "slow_#{i}"
          end
        end

        # Test work-stealing algorithm (should be faster due to better load balancing)
        work_stealing_time = Time.measure do
          TaskManager.run_tasks(parallel: true, run_all: true)
        end

        # Verify all tasks completed
        fast_tasks.each { |task_name|
          File.exists?(task_name).should be_true
        }
        slow_tasks.each { |task_name|
          File.exists?(task_name).should be_true
        }

        # Work-stealing should complete without errors
        work_stealing_time.total_milliseconds.should be < 100 # Should be very fast
      end
    end

    it "should handle single task efficiently" do
      with_scenario("empty") do
        Task.new(output: "single", inputs: [] of String) { "single_task" }

        Time.measure do
          TaskManager.run_tasks(parallel: true, run_all: true)
        end.total_milliseconds.should be < 50

        File.exists?("single").should be_true
      end
    end

    it "should handle more tasks than workers" do
      with_scenario("empty") do
        # Create 20 tasks (more than typical CPU count)
        20.times do |i|
          Task.new(output: "task_#{i}", inputs: [] of String) { "content_#{i}" }
        end

        Time.measure do
          TaskManager.run_tasks(parallel: true, run_all: true)
        end.total_milliseconds.should be < 100

        20.times do |i|
          File.exists?("task_#{i}").should be_true
        end
      end
    end

    it "should handle tasks that throw exceptions properly" do
      with_scenario("empty") do
        # Mix of normal and failing tasks
        Task.new(output: "good1", inputs: [] of String) { "good1" }
        Task.new(output: "bad", inputs: [] of String) { raise "Intentional failure" }
        Task.new(output: "good2", inputs: [] of String) { "good2" }

        # Should not raise when keep_going is true
        TaskManager.run_tasks(parallel: true, run_all: true, keep_going: true)

        # Good tasks should complete
        File.exists?("good1").should be_true
        File.exists?("good2").should be_true
        # Bad task should not create output
        File.exists?("bad").should be_false
      end
    end
  end

  describe "Edge Cases" do
    it "should handle zero tasks gracefully" do
      with_scenario("empty") do
        TaskManager.run_tasks(parallel: true, run_all: true) # Should not hang or error
      end
    end

    it "should handle independent tasks efficiently" do
      with_scenario("empty") do
        # Create independent tasks that can be run in parallel
        Task.new(output: "a", inputs: [] of String) { "input_a" }
        Task.new(output: "b", inputs: [] of String) { "input_b" }
        Task.new(output: "c", inputs: [] of String) { "input_c" }

        # These should all run in parallel efficiently
        TaskManager.run_tasks(parallel: true, run_all: true)

        File.read("a").should eq("input_a")
        File.read("b").should eq("input_b")
        File.read("c").should eq("input_c")
      end
    end
  end
end
