require "./spec_helper"
include Croupier

describe "Work-Stealing Algorithm" do
  describe "vs Static Chunking" do
    it "should handle tasks with varying execution times" do
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

        TaskManager.run_tasks(parallel: true, run_all: true)

        # Verify all tasks completed
        fast_tasks.each { |task_name|
          File.exists?(task_name).should be_true
        }
        slow_tasks.each { |task_name|
          File.exists?(task_name).should be_true
        }
      end
    end

    it "should handle single task efficiently" do
      with_scenario("empty") do
        Task.new(output: "single", inputs: [] of String) { "single_task" }

        TaskManager.run_tasks(parallel: true, run_all: true)

        File.exists?("single").should be_true
      end
    end

    it "should run more tasks than workers concurrently" do
      with_scenario("empty") do
        # 20 tasks (more than typical CPU count) that each take a
        # little while: running them in parallel must be faster than
        # the same work done serially. The comparison is relative, so
        # a slow or loaded machine can't flake it the way an absolute
        # wall-clock budget could.
        20.times do |i|
          Task.new(output: "serial_#{i}", inputs: [] of String) do
            sleep 30.milliseconds
            "content_#{i}"
          end
        end
        serial = Time.measure { TaskManager.run_tasks(run_all: true) }

        20.times do |i|
          Task.new(output: "parallel_#{i}", inputs: [] of String) do
            sleep 30.milliseconds
            "content_#{i}"
          end
        end
        parallel = Time.measure { TaskManager.run_tasks(parallel: true, run_all: true) }

        20.times { |i| File.exists?("parallel_#{i}").should be_true }
        # With at least two workers, ~20x30ms of work takes at most
        # half the serial time; the margin is wide by construction
        parallel.total_milliseconds.should be < serial.total_milliseconds
      end
    end

    it "should handle tasks that throw exceptions properly" do
      with_scenario("empty") do
        # Mix of normal and failing tasks
        Task.new(output: "good1", inputs: [] of String) { "good1" }
        Task.new(output: "bad", inputs: [] of String) { raise "Intentional failure" }
        Task.new(output: "good2", inputs: [] of String) { "good2" }

        # keep_going: the run completes, the failure surfaces at the end
        expect_raises(Croupier::RunFailure, /Intentional failure/) do
          TaskManager.run_tasks(parallel: true, run_all: true, keep_going: true)
        end

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
