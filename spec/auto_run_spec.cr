require "./spec_helper"
require "file_utils"
include Croupier

describe "TaskManager" do
  describe "watch" do
    it "should always start with no queued changes" do
      with_scenario("basic", to_create: {"input" => "foo"}) do
        TaskManager.watch
        Fiber.yield
        TaskManager.@queued_changes.empty?.should be_true
      end
    end

    it "should queue changed inputs" do
      with_scenario("basic", to_create: {"input" => "foo"}) do
        TaskManager.watch
        File.open("input", "w") << "bar"
        # The inotify event is delivered asynchronously by the kernel,
        # so a single Fiber.yield can run before the watcher fiber is
        # scheduled: wait (with a timeout) for the event to land.
        wait_until(message: "inotify event for input never arrived") {
          TaskManager.@queued_changes.includes?("input")
        }
        File.open("input2", "w") << "foo"
        wait_until(message: "inotify event for input2 never arrived") {
          TaskManager.@queued_changes.includes?("input2")
        }
        TaskManager.@queued_changes.should eq Set{"input", "input2"}
      end
    end
  end

  describe "inputs" do
    it "should list all inputs, including transitive dependencies" do
      with_scenario("basic") do
        TaskManager.inputs(["output1"]).empty?.should be_true
        TaskManager.inputs(["output3"]).should eq Set{"input"}
        TaskManager.inputs(["output4"]).should eq Set{"input", "output3"}
        TaskManager.inputs(["output5"]).should eq Set{"input2"}
        TaskManager.inputs(["output4", "output5"]).should eq Set{"input", "input2", "output3"}
      end
    end
  end

  describe "auto_run" do
    it "should not re-run dependents when outputs are unchanged" do
      with_scenario("empty", to_create: {"seed" => "one"}) do
        producer_runs = 0
        dependent_runs = 0
        # The producer re-runs every cycle (always_run) but always
        # writes the same content: its dependent must not re-run
        Task.new(output: "up", inputs: ["seed"], always_run: true) {
          producer_runs += 1
          "same"
        }
        Task.new(output: "down", inputs: ["up"]) {
          dependent_runs += 1
          "d"
        }

        TaskManager.auto_run
        Fiber.yield

        File.write("seed", "two")
        wait_until(message: "first producer cycle never ran") {
          producer_runs == 1 && auto_cycle_settled?
        }
        dependent_runs.should eq 1

        File.write("seed", "three")
        wait_until(message: "second producer cycle never ran") {
          producer_runs == 2 && auto_cycle_settled?
        }
        # Early cutoff must work across auto cycles: "up" was
        # rewritten with identical content. Once a cycle settled, no
        # further run can happen without a new event, so this is
        # deterministic.
        dependent_runs.should eq 1

        TaskManager.auto_stop
      end
    end

    it "should watch inputs of subtasks created during auto_run" do
      with_scenario("empty", to_create: {"seed" => "one"}) do
        # The master creates a subtask whose input does not exist yet
        # when auto_run starts watching
        Task.new(output: "master_out", inputs: ["seed"], master_task: true) do
          File.write("sub_input", "1")
          Task.new(output: "sub_out", inputs: ["sub_input"]) { File.read("sub_input") }
          "master data"
        end

        TaskManager.auto_run
        Fiber.yield
        # A change to the master's input triggers a cycle that creates
        # the subtask and runs it
        File.write("seed", "two")
        wait_until(message: "subtask never ran") {
          File.exists?("sub_out") && auto_cycle_settled?
        }
        File.read("sub_out").should eq "1"

        # The subtask's input was not known when auto_run started
        # watching: without re-watching, changes to it are invisible
        File.write("sub_input", "2")
        wait_until(message: "subtask never re-ran") { File.read("sub_out") == "2" }

        TaskManager.auto_stop
      end
    end

    it "should run tasks when inputs change" do
      with_scenario("basic") do
        TaskManager.auto_run
        # We need to yield or else the watch callbacks never run
        Fiber.yield
        # At this point output3 doesn't exist
        File.exists?("output3").should be_false
        # We create input, which is output3's dependency
        File.open("input", "w") << "bar"
        wait_until(message: "output3 never built") { File.exists?("output3") }
        # We create input2, which is output5's dependency
        File.open("input2", "w") << "bar"
        wait_until(message: "output5 never built") { File.exists?("output5") }
        TaskManager.auto_stop
        # And now output3 should exist
        File.exists?("output3").should be_true
      end
    end

    it "should not re-raise exceptions" do
      with_scenario("empty") do
        x = 0
        error_proc = TaskProc.new { x += 1; raise "boom" }
        Task.new(output: "t1", inputs: ["i"], proc: error_proc)
        TaskManager.auto_run
        Fiber.yield
        File.open("i", "w") << "foo"
        wait_until(message: "task never ran") { x > 0 }
        TaskManager.auto_stop
        # auto_run logs all errors and continues, because it's
        # normal to have failed runs in auto mode
        (x > 0).should be_true
      end
    end

    it "should not run when no inputs have changed" do
      with_scenario("empty") do
        x = 0
        counter = TaskProc.new { x += 1; x.to_s }
        Task.new(output: "t1", inputs: ["i"], proc: counter)
        TaskManager.auto_run
        # We need to yield or else the watch callbacks never run
        Fiber.yield
        TaskManager.auto_stop
        # It should never have ran
        x.should eq 0
      end
    end

    it "should run only when inputs have changed" do
      with_scenario("empty") do
        x = 0
        counter = TaskProc.new { x += 1; x.to_s }
        Task.new(output: "t1", inputs: ["i"], proc: counter)
        TaskManager.auto_run
        Fiber.yield
        File.open("i", "w") << "foo"
        wait_until(message: "task never ran") { x == 1 }
        TaskManager.auto_stop
        # It should only have ran once
        x.should eq 1
      end
    end

    it "should run tasks without outputs" do
      with_scenario("empty") do
        x = 0
        counter = TaskProc.new { x += 1; x.to_s }
        Task.new(id: "t1", inputs: ["i"], proc: counter)
        TaskManager.auto_run
        Fiber.yield
        File.open("i", "w") << "foo"
        wait_until(message: "task never ran") { x == 1 }
        TaskManager.auto_stop
        # It should only have ran once
        x.should eq 1
      end
    end

    it "should not run if there are no inputs" do
      with_scenario("empty") do
        Task.new(id: "t1")
        expect_raises(Exception, "No inputs to watch") do
          TaskManager.auto_run
        end
      end
    end

    it "should only run the specified targets" do
      with_scenario("basic") do
        TaskManager.auto_run(targets: ["output3"])
        # At this point output1/3 doesn't exist
        File.exists?("output1").should be_false
        File.exists?("output3").should be_false

        # This triggers building output3
        File.open("input", "w") << "bar"
        wait_until(message: "output3 never built") { File.exists?("output3") }
        TaskManager.auto_stop
        # At this point output3 exists, output1 doesn't
        File.exists?("output1").should be_false
        File.exists?("output3").should be_true
      end
    end

    it "should run on every modification of inputs" do
      with_scenario("basic") do
        TaskManager.auto_run(targets: ["output3"])
        # At this point output3 doesn't exist
        File.exists?("output3").should be_false
        # This triggers building output3
        File.open("input", "w") << "bar1"
        wait_until(message: "output3 never built") {
          File.exists?("output3") && auto_cycle_settled?
        }
        # We delete things, and then trigger another build
        File.delete("output3")
        File.delete("input")
        File.open("input", "w") << "bar2"
        wait_until(message: "output3 never rebuilt") {
          File.exists?("output3") && auto_cycle_settled?
        }
        TaskManager.auto_stop
        File.exists?("output3").should be_true
      end
    end

    it "should not be triggered by deps for not specified targets" do
      with_scenario("basic") do
        TaskManager.auto_run(targets: ["output5"])
        # At this point output5 doesn't exist
        File.exists?("output5").should be_false
        File.exists?("output3").should be_false
        # This would trigger output3's task in a full run, but "input"
        # is not watched in this one (only input2 is): nothing must
        # happen. Negative assertion, so a bounded window it, with a
        # settle check that no cycle is pending either.
        File.open("input", "w") << "bar"
        sleep 0.1.seconds
        auto_cycle_settled?.should be_true
        TaskManager.auto_stop
        # No outputs created
        File.exists?("output5").should be_false
        File.exists?("output3").should be_false
      end
    end

    it "should not try to watch k/v keys" do
      with_scenario("empty") do
        Task.new(inputs: ["kv://foo"], output: "bar")
        # This crashes if it tries to watch the wrong path
        TaskManager.auto_run
        TaskManager.auto_stop
      end
    end

    it "should rerun tasks if a kv:// input changes" do
      with_scenario("empty") do
        x = 0
        TaskManager.set("foo", "bar1")
        Task.new(inputs: ["kv://foo"], output: "kv://bar",
          proc: TaskProc.new { (x = x + 1).to_s })
        TaskManager.auto_run
        x.should eq 0
        TaskManager.set("foo", "bar2")
        wait_until(message: "task never ran after kv change") { x == 1 }
        TaskManager.set("foo", "bar3")
        wait_until(message: "task never re-ran after second kv change") { x == 2 }
        TaskManager.auto_stop
        # With the auto mode fix, both changes are detected (not just the first)
        x.should eq 2
      end
    end

    it "should run tasks if a watched folder is created" do
      with_scenario("a_dir") do
        File.exists?("output3").should be_false
        TaskManager.auto_run
        Dir.mkdir("a_dir")
        wait_until(message: "output3 never built") { File.exists?("output3") }
        TaskManager.auto_stop
      end
    end

    it "should run tasks if a file is created inside a watched folder" do
      with_scenario("a_dir") do
        Dir.mkdir("a_dir")
        TaskManager.auto_run
        File.exists?("output3").should be_false
        File.open("a_dir/input", "w") << "bar"
        wait_until(message: "output3 never built") { File.exists?("output3") }
        TaskManager.auto_stop
      end
    end

    it "should run tasks if a subdirectory is created in a watched folder" do
      with_scenario("a_dir") do
        Dir.mkdir("a_dir")
        TaskManager.auto_run
        File.exists?("output3").should be_false
        # Create a nested subdirectory with a file
        Dir.mkdir("a_dir/subdir")
        File.open("a_dir/subdir/input", "w") << "bar"
        wait_until(message: "output3 never built") { File.exists?("output3") }
        TaskManager.auto_stop
      end
    end

    it "should run tasks if a file is modified in a nested subdirectory" do
      with_scenario("a_dir") do
        Dir.mkdir("a_dir")
        Dir.mkdir("a_dir/subdir")
        File.open("a_dir/subdir/input", "w") << "bar"
        # Run once to create output3
        TaskManager.run_tasks
        File.exists?("output3").should be_true
        initial_content = File.read("output3")
        # Now watch for changes
        TaskManager.auto_run
        # Modify the nested file
        File.open("a_dir/subdir/input", "w") << "modified"
        # Task should have run again, content should change
        wait_until(message: "output3 never rebuilt") { File.read("output3") != initial_content }
        TaskManager.auto_stop
      end
    end

    it "should run tasks if a file is deleted from a nested subdirectory" do
      with_scenario("a_dir") do
        Dir.mkdir("a_dir")
        Dir.mkdir("a_dir/subdir")
        File.open("a_dir/subdir/input", "w") << "bar"
        # Run once to create output3
        TaskManager.run_tasks
        File.exists?("output3").should be_true
        initial_content = File.read("output3")
        # Now watch for changes
        TaskManager.auto_run
        # Delete the nested file
        File.delete("a_dir/subdir/input")
        # Task should have run again, content should change
        wait_until(message: "output3 never rebuilt") { File.read("output3") != initial_content }
        TaskManager.auto_stop
      end
    end
  end
end
