require "./spec_helper"
require "file_utils"
include Croupier

describe "TaskManager" do
  describe "tasks" do
    it "should fail when you fetch a task that doesn't exist" do
      with_scenario("basic") do
        expect_raises(KeyError) do
          TaskManager.tasks["foo"]
        end
      end
    end

    it "should include registered tasks" do
      with_scenario("basic") do
        TaskManager.tasks.has_key?("output1").should eq true
      end
    end
  end
  describe "all_inputs" do
    it "should list all inputs for all tasks" do
      with_scenario("basic") do
        TaskManager.all_inputs.should eq Set{"input", "output3", "input2"}
      end
    end

    it "should not repeat inputs shared by several tasks" do
      with_scenario("basic") do
        Task.new(inputs: ["input"], output: "also_consumes_input") { "x" }
        Task.new(inputs: ["input"], output: "and_this_one") { "y" }
        # "input" is now consumed by three tasks: all_inputs is a Set,
        # so it still counts once and the collection doesn't grow
        TaskManager.all_inputs.should eq Set{"input", "output3", "input2"}
        TaskManager.all_inputs.size.should eq 3
      end
    end

    it "should be invalidated when a task is added" do
      with_scenario("basic") do
        # Force the cache to populate.
        TaskManager.all_inputs.should eq Set{"input", "output3", "input2"}

        # Register a new task with a previously-unseen input. The cache
        # must be invalidated so all_inputs reflects the new input without
        # an intervening graph rebuild.
        Task.new(inputs: ["new_input"], output: "new_output") { "new" }
        TaskManager.all_inputs.should eq Set{"input", "output3", "input2", "new_input"}
      end
    end
  end

  describe "add_input" do
    it "adds an input, invalidates the input cache, and affects the next run" do
      with_scenario("empty", to_create: {"seed" => "seed", "extra" => "extra"}) do
        runs = 0
        task = Task.new(output: "out", inputs: ["seed"]) {
          runs += 1
          "data"
        }
        TaskManager.run_tasks
        runs.should eq 1

        TaskManager.add_input("out", "extra").should be_true
        task.inputs.should contain "extra"
        # Visible to the caches without registering any new task
        TaskManager.all_inputs.should contain "extra"

        # The new dependency makes the task stale when the input changes
        File.write("extra", "changed")
        TaskManager.run_tasks
        runs.should eq 2
      end
    end

    it "is a no-op for inputs the task already has" do
      with_scenario("empty", to_create: {"seed" => "seed"}) do
        Task.new(output: "out", inputs: ["seed"]) { "data" }
        TaskManager.add_input("out", "seed").should be_false
        TaskManager.tasks["out"].inputs.size.should eq 1
      end
    end

    it "raises for unknown tasks and self-cycles" do
      with_scenario("empty") do
        Task.new(output: "out", inputs: [] of String) { "data" }
        expect_raises(Exception, /Unknown task/) { TaskManager.add_input("nope", "x") }
        expect_raises(Exception, /Cycle detected/) { TaskManager.add_input("out", "out") }
      end
    end
  end

  describe "sorted_task_graph" do
    it "should create a topologically sorted task graph" do
      expected = {
        "start"   => Set{"input", "input2", "output1", "output2"},
        "input"   => Set{"output3"},
        "input2"  => Set{"output5"},
        "output1" => Set(String).new,
        "output2" => Set(String).new,
        "output3" => Set{"output4"},
        "output4" => Set(String).new,
        "output5" => Set(String).new,
      }
      with_scenario("basic") do
        g, s = TaskManager.sorted_task_graph
        g.should eq expected
        s.size.should eq TaskManager.tasks.size
        # The exact order among independent tasks is unspecified (it
        # only needs to be deterministic); what matters is that every
        # task comes after its dependencies
        positions = s.map_with_index { |name, index| {name, index} }.to_h
        TaskManager.tasks.each_value do |task|
          task.inputs.each do |input|
            if TaskManager.tasks.has_key?(input)
              (positions[input] < positions[task.keys.first]).should be_true
            end
          end
        end
      end
    end

    it "should detect cycles in the graph" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        Task.new("input", ["output4"])
        expect_raises(Exception, "Cycle detected") do
          TaskManager.sorted_task_graph
        end
      end
    end
  end

  describe "topological_sort" do
    it "reports unreachable vertices instead of claiming a cycle" do
      graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      graph["start"] << "a"
      # An acyclic island the DFS from "start" never sees
      graph["island"] << "island2"

      expect_raises(Exception, /unreachable.*island/i) do
        topological_sort(graph)
      end
    end

    it "still reports a cycle among unreachable vertices" do
      graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      graph["start"] << "a"
      graph["x"] << "y"
      graph["y"] << "x"

      expect_raises(Exception, "Cycle detected") do
        topological_sort(graph)
      end
    end

    it "accepts plain hashes without a default block" do
      graph = {"start" => Set{"a"}} of String => Set(String)

      topological_sort(graph).should contain "a"
    end

    it "visits siblings in a deterministic order" do
      graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      graph["start"] << "b"
      graph["start"] << "a"
      graph["start"] << "c"

      # Sorted adjacency: the exact sibling order is part of the
      # contract, so a stdlib hash-layout change can't silently
      # reshuffle serial run order
      topological_sort(graph).should eq ["start", "a", "b", "c"]
    end
  end

  # Run the same tests for parallel and serial execution of tasks
  [false, true].each do |parallel|
    describe "run_tasks, parallel = #{parallel}" do
      it "should run all stale tasks when run_all is false" do
        with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
          # Run once to create all outputs
          TaskManager.run_tasks(parallel: parallel)
          # All outputs should exist after first run
          TaskManager.tasks.keys.each do |k|
            File.exists?(k).should be_true
          end
          # Second run with run_all=false should do nothing (no stale tasks)
          # and should not raise any errors
          TaskManager.run_tasks(parallel: parallel, run_all: false)
        end
      end

      it "should fail if the next task to run is not ready" do
        with_scenario("empty") do
          Task.new(output: "t1", inputs: ["kv://foo"], proc: TaskProc.new { "" })
          expect_raises(Exception) do
            TaskManager.tasks["t1"].ready?.should be_false
            TaskManager.run_tasks
          end
        end
      end

      it "should not consume other tasks' input changes on a targeted run" do
        with_scenario("empty", to_create: {"a_in" => "1", "b_in" => "1"}) do
          Task.new(output: "a_out", inputs: ["a_in"]) { File.read("a_in") }
          Task.new(output: "b_out", inputs: ["b_in"]) { File.read("b_in") }

          TaskManager.run_tasks(parallel: parallel)
          File.read("b_out").should eq "1"

          # Targeted run of a_out's closure must not touch b's state
          File.write("b_in", "2")
          TaskManager.run_tasks(["a_out"], parallel: parallel)

          # A later full run must still see b_in as modified and rebuild
          TaskManager.run_tasks(parallel: parallel)
          File.read("b_out").should eq "2"
        end
      end

      it "should not re-run kv dependents when the kv value is unchanged" do
        with_scenario("empty") do
          consumer_runs = 0
          # The producer re-runs every time but always yields the same
          # value: its dependents must not be re-staled by the re-set
          Task.new(output: "kv://k1", inputs: [] of String, always_run: true) { "same" }
          Task.new(output: "out", inputs: ["kv://k1"]) {
            consumer_runs += 1
            "data"
          }

          TaskManager.run_tasks(parallel: parallel)
          consumer_runs.should eq 1

          TaskManager.run_tasks(parallel: parallel)
          consumer_runs.should eq 1
        end
      end

      it "should re-run kv dependents when the kv value changes" do
        with_scenario("empty") do
          consumer_runs = 0
          value = "one"
          Task.new(output: "kv://k1", inputs: [] of String, always_run: true) { value }
          Task.new(output: "out", inputs: ["kv://k1"]) {
            consumer_runs += 1
            "data"
          }

          TaskManager.run_tasks(parallel: parallel)
          consumer_runs.should eq 1

          value = "two"
          TaskManager.run_tasks(parallel: parallel)
          consumer_runs.should eq 2
        end
      end

      it "should retry failed tasks on the next run when using keep_going" do
        with_scenario("empty", to_create: {"seed" => "one"}) do
          broken = false
          runs = 0
          Task.new(output: "out", inputs: ["seed"]) {
            runs += 1
            raise "boom" if broken
            File.read("seed")
          }

          # First run succeeds and records the input hash
          TaskManager.run_tasks(parallel: parallel)
          runs.should eq 1

          # Input changes and the task fails, but keep_going lets the
          # run (and the state save) finish
          File.write("seed", "two")
          broken = true
          TaskManager.run_tasks(parallel: parallel, keep_going: true)
          runs.should eq 2

          # Repaired: the next run must still see the input as
          # modified (the failed run must not have consumed it)
          broken = false
          TaskManager.run_tasks(parallel: parallel)
          runs.should eq 3
          File.read("out").should eq "two"
        end
      end

      it "should report unknown inputs on a targeted run" do
        with_scenario("empty") do
          Task.new(output: "out", inputs: ["missing"]) { "x" }
          # The specific message matters: auto_run's warn suppression
          # matches it, so missing inputs stay quiet instead of
          # logging a warning on every retry
          expect_raises(Exception, "Can't run: Unknown inputs missing") do
            TaskManager.run_tasks(["out"])
          end
        end
      end

      it "should not run dependents of a failed task" do
        with_scenario("empty", to_create: {"seed" => "x"}) do
          downstream_runs = 0
          Task.new(output: "up", inputs: ["seed"]) { raise "boom" }
          Task.new(output: "down", inputs: ["up"]) {
            downstream_runs += 1
            "d"
          }
          Task.new(output: "side", inputs: ["seed"]) { "s" }

          # keep_going: the failure doesn't abort the run...
          TaskManager.run_tasks(parallel: parallel, keep_going: true)

          # ...but the dependent of the failed task must not run against
          # the missing output
          downstream_runs.should eq 0
          File.exists?("down").should be_false

          # Unrelated tasks still ran
          File.exists?("side").should be_true
        end
      end

      it "should abort with the task failure when not using keep_going" do
        with_scenario("empty", to_create: {"seed" => "x"}) do
          Task.new(output: "up", inputs: ["seed"]) { raise "boom" }
          Task.new(output: "down", inputs: ["up"]) { "d" }

          # The failure itself must surface, not a "waiting for" message
          # about the dependent blocked behind it
          expect_raises(Exception, /boom/) do
            TaskManager.run_tasks(parallel: parallel)
          end
          File.exists?("down").should be_false
        end
      end

      it "should run tasks in dependency order even if targets are not sorted" do
        with_scenario("empty") do
          # Register the consumer first, so tasks.keys (same size as the
          # registry) is NOT in topological order: that must not bypass
          # the topological sort (issue #23)
          Task.new(output: "downstream", inputs: ["upstream"]) { "downstream data" }
          Task.new(output: "upstream", inputs: [] of String) { "upstream data" }

          TaskManager.run_tasks(TaskManager.tasks.keys, parallel: parallel)

          File.read("upstream").should eq "upstream data"
          File.read("downstream").should eq "downstream data"
        end
      end

      it "should reject unknown targets even when their count matches the registry size" do
        with_scenario("empty") do
          Task.new(output: "out", inputs: [] of String) { "data" }
          # 1 target, 1 registered task: the size check alone would take
          # the fast path and silently skip the unknown target (issue #23)
          expect_raises(Exception, "Unknown output bogus") do
            TaskManager.run_tasks(["bogus"], parallel: parallel)
          end
        end
      end

      it "should run tasks downstream of an input-less task" do
        with_scenario("empty") do
          Task.new(output: "upstream", inputs: [] of String) { "upstream data" }
          Task.new(output: "downstream", inputs: ["upstream"]) { "downstream data" }

          TaskManager.run_tasks(parallel: parallel)

          File.read("upstream").should eq "upstream data"
          File.read("downstream").should eq "downstream data"
        end
      end

      it "should run tasks downstream of an always_run task" do
        with_scenario("empty", to_create: {"seed" => "seed"}) do
          Task.new(output: "upstream", inputs: ["seed"], always_run: true) { "upstream data" }
          Task.new(output: "downstream", inputs: ["upstream"]) { "downstream data" }

          TaskManager.run_tasks(parallel: parallel)

          File.read("upstream").should eq "upstream data"
          File.read("downstream").should eq "downstream data"
        end
      end

      it "should re-run fresh tasks when run_all is true" do
        with_scenario("empty", to_create: {"seed" => "seed"}) do
          runs = 0
          Task.new(output: "counter", inputs: ["seed"]) {
            runs += 1
            "data"
          }

          TaskManager.run_tasks(parallel: parallel)
          runs.should eq 1

          # Everything is fresh now: a normal run skips it
          TaskManager.run_tasks(parallel: parallel)
          runs.should eq 1

          # run_all must run fresh tasks too
          TaskManager.run_tasks(parallel: parallel, run_all: true)
          runs.should eq 2
        end
      end

      it "should not persist input state on a dry run" do
        with_scenario("empty", to_create: {"seed" => "one"}) do
          runs = 0
          Task.new(output: "out", inputs: ["seed"]) {
            runs += 1
            "data"
          }

          # First run builds and records the input hash
          TaskManager.run_tasks(parallel: parallel)
          runs.should eq 1

          # Input changes, then a dry run must not consume the change
          File.write("seed", "two")
          TaskManager.run_tasks(parallel: parallel, dry_run: true)
          runs.should eq 1

          # The real run must still see the modified input and rebuild
          TaskManager.run_tasks(parallel: parallel)
          runs.should eq 2
        end
      end

      it "should run no tasks when dry_run is true" do
        with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
          TaskManager.run_tasks(parallel: parallel, run_all: true, dry_run: true)
          TaskManager.tasks.keys.each do |k|
            File.exists?(k).should be_false
          end
        end
      end

      it "should run all tasks when run_all is true" do
        with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
          TaskManager.run_tasks(parallel: parallel, run_all: true)
          TaskManager.tasks.keys.each do |k|
            File.exists?(k).should be_true
          end
        end
      end

      it "should rerun fresh tasks when run_all is true" do
        marker_proc = TaskProc.new do
          File.open("marker", "a") { |io| io << "x\n" }
          ""
        end
        with_scenario("run_all_fresh",
          to_create: {"in.txt" => "data", "out1" => "existing"},
          procs: {"append_marker" => marker_proc}) do
          # First run: no state file yet, so the task is stale and runs.
          TaskManager.run_tasks(parallel: parallel)
          File.read("marker").lines.size.should eq 1

          # Second run: inputs unchanged and outputs exist, so the task is
          # fresh and must NOT run again without run_all.
          TaskManager.run_tasks(parallel: parallel)
          File.read("marker").lines.size.should eq 1

          # run_all must force execution even though nothing is stale.
          TaskManager.run_tasks(parallel: parallel, run_all: true)
          File.read("marker").lines.size.should eq 2
        end
      end

      it "should save files but respect the no_save flag" do
        with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
          File.exists?("output1").should be_false
          File.exists?("output2").should be_false

          TaskManager.run_tasks(parallel: parallel, run_all: true)

          # The output task has no_save = false, so it should be created
          File.exists?("output1").should be_true
          # The output2 task has no_save = true
          # so it's created by the proc, which creates it
          # with "foo" as the contents
          File.exists?("output2").should be_true
          File.read("output2").should eq "foo"
        end
      end

      it "should run only required tasks to produce specified outputs" do
        with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
          TaskManager.run_tasks(parallel: parallel, targets: ["output4", "output5"])
          File.exists?("output1").should be_false
          File.exists?("output2").should be_false
          File.exists?("output3").should be_true # Required for output4
          File.exists?("output4").should be_true # Required
          File.exists?("output5").should be_true # Required
        end
      end

      it "should fail to run if a task depends on an input that doesn't exist and won't be generated" do
        with_scenario("basic", to_create: {"input2" => "bar"}) do
          expect_raises(Exception, "Unknown inputs") do
            TaskManager.run_tasks(parallel: parallel)
          end
        end
      end

      it "should handle a no_save task that generates multiple outputs" do
        with_scenario("empty") do
          p = TaskProc.new { File.open("output1", "w") << ""; File.open("output2", "w") << ""; "" }
          Task.new(["output1", "output2"], proc: p, no_save: true)
          TaskManager.run_tasks(parallel: parallel)
        end
      end

      it "should support a no_save task with a directory output" do
        with_scenario("empty", to_create: {"seed" => "one"}) do
          producer_runs = 0
          consumer_runs = 0
          Task.new(output: "blog", inputs: ["seed"], no_save: true) {
            producer_runs += 1
            Dir.mkdir_p("blog")
            File.write("blog/post.md", "hi")
            ""
          }
          Task.new(output: "out.md", inputs: ["blog"]) {
            consumer_runs += 1
            File.read("blog/post.md")
          }

          # First run: the producer makes the directory, the consumer
          # runs after it without the run failing on hashing the
          # directory output
          TaskManager.run_tasks(parallel: parallel)
          producer_runs.should eq 1
          consumer_runs.should eq 1
          File.read("out.md").should eq "hi"

          # Second run: the recorded directory digest matches what the
          # input scan computes for "blog", so both tasks stay fresh
          TaskManager.run_tasks(parallel: parallel)
          producer_runs.should eq 1
          consumer_runs.should eq 1

          # Touching a file inside the directory re-stales the consumer
          # (and only the consumer: the producer's own inputs didn't
          # change)
          File.write("blog/post.md", "changed")
          TaskManager.run_tasks(parallel: parallel)
          producer_runs.should eq 1
          consumer_runs.should eq 2
          File.read("out.md").should eq "changed"
        end
      end

      it "should fail if a proc raises an exception" do
        with_scenario("empty") do
          b = TaskProc.new { raise "foo" }
          Task.new(["output2"], proc: b)
          expect_raises(Exception, "Task 052cd9c6f04c::output2 failed: foo") do
            TaskManager.run_tasks(parallel: parallel)
          end
        end
      end

      # This is very hard to assert on parallel execution
      unless parallel
        it "should abort when a proc raises an exception" do
          with_scenario("empty") do
            b = TaskProc.new { raise "foo" }
            Task.new(["output2"], proc: b)
            Task.new(["output1"], proc: TaskProc.new { "foo" })
            # Downstream of the failure: must not run before output2
            Task.new(["output3"], ["output2"] of String, TaskProc.new {
              File.write("downstream_ran", "")
              ""
            })
            expect_raises(Exception, "Task 052cd9c6f04c::output2 failed: foo") do
              TaskManager.run_tasks
            end
            # The dependent of the failing task never executed
            File.exists?("downstream_ran").should be_false
          end
        end
      end

      it "should not abort when a proc raises an exception with keep_going flag" do
        with_scenario("empty") do
          Task.new(["output2"], proc: TaskProc.new { raise "foo" })
          Task.new(["output1"], proc: TaskProc.new { "foo" })
          # Even though a proc raises an exception, it's caught
          TaskManager.run_tasks(parallel: parallel, keep_going: true)
          # It should never have executed the second task
          File.exists?("output1").should be_true
        end
      end

      it "should handle a task that generates multiple outputs" do
        with_scenario("empty") do
          p = TaskProc.new { ["foo", "bar"] }
          Task.new(["output1", "output2"], proc: p)

          TaskManager.run_tasks(parallel: parallel)

          # The two files should be created with the right contents
          File.read("output1").should eq "foo"
          File.read("output2").should eq "bar"
        end
      end

      it "should only run a task that generates multiple outputs once" do
        with_scenario("empty") do
          x = 0
          p = TaskProc.new { x += 1; ["foo #{x}", "bar #{x}"] }
          Task.new(["output1", "output2"], proc: p)

          TaskManager.run_tasks(parallel: parallel)

          # The two files should be created with the right contents
          # if instead of a 1 there is a 2, it means the task was
          # run twice
          File.read("output1").should eq "foo 1"
          File.read("output2").should eq "bar 1"
        end
      end

      it "should fail if a task generates wrong number of outputs" do
        with_scenario("empty") do
          p = TaskProc.new { ["foo", "bar"] }
          Task.new(["output1", "output2", "output3"], proc: p)

          expect_raises(Exception, "correct number of outputs") do
            TaskManager.run_tasks(parallel: parallel)
          end
        end
      end

      it "should fail if a task generates invalid output" do
        with_scenario("empty") do
          # The proc in a task with multiple outputs should return an array
          p = TaskProc.new { "foo" }
          Task.new(["output1", "output2", "output3"], proc: p)

          expect_raises(Exception, "did not return the correct number of outputs") do
            TaskManager.run_tasks(parallel: parallel)
          end
        end
      end

      it "should run tasks marked with 'always_run' even if the dependencies are not changed" do
        x1 = 0
        counter_proc_1 = TaskProc.new {
          x1 += 1
          ""
        }
        x2 = 0
        counter_proc_2 = TaskProc.new {
          x2 += 1
          ""
        }
        with_scenario("empty") do
          # Need to have an input file, because tasks without
          # inputs are implicitly always_run
          File.open("input", "w") << ""
          Task.new(
            inputs: ["input"],
            always_run: true,
            proc: counter_proc_1,
            id: "t1"
          )
          Task.new(
            inputs: ["input"],
            always_run: false,
            proc: counter_proc_2,
            id: "t2"
          )
          x1.should eq 0
          x2.should eq 0
          TaskManager.run_tasks(parallel: parallel)
          x1.should eq 1
          x2.should eq 1
          TaskManager.run_tasks(parallel: parallel)
          x1.should eq 2
          x2.should eq 1
        end
      end
    end
  end

  describe "depends_on" do
    it "should return all targets that depend on a given input" do
      with_scenario("basic") do
        TaskManager.depends_on("input").should eq Set.new(["output3", "output4"])
      end
    end

    it "should return the transitive dependents on a diamond" do
      with_scenario("empty", to_create: {"i" => "data"}) do
        Task.new(output: "o_a", inputs: ["i"]) { "a" }
        Task.new(output: "o_b", inputs: ["i"]) { "b" }
        Task.new(output: "o_c", inputs: ["o_a", "o_b"]) { "c" }

        TaskManager.depends_on("i").should eq Set.new(["o_a", "o_b", "o_c"])
        TaskManager.depends_on(["o_a", "o_b"]).should eq Set.new(["o_c"])
      end
    end
  end

  describe "scan_inputs" do
    it "should calculate hashes for all preexisting inputs" do
      # Even though output3 is an input to a task, it's generated by another
      # So when running from scratch it's not there
      expected = {"input"  => "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33",
                  "input2" => "62cdb7020ff920e5aa642c3d4066950dd1f01f4d"}
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.scan_inputs.should eq expected
      end
    end

    it "should not hash files that don't exist" do
      with_scenario("basic") do
        TaskManager.scan_inputs.size.should eq 0
      end
    end

    it "should hash directories" do
      with_scenario("empty") do
        Dir.mkdir("dir")
        Task.new(
          inputs: ["dir"],
          always_run: true,
          proc: nil,
          id: "t1"
        )
        # Directory hashes use a hash-of-hashes (per-file SHA1 folded into a
        # final SHA1, with explicit framing) so files hash in parallel.
        TaskManager.scan_inputs.should eq({"dir" => "71853c6197a6a7f222db0f1978c7cb232b87c5ee"})
        File.write("dir/input", "foo")
        TaskManager.scan_inputs.should eq({"dir" => "489b711174b065f00574ceab0782f56d99f0bb66"})
        # This mode doesn't ignore file contents
        File.write("dir/input", "bar")
        TaskManager.scan_inputs.should eq({"dir" => "bd39227745e0c9ecfc68e24abe1f6844594ce320"})
        Dir.mkdir("dir/dir1")
        TaskManager.scan_inputs.should eq({"dir" => "e6b2e5679b55e24c3bc87c2c42a54485e312a617"})
      end
    end
    it "should hash directories in fast_dirs mode" do
      with_scenario("empty") do
        TaskManager.fast_dirs = true
        Dir.mkdir("dir")
        Task.new(
          inputs: ["dir"],
          always_run: true,
          proc: nil,
          id: "t1"
        )
        TaskManager.scan_inputs.should eq({"dir" => "da39a3ee5e6b4b0d3255bfef95601890afd80709"})
        File.write("dir/input", "foo")
        TaskManager.scan_inputs.should eq({"dir" => "18e96066fa04ae6c67b5cdcfb02c7c5646ae2402"})
        # This mode ignores file contents
        File.write("dir/input", "bar")
        TaskManager.scan_inputs.should eq({"dir" => "18e96066fa04ae6c67b5cdcfb02c7c5646ae2402"})
        Dir.mkdir("dir/dir1")
        TaskManager.scan_inputs.should eq({"dir" => "f6fa5320de20f424aaab984f56d470386ca9cb96"})
      end
    end

    it "should hash many files in parallel with identical results to serial" do
      with_scenario("empty") do
        # More files than typical CPU count to exercise the worker pool.
        files = {} of String => String
        20.times do |i|
          name = "f#{i}"
          content = "content-#{i}-#{i * i}"
          File.write(name, content)
          files[name] = Digest::SHA1.hexdigest(content)
          Task.new(inputs: [name], always_run: true, proc: nil, id: "t#{i}")
        end

        result = TaskManager.scan_inputs
        result.should eq(files)
      end
    end
  end

  describe "mark_stale_inputs" do
    it "should mark all tasks as stale if there is no .croupier file" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure al tasks run, but no files are marked
        # modified and there is no .croupier file
        tasks = TaskManager.tasks
        TaskManager.run_tasks
        # After a completed run no task reports stale, not even the
        # 2 without inputs (those go stale again at the start of the
        # next run, when propagate_staleness resets them)
        tasks.values.count(&.stale?).should eq 0

        TaskManager.tasks.values.each(&.stale = nil) # Reset to trigger recomputation
        TaskManager.modified.clear
        File.delete(".croupier")
        TaskManager.mark_stale_inputs

        # All 5 tasks should be stale
        tasks.values.count(&.stale?).should eq 5
      end
    end

    it "should mark file with wrong hash as modified" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure no files are modified
        TaskManager.modified.empty?.should be_true
        File.open(".croupier", "w") do |f|
          f.puts(%({
              "__version": "1",
              "input": "thisiswrong",
              "input2": "62cdb7020ff920e5aa642c3d4066950dd1f01f4d",
              "output3": "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          }))
        end

        TaskManager.mark_stale_inputs
        TaskManager.modified.should eq Set{"input"}
      end
    end

    it "should not mark any inputs as modified with a correct .croupier" do
      with_scenario("basic") do
        # Set things up as they should look after running
        File.write("input", "foo")
        File.write("input2", "bar")
        File.write("output1", "")
        File.write("output2", "foo")
        File.write("output3", "")
        File.write("output4", "")
        File.write("output5", "")
        File.write(".croupier", YAML.dump({
          "__version" => "1",
          "input"     => "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33",
          "input2"    => "62cdb7020ff920e5aa642c3d4066950dd1f01f4d",
          "output1"   => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output2"   => "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15",
          "output3"   => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "output4"   => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output5"   => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
        }))
        TaskManager.tasks.size.should eq 5
        TaskManager.mark_stale_inputs
        # Since .croupier describes all inputs, none should be
        # considered modified
        TaskManager.modified.empty?.should be_true
      end
    end

    it "should preserve input hashes across fast-mode runs" do
      with_scenario("empty", to_create: {"seed" => "one"}) do
        runs = 0
        Task.new(output: "out", inputs: ["seed"]) {
          runs += 1
          "data"
        }
        TaskManager.run_tasks
        runs.should eq 1

        # A fresh process starts with no in-memory hashes; and the task
        # must RUN in fast mode (a succeeded task's inputs are not
        # reverted by drop_unfinished_inputs)
        TaskManager.this_run.clear
        TaskManager.fast_mode = true
        sleep 0.01.seconds        # mtime granularity
        File.write("seed", "one") # same content, new mtime
        TaskManager.run_tasks
        runs.should eq 2

        # ...so switching back to hash mode doesn't treat unchanged
        # inputs as modified and force a surprise full rebuild
        TaskManager.fast_mode = false
        TaskManager.run_tasks
        runs.should eq 2
      end
    end

    it "should detect inputs modified during a fast-mode run" do
      with_scenario("empty", to_create: {"seed" => "v1", "mid" => "v1"}) do
        b_runs = 0
        version = 1
        # A rewrites "mid" every time it runs; B consumes "mid"
        Task.new(output: "a_out", inputs: ["seed"]) {
          version += 1
          File.write("mid", "v#{version}")
          "a"
        }
        Task.new(output: "b_out", inputs: ["mid"]) {
          b_runs += 1
          "b"
        }

        # Hash mode: A rewrites mid, B consumes it after A
        TaskManager.run_tasks
        b_runs.should eq 1

        # Fast mode: A runs again and rewrites mid DURING the run
        # (after the mtime scan). Later fast runs must still see that
        # mid changed, instead of missing it forever because its mtime
        # predates the state-file save
        TaskManager.fast_mode = true
        File.write("seed", "v2")
        TaskManager.run_tasks
        TaskManager.run_tasks
        (b_runs > 1).should be_true
      end
    end

    it "should mark as modified all inputs newer than .croupier when in fast mode" do
      with_scenario("basic") do
        TaskManager.fast_mode = true
        # Set things up as they should look after running
        File.write("input", "foo")
        File.write("input2", "bar")
        File.write("output1", "")
        File.write("output2", "foo")
        File.write("output4", "")
        File.write("output5", "")
        # Ensure the files are strictly older than .croupier: same-tick
        # mtime granularity could otherwise make them look newer
        sleep 0.01.seconds
        File.write(".croupier", YAML.dump({
          "input"   => "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15",
          "input2"  => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output1" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output2" => "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15",
          "output3" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output4" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output5" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
        }))
        sleep 0.01.seconds
        File.write("output3", "")
        TaskManager.mark_stale_inputs
        TaskManager.tasks.size.should eq 5
        # output3 is newer than .croupier so is considered modified
        # in fast_mode
        TaskManager.modified.should eq Set{"output3"}
      end
    end
  end
end
