require "./spec_helper"
require "file_utils"
include Croupier

describe "TaskManager" do
  describe "task creation during runs" do
    it "should reject task creation while a parallel run is in progress" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        rejected = false
        Task.new(id: "creator", output: "out_c", inputs: ["seed"]) do
          caught = begin
            Task.new(id: "mid_run", output: "out_m") { "m" }
            nil
          rescue ex : Croupier::UsageError
            ex
          end
          caught.should be_a(Croupier::UsageError)
          rejected = true
          # The registry is untouched: no half-created task
          TaskManager.tasks.has_key?("out_m").should be_false
          TaskManager.tasks_by_id.has_key?("mid_run").should be_false
          nil
        end

        # The rejection is a task failure (the proc raised), reported
        # through the run's normal error path
        expect_raises(Croupier::RunFailure) { TaskManager.run_tasks(parallel: true) }
        TaskManager.tasks_by_id.has_key?("mid_run").should be_false
      end
    end

    it "should reject remove_task while a run is in progress" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        Task.new(id: "target", output: "out_t", inputs: ["seed"]) { "t" }
        Task.new(id: "mutator", output: "out_m", inputs: ["seed"]) do
          begin
            TaskManager.remove_task("out_t")
          rescue Croupier::UsageError
            nil
          end
          # The registry is untouched: the task is still registered
          TaskManager.tasks.has_key?("out_t").should be_true
          nil
        end

        # Serial
        expect_raises(Croupier::RunFailure) { TaskManager.run_tasks }
        TaskManager.tasks.has_key?("out_t").should be_true

        # And parallel (mutator re-runs via run_all)
        expect_raises(Croupier::RunFailure) { TaskManager.run_tasks(run_all: true, parallel: true) }
        TaskManager.tasks.has_key?("out_t").should be_true
      end
    end

    it "should reject task creation while a serial run is in progress" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        Task.new(id: "creator", output: "out_c", inputs: ["seed"]) do
          begin
            Task.new(id: "mid_run", output: "out_m") { "m" }
          rescue Croupier::UsageError
            nil
          end
          nil
        end

        expect_raises(Croupier::RunFailure) { TaskManager.run_tasks }
        TaskManager.tasks_by_id.has_key?("mid_run").should be_false
      end
    end
  end

  describe "k/v store modification detection" do
    it "should re-run tasks when k/v store values are modified via set()" do
      with_scenario("empty") do
        # Set initial value
        TaskManager.set("test_key", "100")

        # Create a task that depends on the k/v store value
        Task.new(
          id: "kv_task",
          inputs: ["kv://test_key"],
          outputs: ["kv://output_key"],
        ) do
          value = TaskManager.get("test_key") || "0"
          (value.to_i * 2).to_s
        end

        # First run - should work correctly
        TaskManager.run_tasks
        TaskManager.get("output_key").should eq "200"

        # Change the k/v store value
        TaskManager.set("test_key", "200")

        # Second run - should re-run the task because the input changed
        TaskManager.run_tasks
        TaskManager.get("output_key").should eq "400"
      end
    end

    it "should detect multiple k/v store modifications in a single run" do
      with_scenario("empty") do
        # Set initial values
        TaskManager.set("key1", "10")
        TaskManager.set("key2", "20")

        # Create tasks that depend on k/v store values
        Task.new(
          id: "task1",
          inputs: ["kv://key1"],
          outputs: ["kv://out1"],
        ) do
          val = TaskManager.get("key1")
          ((val || "0").to_i * 2).to_s
        end

        Task.new(
          id: "task2",
          inputs: ["kv://key2"],
          outputs: ["kv://out2"],
        ) do
          val = TaskManager.get("key2")
          ((val || "0").to_i * 3).to_s
        end

        # First run
        TaskManager.run_tasks
        TaskManager.get("out1").should eq "20"
        TaskManager.get("out2").should eq "60"

        # Modify both k/v store values
        TaskManager.set("key1", "100")
        TaskManager.set("key2", "200")

        # Second run - should re-run both tasks
        TaskManager.run_tasks
        TaskManager.get("out1").should eq "200"
        TaskManager.get("out2").should eq "600"
      end
    end
  end

  describe "early cutoff optimization" do
    it "should not rebuild downstream tasks when upstream output is unchanged" do
      with_scenario("empty") do
        # Create a chain: file1 -> file2 -> file3
        # file2 normalizes content (e.g., trim whitespace, normalize)

        # Create file1 with trailing space
        File.write("file1", "hello   ")

        # Task 1: file1 -> file2 (strip and uppercase)
        t1_run_count = 0
        Task.new(
          inputs: ["file1"],
          outputs: ["file2"],
        ) do
          t1_run_count += 1
          File.read("file1").strip.upcase
        end

        # Task 2: file2 -> file3 (add suffix)
        t2_run_count = 0
        Task.new(
          inputs: ["file2"],
          outputs: ["file3"],
        ) do
          t2_run_count += 1
          File.read("file2") + "_SUFFIX"
        end

        # First run - both tasks should run
        TaskManager.run_tasks
        t1_run_count.should eq 1
        t2_run_count.should eq 1
        File.read("file2").should eq "HELLO"
        File.read("file3").should eq "HELLO_SUFFIX"

        # Modify file1 with different formatting but same output after processing
        File.write("file1", "hello") # No trailing space, still produces "HELLO"

        # Second run - t1 should run, but t2 should NOT run (early cutoff)
        TaskManager.run_tasks
        t1_run_count.should eq 2 # t1 ran again (file1 changed)
        t2_run_count.should eq 1 # t2 was skipped (early cutoff - file2 unchanged!)
      end
    end

    it "should rebuild downstream tasks when upstream output changes" do
      with_scenario("empty") do
        # Create a chain: file1 -> file2 -> file3

        File.write("file1", "hello")

        t1_run_count = 0
        Task.new(
          inputs: ["file1"],
          outputs: ["file2"],
        ) do
          t1_run_count += 1
          File.read("file1").upcase
        end

        t2_run_count = 0
        Task.new(
          inputs: ["file2"],
          outputs: ["file3"],
        ) do
          t2_run_count += 1
          File.read("file2") + "_SUFFIX"
        end

        # First run
        TaskManager.run_tasks
        t1_run_count.should eq 1
        t2_run_count.should eq 1

        # Modify file1 with DIFFERENT content
        File.write("file1", "world")

        # Second run - both should run
        TaskManager.run_tasks
        t1_run_count.should eq 2
        t2_run_count.should eq 2 # t2 ran because file2 changed
        File.read("file2").should eq "WORLD"
        File.read("file3").should eq "WORLD_SUFFIX"
      end
    end

    it "should rebuild dependent tasks that have multiple stale dependencies" do
      with_scenario("empty") do
        # t3 depends on both t1 (file2) and t2 (file4)
        # If t1's output is unchanged but t2's output CHANGES, t3 should still run

        File.write("file1", "hello")
        File.write("file3", "world")

        # Task 1: file1 -> file2 (uppercase)
        t1_run_count = 0
        Task.new(
          inputs: ["file1"],
          outputs: ["file2"],
        ) do
          t1_run_count += 1
          File.read("file1").upcase
        end

        # Task 2: file3 -> file4 (uppercase)
        t2_run_count = 0
        Task.new(
          inputs: ["file3"],
          outputs: ["file4"],
        ) do
          t2_run_count += 1
          File.read("file3").upcase
        end

        # Task 3: depends on both file2 and file4
        t3_run_count = 0
        Task.new(
          inputs: ["file2", "file4"],
          outputs: ["file5"],
        ) do
          t3_run_count += 1
          File.read("file2") + "_" + File.read("file4")
        end

        # First run - all tasks should run
        TaskManager.run_tasks
        t1_run_count.should eq 1
        t2_run_count.should eq 1
        t3_run_count.should eq 1
        File.read("file2").should eq "HELLO"
        File.read("file4").should eq "WORLD"
        File.read("file5").should eq "HELLO_WORLD"

        # Modify file1 with different formatting but same output (file2 unchanged)
        File.write("file1", "hello") # Same content

        # Second run - t1 doesn't run (file1 unchanged)
        # t2 doesn't run (file3 unchanged)
        # t3 doesn't run (both inputs unchanged)
        TaskManager.run_tasks
        t1_run_count.should eq 1
        t2_run_count.should eq 1
        t3_run_count.should eq 1

        # Now modify file3 - t2 should run and produce DIFFERENT file4
        # t3 SHOULD run because file4 changed
        File.write("file3", "earth") # Different content!

        TaskManager.run_tasks
        t1_run_count.should eq 1 # t1 didn't run
        t2_run_count.should eq 2 # t2 ran
        t3_run_count.should eq 2 # t3 ran because file4 changed!

        # Verify file5 is updated
        File.read("file2").should eq "HELLO"
        File.read("file4").should eq "EARTH"
        File.read("file5").should eq "HELLO_EARTH"
      end
    end
  end

  describe "state file" do
    it "should recover from a corrupted state file" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        runs = 0
        Task.new(output: "out", inputs: ["seed"]) {
          runs += 1
          "d"
        }
        TaskManager.run_tasks
        runs.should eq 1

        # A crash mid-write leaves a truncated state file: the next run
        # must recover by treating everything as modified, not raise
        File.write(".croupier", "{{{ not yaml")
        TaskManager.run_tasks
        runs.should eq 2
      end
    end

    it "should recover from a state file holding valid YAML that is not a mapping" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        runs = 0
        Task.new(output: "out", inputs: ["seed"]) {
          runs += 1
          "d"
        }
        TaskManager.run_tasks
        runs.should eq 1

        # A scalar is valid YAML but not a state file: loading it must
        # self-heal (full rebuild) instead of crashing with a cast error
        File.write(".croupier", "just a scalar\n")
        TaskManager.run_tasks
        runs.should eq 2

        # Same for a list
        File.write(".croupier", "- a\n- b\n")
        TaskManager.run_tasks
        runs.should eq 3
      end
    end

    it "should version the state file and discard older formats" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        runs = 0
        Task.new(output: "out", inputs: ["seed"]) {
          runs += 1
          "d"
        }
        TaskManager.run_tasks
        runs.should eq 1

        # The saved state carries a schema version
        YAML.parse(File.read(".croupier"))["__version"]?.should_not be_nil

        # A state file without it (as written by older croupiers) is
        # discarded even if the hashes look current: comparing hashes
        # computed by a different scheme would silently skip rebuilds
        state = {"seed" => Digest::SHA1.hexdigest(File.read("seed"))}
        File.write(".croupier", YAML.dump(state))
        TaskManager.run_tasks
        runs.should eq 2
      end
    end

    it "should not leave temporary files behind" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        Task.new(output: "out", inputs: ["seed"]) { "d" }
        TaskManager.run_tasks
        File.exists?(".croupier").should be_true
        File.exists?(".croupier.tmp").should be_false
      end
    end
  end

  describe "progress_callback" do
    it "should be called with the task id when a task runs" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        reported = [] of String
        TaskManager.progress_callback = ->(id : String) { reported << id }
        Task.new(output: "out", inputs: ["seed"]) { "data" }

        TaskManager.run_tasks

        reported.size.should eq 1
        reported.first.should eq TaskManager.tasks["out"].id
      end
    end

    it "should be called once per task of a parallel run, on worker fibers" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        # The callback fires on worker fibers, so the shared arrays
        # need a guard the serial case never exercises. Work stealing
        # does not guarantee every spawned worker receives a task, so
        # the worker-name set is asserted to be a non-empty subset of
        # the pool, not equal to it.
        lock = Sync::Mutex.new
        reported = [] of {String, String}
        TaskManager.progress_callback = ->(id : String) {
          lock.synchronize { reported << {id, Fiber.current.name || ""} }
        }
        4.times { |i| Task.new(output: "out_#{i}", inputs: ["seed"]) { "data_#{i}" } }

        TaskManager.run_tasks(parallel: true)

        reported.size.should eq 4
        expected = (0...4).map { |i| TaskManager.tasks["out_#{i}"].id }.to_set
        reported.map(&.[0]).to_set.should eq expected
        worker_names = reported.map(&.[1]).to_set
        worker_names.all?(&.starts_with?("croupier-worker-")).should be_true
        worker_names.size.should be > 0
        worker_names.size.should be <= 4
      end
    end
  end

  describe "no_save with kv outputs" do
    it "should let the proc store the kv data itself" do
      with_scenario("empty") do
        # A no_save task is responsible for saving its own outputs: for
        # a kv:// output that means calling set() from the proc
        Task.new(output: "kv://k", inputs: [] of String, no_save: true) {
          TaskManager.set("k", "from proc")
          nil
        }
        TaskManager.run_tasks

        TaskManager.get("k").should eq "from proc"
      end
    end
  end

  describe "cleanup" do
    it "should reset session state" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        # Dirty up everything a session can carry
        TaskManager.state_file = "custom_state"
        TaskManager.early_cutoff = false
        TaskManager.fast_mode = true
        TaskManager.fast_dirs = true
        TaskManager.auto_mode = true
        TaskManager.add_mutex("db")
        TaskManager.before_run_hook = ->(_changes : Set(String)) { File.write("hook_fired", "") }
        TaskManager.progress_callback = ->(_id : String) { File.write("progress_fired", "") }
        # And the caches and run state
        TaskManager.set("k", "v")
        TaskManager.modified << "ghost"
        Task.new(output: "out", inputs: ["seed"]) { "data" }
        TaskManager.run_tasks
        # The dirty run fired the still-installed hooks; remove their
        # markers so the post-cleanup check below starts clean
        File.delete?("progress_fired")
        File.delete?("hook_fired")

        TaskManager.cleanup

        TaskManager.state_file.should eq ".croupier"
        TaskManager.early_cutoff?.should be_true
        # Every mode flag must reset: a stale one silently changes the
        # behavior of the next session (this bit fast_dirs once)
        TaskManager.fast_mode?.should be_false
        TaskManager.fast_dirs?.should be_false
        TaskManager.auto_mode?.should be_false
        TaskManager.mutexes.empty?.should be_true
        TaskManager.modified.empty?.should be_true
        TaskManager.@existing_files.empty?.should be_true
        TaskManager.@store_cache.empty?.should be_true
        # Through the accessor: the cache is nil-when-invalid now,
        # and cleanup must leave it invalidated
        TaskManager.all_inputs.empty?.should be_true

        # The hooks are gone: running a task must not fire them
        Task.new(output: "out2", inputs: ["seed"]) { "data" }
        TaskManager.run_tasks
        File.exists?("hook_fired").should be_false
        File.exists?("progress_fired").should be_false
      end
    end

    it "should stop the autorun fiber" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        Task.new(output: "out", inputs: ["seed"]) { "data" }

        baseline = live_fiber_count
        TaskManager.auto_run
        # The autorun fiber (plus the watcher's) are running
        wait_until(message: "autorun fibers never started") { live_fiber_count > baseline }
        during = live_fiber_count

        TaskManager.cleanup
        TaskManager.@autorun_running.get.should be_false
        # The autorun fiber and the watcher's reader are gone. (One
        # filesystem event-loop fiber stays parked on the library's own
        # channel forever — an upstream leak croupier can't retire.)
        wait_until(message: "autorun fibers never stopped") { live_fiber_count < during }
      end
    end
  end

  describe "fiber hygiene" do
    it "should not leak worker fibers from parallel runs" do
      seeds = (0...20).map { |i| {"seed_#{i}" => "data"} of String => String }
        .reduce { |acc, hash| acc.merge(hash) }
      with_scenario("empty", to_create: seeds) do
        seeds.each_key { |k| Task.new(output: "out_#{k}", inputs: [k]) { "data" } }

        # Count only croupier's own worker fibers, by name: the raw
        # registry also holds stdlib thread infrastructure that comes
        # and goes with load (GC marker roots, the thread pool's lazy
        # main-thread loop — see #64), so a whole-registry baseline
        # compares croupier against noise it does not control
        worker_count = -> {
          count = 0
          Fiber.each { |fiber| count += 1 if fiber.name.try(&.starts_with?("croupier-worker")) }
          count
        }

        # First round also absorbs the one-time execution-context
        # resize; its workers must all retire
        TaskManager.run_tasks(parallel: true)
        5.times { TaskManager.scan_inputs }
        wait_until(message: "round 1 worker fibers never exited") { worker_count.call == 0 }

        # A second round of the same width: worker fibers that drained
        # their (closed) queue exit, fibers parked on a never-closed
        # channel would accumulate forever. (All seeds change so the
        # wave width matches the first round.)
        seeds.each_key { |k| File.write(k, "changed") }
        TaskManager.run_tasks(parallel: true)
        5.times { TaskManager.scan_inputs }
        wait_until(message: "worker fibers never exited") { worker_count.call == 0 }
      end
    end
  end
end
