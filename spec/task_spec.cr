require "./spec_helper"
require "file_utils"
include Croupier

describe "Task" do
  describe "serialization" do
    it "should round-trip through YAML" do
      with_scenario("empty") do
        task = Task.new(output: "out", inputs: ["seed"], no_save: true, always_run: true)

        restored = Task.from_yaml(task.to_yaml)

        restored.@outputs.should eq ["out"]
        restored.@inputs.should eq Set.new(["seed"])
        restored.no_save?.should be_true
        restored.always_run?.should be_true
        # procs and runtime state are not serialized
        restored.@procs.should be_empty
      end
    end

    it "should have a nice string representation" do
      with_scenario("basic") do
        id = "77012200e4c39aa279b0d3e16dca43a7b02eb4a5"
        TaskManager.tasks["output1"].to_s.should eq "#{id}::output1"
      end
    end

    it "should be yaml serializable" do
      with_scenario("basic") do
        expected = {
          "id"          => "77012200e4c39aa279b0d3e16dca43a7b02eb4a5",
          "inputs"      => [] of String,
          "outputs"     => ["output1"],
          "always_run"  => false,
          "no_save"     => false,
          "mergeable"   => true,
          "master_task" => false,
        }
        YAML.parse(TaskManager.tasks["output1"].to_yaml).should eq expected
      end
    end
  end

  describe "task semantics small items" do
    it "should normalize input and output paths" do
      with_scenario("empty", to_create: {"seed" => "x"}) do
        Task.new(output: "a_out", inputs: ["seed"]) { "a" }
        # Unnormalized references to the same files must be the same
        # graph vertices, so the dependency edge exists
        Task.new(output: "b_out", inputs: ["./a_out", "dir/../seed"]) { "b" }

        TaskManager.all_inputs.should contain("seed")
        TaskManager.all_inputs.should_not contain("./a_out")

        sorted = TaskManager.sorted_task_graph[1]
        # The edge a_out -> b_out puts a_out first
        producer_position = sorted.index("a_out") || sorted.size
        consumer_position = sorted.index("b_out") || sorted.size
        (producer_position < consumer_position).should be_true

        # Directory inputs lose the trailing slash
        Task.new(output: "c_out", inputs: ["dir/"]) { "c" }
        TaskManager.all_inputs.should contain("dir")
      end
    end

    it "should reject empty kv:// keys" do
      with_scenario("empty") do
        expect_raises(Exception, "empty kv:// key") do
          Task.new(output: "kv://", inputs: [] of String) { "x" }
        end
        expect_raises(Exception, "empty kv:// key") do
          Task.new(output: "ok", inputs: ["kv://"]) { "x" }
        end
      end
    end

    it "should hash dotfiles inside directory inputs" do
      with_scenario("empty") do
        Dir.mkdir("dir")
        File.write("dir/.env", "one")
        Task.new(output: "out", inputs: ["dir"]) { "x" }

        first = TaskManager.scan_inputs["dir"]
        File.write("dir/.env", "two")
        TaskManager.scan_inputs["dir"].should_not eq first
      end
    end

    it "should reject duplicate explicit ids on output-ful tasks" do
      with_scenario("empty") do
        Task.new(id: "dup", outputs: ["a"]) { "x" }
        expect_raises(Exception, "already used") do
          Task.new(id: "dup", outputs: ["b"]) { "x" }
        end
        # Output-less tasks may still merge under a shared id
        Task.new(id: "shared")
        Task.new(id: "shared")
        TaskManager.tasks.keys.should contain("shared")
      end
    end

    it "should use wider computed ids" do
      with_scenario("empty") do
        Task.new(output: "out1", inputs: [] of String) { "x" }
        # 12 hex chars = 48 bits: no realistic birthday collisions
        TaskManager.tasks["out1"].@id.size.should eq 12
      end
    end

    it "should discard (with a warning) surplus proc results" do
      with_scenario("empty") do
        # Two results for one declared output: the extra is discarded
        # and the declared output still gets the first result
        Task.new(output: "out", inputs: [] of String) { ["first", "second"] }
        TaskManager.run_tasks
        File.read("out").should eq "first"
      end
    end
  end

  describe "mutex" do
    it "should lock via the mutex given to the block initializer" do
      with_scenario("empty") do
        inside = 0
        max_overlap = 0
        2.times do |i|
          Task.new(output: "out_#{i}", inputs: [] of String, mutex: "db") {
            inside += 1
            current = inside
            max_overlap = current if current > max_overlap
            sleep 0.05.seconds
            inside -= 1
            "x"
          }
        end
        TaskManager.run_tasks(parallel: true)
        # The two procs share the "db" mutex: they must never overlap
        max_overlap.should eq 1
      end
    end

    it "should register mutexes set through the setter" do
      with_scenario("empty") do
        task = Task.new(output: "out", inputs: [] of String) { "x" }
        task.mutex = "db"
        # Running must not KeyError on the unregistered mutex
        task.run
        File.exists?("out").should be_true
      end
    end

    it "should keep the mutex of merged tasks and reject mismatches" do
      with_scenario("empty") do
        first = Task.new(output: "o", inputs: [] of String, mutex: "db") { "a" }
        first.mutex.should eq "db"

        # A colliding task with the SAME mutex merges and keeps it
        Task.new(outputs: ["o", "o2"], inputs: [] of String, mutex: "db") { "b" }
        TaskManager.tasks["o"].should eq first
        first.mutex.should eq "db"

        # A colliding task with a DIFFERENT mutex is refused whole
        expect_raises(Exception, "different mutexes") do
          Task.new(output: "o", inputs: [] of String, mutex: "other") { "c" }
        end
        # And the registry is untouched by the refusal
        TaskManager.tasks["o"].should eq first
      end
    end

    it "should merge subtask ids" do
      with_scenario("empty") do
        first = Task.new(output: "o", inputs: [] of String) { "a" }
        first.subtask_ids << "s1"
        second = Task.new(output: "p", inputs: [] of String) { "b" }
        second.subtask_ids << "s2"

        first.merge(second)
        first.subtask_ids.should eq Set.new(["s1", "s2"])
      end
    end
  end

  describe "new" do
    it "should be possible to create a task and fetch it" do
      with_scenario("basic") do
        t = TaskManager.tasks["output1"]
        t.@outputs.should eq ["output1"]
        t.@inputs.empty?.should be_true
        t.stale?.should be_true
      end
    end

    it "should be possible to create tasks without output and fetch them" do
      with_scenario("empty") do
        Task.new(id: "t1")
        Task.new(id: "t1")
        Task.new(id: "t2")

        TaskManager.tasks.keys.should eq ["t1", "t2"]
      end
    end

    it "should allow a task to depend on a task without output referenced by id" do
      with_scenario("empty") do
        Task.new(inputs: ["t2"], id: "t1")
        Task.new(id: "t2")

        TaskManager.tasks.keys.should eq ["t1", "t2"]
        # Should respect dependencies even if they are just IDs
        TaskManager.sorted_task_graph[1].should eq ["t2", "t1"]
      end
    end

    it "should reject self-cyclical tasks" do
      with_scenario("basic") do
        expect_raises(Exception, "Cycle detected") do
          Task.new("output6", ["input.txt", "output6"])
        end
      end
    end

    it "should allow creating two tasks with the same output" do
      with_scenario("empty") do
        dummy_proc = TaskProc.new { "" }
        t1 = Task.new("output", ["i1"] of String, dummy_proc)
        Task.new("output", ["i2"] of String, dummy_proc)

        # t2 is merged into t1
        TaskManager.tasks["output"].should eq t1
        t1.@inputs == ["i1", "i2"]
      end
    end

    it "should handle complex merges" do
      with_scenario("empty") do
        d1 = TaskProc.new { "1" }
        d2 = TaskProc.new { "2" }
        d3 = TaskProc.new { "3" }

        # All these tasks should be merged into one and registered
        # that one into all outputs, to avoid duplicating procs
        t1 = Task.new(["o1", "o2"], ["i1"] of String, d1)
        Task.new(["o1", "o3"], ["i2"] of String, d2)
        Task.new(["o2", "o3"], ["i1", "i3"] of String, d3)

        # All tasks are merged into t1
        TaskManager.tasks["o1"].should eq t1
        TaskManager.tasks["o2"].should eq t1
        TaskManager.tasks["o3"].should eq t1

        # t1 has all 3 inputs, not repeated
        t1.inputs.should eq Set.new(["i1", "i2", "i3"])

        # t1 has all 3 outputs, repeated as needed
        t1.outputs.should eq ["o1", "o2", "o1", "o3", "o2", "o3"]

        # t1 has all 3 procs
        t1.@procs.should eq [d1, d2, d3]
      end
    end

    it "should not merge tasks marked as not mergeable" do
      with_scenario("empty") do
        Task.new(["o1"], ["i1"] of String)
        expect_raises(Exception, "Can't merge task") do
          Task.new(["o1"], ["i2"] of String, mergeable: false)
        end
      end
    end

    it "should not merge into tasks marked as not mergeable" do
      with_scenario("empty") do
        Task.new(["o1"], ["i1"] of String, mergeable: false)
        expect_raises(Exception, "Can't merge task") do
          Task.new(["o1"], ["i2"] of String)
        end
      end
    end

    it "should allow creating tasks with more than one output" do
      with_scenario("empty") do
        t1 = Task.new(["output1", "output2"])

        # Should be visible in two places
        TaskManager.tasks["output1"].should eq t1
        TaskManager.tasks["output2"].should eq t1
      end
    end

    it "should allow creating tasks using @store as i/o" do
      Task.new(outputs: ["kv://o1", "kv://o2"], inputs: ["kv://i1"])
    end
  end

  describe "merge" do
    it "should not allow merging tasks with different `no_save`" do
      with_scenario("empty") do
        Task.new("output", no_save: true)
        expect_raises(Exception, "different no_save settings") do
          Task.new("output", no_save: false)
        end
      end
    end

    it "should not allow merging tasks with different `always_run`" do
      with_scenario("empty") do
        Task.new("output", always_run: true)
        expect_raises(Exception, "different always_run settings") do
          Task.new("output", always_run: false)
        end
      end
    end

    it "should leave the registry untouched when a multi-way merge fails" do
      with_scenario("empty") do
        e1_proc = TaskProc.new { "e1" }
        e2_proc = TaskProc.new { "e2" }
        e1 = Task.new("o1", [] of String, e1_proc, no_save: true)
        e2 = Task.new("o2", [] of String, e2_proc, no_save: true)

        # E1 and E2 are compatible with each other but not with the new
        # task, so the reduce merges E1+E2 first (mutating E1 in place)
        # and only then raises
        expect_raises(Exception, "different no_save settings") do
          Task.new(["o1", "o2"], [] of String, TaskProc.new { "n" }, no_save: false)
        end

        # The registry must be exactly as it was before the failed merge
        TaskManager.tasks["o1"].should eq e1
        TaskManager.tasks["o2"].should eq e2
        e1.@outputs.should eq ["o1"]
        e1.@inputs.should eq Set.new([] of String)
        e1.@procs.should eq [e1_proc]
        e2.@procs.should eq [e2_proc]
      end
    end

    it "should add the effects of the merged task to the first one" do
      with_scenario("empty") do
        proc1 = TaskProc.new { File.open("1", "w") << ""; "foo" }
        proc2 = TaskProc.new { File.open("2", "w") << ""; "bar" }
        t1 = Task.new("output", [] of String, proc1)
        Task.new("output", [] of String, proc2)

        # t2 merges into t1
        TaskManager.tasks["output"].should eq t1

        TaskManager.run_tasks

        # output should have result of t2
        File.read("output").should eq "bar"

        # Files 1 and 2 should exist because both procs ran
        File.exists?("1").should be_true
        File.exists?("2").should be_true
      end
    end

    it "should add the outputs of the merged task to the first one" do
      with_scenario("empty") do
        proc1 = TaskProc.new { File.open("1", "w") << ""; ["foo1", "foo2"] }
        proc2 = TaskProc.new { File.open("2", "w") << ""; ["bar1", "bar2"] }
        t1 = Task.new(["output", "output2"], [] of String, proc1)
        Task.new(["output", "output3"], [] of String, proc2)

        # t2 merges into t1, which is registered in the 3 outputs
        TaskManager.tasks["output"].should eq t1
        TaskManager.tasks["output2"].should eq t1
        TaskManager.tasks["output3"].should eq t1
        # Yes, output is there twice, because it will be written twice!
        t1.outputs.should eq ["output", "output2", "output", "output3"]

        TaskManager.run_tasks

        # output should have result of t2
        File.read("output").should eq "bar1"

        # The other outputs should be ok
        File.read("output2").should eq "foo2"
        File.read("output3").should eq "bar2"

        # Files 1 and 2 should exist because both procs ran
        File.exists?("1").should be_true
        File.exists?("2").should be_true
      end
    end
  end

  describe "run" do
    it "should execute the task's proc when called" do
      with_scenario("empty") do
        y = x = 0
        b = TaskProc.new {
          x += 1
          File.write("output2", "foo")
          ""
        }
        t = Task.new(
          "output2",
          [] of String,
          b,
          no_save: true)
        t.run
        x.should eq y + 1
        t.run
        x.should eq y + 2
      end
    end

    it "should fail if a no_save task doesn't generate the output when called" do
      with_scenario("empty") do
        b = TaskProc.new {
          ""
        }
        t = Task.new(
          "output2",
          [] of String,
          b,
          no_save: true)
        expect_raises(Exception, "Task 052cd9c6f04c::output2 did not generate output2") do
          t.run
        end
      end
    end

    it "should fail if the proc raises an exception" do
      with_scenario("empty") do
        b = TaskProc.new { raise "foo" }
        t = Task.new("output2", proc: b)
        expect_raises(Exception, "Task 052cd9c6f04c::output2 failed: foo") do
          t.run
        end
      end
    end

    it "should raise TaskFailure with the original exception as cause" do
      with_scenario("empty") do
        t = Task.new("output2", proc: TaskProc.new { raise ArgumentError.new("bad argument") })
        ex = expect_raises(TaskFailure) { t.run }
        ex.message.should eq("Task 052cd9c6f04c::output2 failed: bad argument")
        cause = ex.cause
        cause.should be_a(ArgumentError)
        cause.as(ArgumentError).message.should eq("bad argument")
        # The cause keeps the backtrace of the failing proc, not the wrapper's
        cause.as(Exception).backtrace.first.should contain("task_spec")
      end
    end

    it "should propagate TaskFailure through run_tasks" do
      with_scenario("empty") do
        Task.new("output2", inputs: [] of String, proc: TaskProc.new { raise "foo" })
        Task.new("output3", inputs: ["output2"], proc: TaskProc.new { "" })
        # run_tasks wraps the failure in a RunFailure; the TaskFailure
        # (with the proc's exception as its cause) is carried in #errors
        failure = expect_raises(RunFailure, "Task 052cd9c6f04c::output2 failed: foo") do
          TaskManager.run_tasks
        end
        failure.errors.size.should eq 1
        failure.errors.first.should be_a(TaskFailure)
        failure.errors.first.cause.should_not be_nil
      end
    end

    it "should record hash for outputs in the TaskManager" do
      with_scenario("empty") do
        t = Task.new(
          "output2",
          [] of String,
          TaskProc.new {
            "sarasa"
          },
        )
        t.run
        # this is the sha1sum of "sarasa"
        TaskManager.next_run["output2"].should eq \
          "609df08764e873e6f090a0064b38b2c5422cdf87"
      end
    end

    it "should run if inputs are k/v store" do
      with_scenario("empty") do
        proc = TaskProc.new {
          x = TaskManager.get("i1").to_s
          ["sarasa", x]
        }
        t = Task.new(outputs: ["kv://o1", "kv://o2"], inputs: ["kv://i1"], proc: proc)
        TaskManager.set("i1", "foo")
        t.run
        TaskManager.get("o1").should eq "sarasa"
        TaskManager.get("o2").should eq "foo"
      end
    end
  end

  describe "stale?" do
    it "should make a task stale if its input is marked modified" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        t = TaskManager.tasks["output3"]
        t.stale.should be_false
        t.stale = nil # Reset to trigger recalculation
        t.stale.should be_nil
        TaskManager.modified.clear
        TaskManager.modified << "input"
        t.stale?.should be_true
      end
    end

    it "should mark a task stale if a task it depends on is stale" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        t = TaskManager.tasks["output4"]
        TaskManager.modified.clear
        # Force recalculation of stale states
        TaskManager.tasks.values.each do |task|
          task.stale = nil
        end
        # input is not a direct dependency of t, but an indirect one
        TaskManager.modified << "input"
        t.stale?.should be_true
      end
    end

    it "should invalidate tasks which indirectly depend on modified files" do
      with_scenario("empty") do
        t1 = Task.new(id: "t1", inputs: ["input"], outputs: ["output1"]) {
          File.read("input").downcase
        }
        t2 = Task.new(id: "t2", inputs: ["output1"], outputs: ["output2"]) {
          File.read("output1").upcase
        }

        File.write("input", "Foo")
        TaskManager.run_tasks
        File.read("output1").should eq "foo"
        File.read("output2").should eq "FOO"
        t1.stale?.should be_false
        t2.stale?.should be_false
        File.write("input", "Bar")
        TaskManager.mark_stale_inputs
        # Set to nil to force recalculation
        t1.stale = nil
        t2.stale = nil
        t1.stale?.should be_true
        t2.stale?.should be_true

        TaskManager.run_tasks
        File.read("output1").should eq "bar"
        File.read("output2").should eq "BAR"
      end
    end

    it "should mark tasks depending (in)directly on a modified file as stale" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure all outputs exists and no files are modified
        tasks = TaskManager.tasks
        tasks.size.should eq 5
        TaskManager.run_tasks
        TaskManager.modified.clear
        tasks.values.each(&.stale = nil) # Reset to trigger recomputation
        # All tasks are reset so their state is recalculated
        tasks.values.count(&.stale.nil?).should eq 5

        # Only input is modified
        TaskManager.modified << "input"

        # Only tasks depending on "input" or that have no inputs should be stale
        # tasks.values.count(&.stale?).should eq 4
        tasks.keys.select { |k| tasks[k].stale? }.should eq ["output1", "output2", "output3", "output4"]
      end
    end

    it "should mark tasks as stale if the output doesn't exist" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        t = TaskManager.tasks["output1"]
        t.stale = nil # Reset to trigger recalculation
        t.stale.should be_nil
        File.delete?("output1")
        t.stale?.should be_true
      end
    end

    it "should not consider tasks with kv inputs as stale unless modified" do
      with_scenario("empty") do
        t = Task.new(id: "t", inputs: ["kv://foo"])
        t.run
        t.stale = nil # Reset to trigger recomputation
        t.stale?.should be_false
        TaskManager.modified << "kv://foo"
        t.stale = nil # Reset again since modified changed
        t.stale?.should be_true
      end
    end

    it "should consider tasks with missing kv outputs as stale" do
      with_scenario("empty") do
        t = Task.new(id: "t", inputs: ["kv://foo"], outputs: ["kv://bar"]) { "bar" }
        t.stale = nil # Reset to trigger recomputation
        # foo and bar are NOT marked modified but bar is not there
        t.stale?.should be_true
        t.run
        t.stale = nil # Reset to trigger recomputation
        # Now the task has run, bar is there, not stale anymore
        t.stale?.should be_false
        # Remove it, stale again. The delete goes around the manager, so
        # the read-through cache entry for it goes too
        TaskManager.@_store.delete("bar")
        TaskManager.@store_cache.delete("bar")
        t.stale = nil # Reset again since store changed
        t.stale?.should be_true
      end
    end

    it "should not consider tasks with existing kv outputs and not modified as stale" do
      with_scenario("empty") do
        TaskManager.use_persistent_store("store")
        TaskManager.@_store.set("foo", "foo")
        TaskManager.@_store.set("bar", "bar")
        # This task has input and output in the persistent store
        # and they are not modified
        p = TaskProc.new { "bar" }
        t = Task.new(id: "t", inputs: ["kv://foo"], outputs: ["kv://bar"], proc: p)
        t.stale = nil # Reset to trigger recomputation
        t.stale?.should be_false
      end
    end

    it "should round-trip the stale property" do
      with_scenario("empty") do
        t = Task.new(id: "t", output: "upstream") { "upstream data" }
        # Starts unknown
        t.stale.should be_nil
        t.stale = true
        t.stale.should be_true
        t.stale?.should be_true
        t.stale = false
        t.stale.should be_false
        t.stale?.should be_false
        # Reset to unknown, then recomputed on demand
        t.stale = nil
        t.stale.should be_nil
        t.stale?.should be_true

        # Computing staleness on demand caches the result in the property
        t2 = Task.new(id: "t2", inputs: ["upstream"], output: "downstream") { "downstream data" }
        t2.stale.should be_nil
        t2.stale?.should be_true # output missing
        t2.stale.should be_true
      end
    end

    it "should not report a finished input-less task as stale" do
      with_scenario("empty") do
        t = Task.new(id: "t", output: "upstream") { "upstream data" }
        # Before running, staleness is unknown: an input-less task is stale
        t.stale?.should be_true
        t.run
        # Once it ran this run, it is not stale anymore, so dependents
        # waiting on it can proceed
        t.stale?.should be_false
      end
    end
  end

  describe "waiting_for" do
    it "should say a task is waiting if a dependency that doesn't exist" do
      with_scenario("basic") do
        t = TaskManager.tasks["output4"]
        t.waiting_for.should eq ["output3"]
      end
    end

    it "should wait for a missing kv:// input and release it when the key is set" do
      with_scenario("empty") do
        t = Task.new(output: "out", inputs: ["kv://foo"], proc: TaskProc.new { "" })

        # Key not in the store: the task is blocked on it
        t.waiting_for.should eq ["kv://foo"]
        t.ready?.should be_false

        TaskManager.set("foo", "bar")

        # Key present: the kv input is satisfied
        t.waiting_for.should eq [] of String
        t.ready?.should be_true
      end
    end

    it "should not consider an existing plain file input as a wait" do
      with_scenario("empty", to_create: {"input" => "data"}) do
        t = Task.new(output: "out", inputs: ["input"], proc: TaskProc.new { "" })

        t.waiting_for.should eq [] of String
        t.ready?.should be_true
      end
    end
  end

  describe "ready?" do
    it "should consider all tasks without task dependencies as ready" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.tasks.values.select(&.ready?).flat_map(&.@outputs).should \
          eq ["output1", "output2", "output3", "output5"]
      end
    end

    it "should consider all tasks with missing file inputs as not ready" do
      with_scenario("basic", to_create: {"input2" => "bar"}) do
        TaskManager.tasks.values.select(&.ready?).flat_map(&.@outputs).should \
          eq ["output1", "output2", "output5"]
      end
    end
  end
end
