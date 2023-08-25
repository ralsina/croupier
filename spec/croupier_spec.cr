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

  # Library of procs
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

describe "Task" do
  describe "serialization" do
    it "should have a nice string representation" do
      with_scenario("basic") do
        id = "77012200e4c39aa279b0d3e16dca43a7b02eb4a5"
        TaskManager.tasks["output1"].to_s.should eq "#{id}::output1"
      end
    end

    it "should be yaml serializable" do
      with_scenario("basic") do
        expected = {
          "id"         => "77012200e4c39aa279b0d3e16dca43a7b02eb4a5",
          "inputs"     => [] of String,
          "outputs"    => ["output1"],
          "always_run" => false,
          "no_save"    => false,
          "stale"      => true,
          "mergeable"  => true,
        }
        YAML.parse(TaskManager.tasks["output1"].to_yaml).should eq expected
      end
    end
  end

  describe "new" do
    it "should be possible to create a task and fetch it" do
      with_scenario("basic") do
        t = TaskManager.tasks["output1"]
        t.@outputs.should eq ["output1"]
        t.@inputs.empty?.should be_true
        t.stale.should be_true
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
        expect_raises(Exception, "Task 052cd9c::output2 did not generate output2") do
          t.run
        end
      end
    end

    it "should fail if the proc raises an exception" do
      with_scenario("empty") do
        b = TaskProc.new { raise "foo" }
        t = Task.new("output2", proc: b)
        expect_raises(Exception, "Task 052cd9c::output2 failed: foo") do
          t.run
        end
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
        t.stale = true # Mark stale to force recalculation
        t.stale.should be_true
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
          task.stale = true
        end
        # input is not a direct dependency of t, but an indirect one
        TaskManager.modified << "input"
        t.stale?.should be_true
      end
    end

    it "should mark tasks depending (in)directly on a modified file as stale" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure all outputs exists and no files are modified
        tasks = TaskManager.tasks
        tasks.size.should eq 5
        TaskManager.run_tasks
        TaskManager.modified.clear
        tasks.values.each(&.stale = true)
        # All tasks are marked stale so theit state is recalculated
        tasks.values.count(&.stale).should eq 5

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
        t.stale = true # Force recalculation of stale state
        t.stale.should be_true
        File.delete?("output1")
        t.stale?.should be_true
      end
    end

    it "should not consider tasks with kv inputs as stale unless modified" do
      with_scenario("empty") do
        t = Task.new(id: "t", inputs: ["kv://foo"])
        t.run
        t.stale = true
        t.stale?.should be_false
        TaskManager.modified << "kv://foo"
        t.stale?.should be_true
      end
    end

    it "should consider tasks with missing kv outputs as stale" do
      with_scenario("empty") do
        t = Task.new(id: "t", inputs: ["kv://foo"], outputs: ["kv://bar"]) { "bar" }
        t.stale = true
        # foo and bar are NOT marked modified but bar is not there
        t.stale?.should be_true
        t.run
        t.stale = true
        # Now the task has run, bar is there, not stale anymore
        t.stale?.should be_false
        # Remove it, stale again
        TaskManager.@_store.delete("bar")
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
        t.stale = true
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
      # TODO: check inputs are not repeated
      with_scenario("basic") do
        TaskManager.all_inputs.should eq Set{"input", "output3", "input2"}
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
        g.@vertice_dict.should eq expected
        s.size.should eq TaskManager.tasks.size
        s.should eq ["output3", "output4", "output5", "output1", "output2"]
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

  # Run the same tests for parallel and serial execution of tasks
  [false, true].each do |parallel|
    describe "run_tasks, parallel = #{parallel}" do
      it "should run all stale tasks when run_all is false" do
        with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
          TaskManager.tasks["output5"].stale = false
          TaskManager.run_tasks(parallel: parallel, run_all: false)
          TaskManager.tasks.keys.each do |k|
            if k == "output5"
              File.exists?(k).should be_false
            else
              File.exists?(k).should be_true
            end
          end
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

      it "should fail if a proc raises an exception" do
        with_scenario("empty") do
          b = TaskProc.new { raise "foo" }
          Task.new(["output2"], proc: b)
          expect_raises(Exception, "Task 052cd9c::output2 failed: foo") do
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
            Task.new(["output1"])
            expect_raises(Exception, "Task 052cd9c::output2 failed: foo") do
              TaskManager.run_tasks
            end
            # It should never have executed the second task
            File.exists?("output1").should be_false
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
  end

  describe "mark_stale_inputs" do
    it "should mark all tasks as stale if there is no .croupier file" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure al tasks run, but no files are marked
        # modified and there is no .croupier file
        tasks = TaskManager.tasks
        TaskManager.run_tasks
        # The 2 tasks without inputs should be stale
        tasks.values.count(&.stale?).should eq 2

        TaskManager.tasks.values.each(&.stale = true)
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
          "input"   => "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33",
          "input2"  => "62cdb7020ff920e5aa642c3d4066950dd1f01f4d",
          "output1" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output2" => "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15",
          "output3" => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "output4" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
          "output5" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
        }))
        TaskManager.tasks.size.should eq 5
        TaskManager.mark_stale_inputs
        # Since .croupier describes all inputs, none should be
        # considered modified
        TaskManager.modified.empty?.should be_true
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
        # We need to yield or else the watch callbacks never run
        Fiber.yield
        TaskManager.@queued_changes.should eq Set{"input"}
        File.open("input2", "w") << "foo"
        sleep 0.1.seconds # FIXME: this should work with a yield
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
    it "should run tasks when inputs change" do
      with_scenario("basic") do
        TaskManager.auto_run
        # We need to yield or else the watch callbacks never run
        Fiber.yield
        # At this point output3 doesn't exist
        File.exists?("output3").should be_false
        # We create input, which is output3's dependency
        File.open("input", "w") << "bar"
        Fiber.yield
        # Tasks are not runnable (missing input2)
        File.exists?("output3").should be_false
        # We create input, which is output3's dependency
        File.open("input2", "w") << "bar"
        Fiber.yield
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
        # We need to yield or else the watch callbacks never run
        Fiber.yield
        # auto_run logs all errors and continues, because it's
        # normal to have failed runs in auto mode
        TaskManager.auto_stop
        # It should have run
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
        Fiber.yield
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
        Fiber.yield
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
        Fiber.yield
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
        # The timing here is tricky, we need to wait longer
        # than the watch interval, but not too long because
        # that makes the test slow
        sleep 0.02.seconds
        File.exists?("output3").should be_true
        # We delete things, and then trigger another build
        File.delete("output3")
        File.delete("input")
        File.open("input", "w") << "bar2"
        Fiber.yield
        sleep 0.02.seconds
        TaskManager.auto_stop
        File.exists?("output3").should be_true
      end
    end

    it "should not be triggered by deps for not specified targets" do
      with_scenario("basic") do
        TaskManager.auto_run(targets: ["output5"])
        sleep 0.2.seconds
        # At this point output5 doesn't exist
        File.exists?("output5").should be_false
        File.exists?("output3").should be_false
        # This triggers output3, which is not requested
        File.open("input", "w") << "bar"
        Fiber.yield
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
        sleep 0.02.seconds
        x.should eq 1
        TaskManager.set("foo", "bar3")
        sleep 0.02.seconds
        TaskManager.auto_stop
        # We modified foo 2 times, so it should have ran 2 times
        x.should eq 2
      end
    end
  end

  describe "save_run" do
    it "should save this_run and next_run merged" do
      with_scenario("empty") do
        TaskManager.this_run = {"foo" => "bar"}
        TaskManager.next_run = {"bat" => "quux"}
        TaskManager.save_run
        File.read(".croupier").should eq %(---\nfoo: bar\nbat: quux\n)
      end
    end

    it "should save all inputs and outputs on a full run" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        File.read(".croupier").should eq "---\n" +
                                         "input: 0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n" +
                                         "input2: 62cdb7020ff920e5aa642c3d4066950dd1f01f4d\n" +
                                         "output3: da39a3ee5e6b4b0d3255bfef95601890afd80709\n" +
                                         "output4: da39a3ee5e6b4b0d3255bfef95601890afd80709\n" +
                                         "output5: da39a3ee5e6b4b0d3255bfef95601890afd80709\n" +
                                         "output1: 356a192b7913b04c54574d18c28d46e6395428ab\n" +
                                         "output2: 0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33\n"
      end
    end
  end

  describe "dependencies" do
    it "should report all tasks required to produce an output" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.dependencies("output4").should eq ["output3", "output4"]
      end
    end

    it "should report all tasks required to produce multiple outputs" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.dependencies(["output4", "output5"]).should eq ["output3", "output4", "output5"]
      end
    end

    it "should fail if asked for dependencies of an unknown output" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        expect_raises(Exception, "Unknown output output99") do
          TaskManager.dependencies("output99")
        end
      end
    end
  end

  describe "store" do
    it "should save and recover values" do
      with_scenario("empty") do
        TaskManager.get("foo").should be_nil
        TaskManager.set("foo", "bar")
        TaskManager.get("foo").should eq "bar"
      end
    end

    it "should be an empty MemoryStore by default" do
      with_scenario("empty") do
        # This would raise an exception if it weren´t one
        TaskManager.@_store_path.nil?.should be_true
        TaskManager.@_store.as(Kiwi::MemoryStore).@mem.empty?.should be_true
      end
    end

    it "should be persistant after calling use_persistent_store" do
      with_scenario("empty") do
        TaskManager.@_store_path.nil?.should be_true
        TaskManager.@_store.as(Kiwi::MemoryStore).@mem.empty?.should be_true
        TaskManager.set("foo", "bar")
        TaskManager.use_persistent_store("store")
        # This would raise an exception if it weren´t a FileStore
        TaskManager.@_store.as(Kiwi::FileStore)
        # Data should be migrated
        TaskManager.get("foo").should eq "bar"
      end
    end
  end
end
