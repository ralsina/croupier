require "./spec_helper"
include Croupier

def with_scenario(
  name,
  keep = [] of String,
  to_create = {} of String => String,
  procs = {} of String => TaskProc, &
)
  # Setup logging, helps coverage
  logs = IO::Memory.new
  Log.setup(:debug, Log::IOBackend.new(io: logs))

  # Library of procs
  x = 0
  _procs = {
    "dummy"   => TaskProc.new { "" },
    "counter" => TaskProc.new {
      x += 1
      ""
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
      File.delete?(f) unless keep.includes?(f) || f == "tasks.yml"
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
          name: t["name"].to_s,
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
        TaskManager.tasks["output1"].to_s.should eq "name::(output1)"
      end
    end

    it "should be yaml serializable" do
      with_scenario("basic") do
        expected = {
          "id"         => "77012200e4c39aa279b0d3e16dca43a7b02eb4a5",
          "name"       => "name",
          "inputs"     => [] of String,
          "outputs"    => ["output1"],
          "always_run" => false,
          "no_save"    => false,
          "stale"      => true,
        }
        YAML.parse(TaskManager.tasks["output1"].to_yaml).should eq expected
      end
    end
  end

  describe "new" do
    it "should be possible to create a task and fetch it" do
      with_scenario("basic") do
        t = TaskManager.tasks["output1"]
        t.@name.should eq "name"
        t.@outputs.should eq ["output1"]
        t.@inputs.empty?.should be_true
        t.@stale.should be_true
      end
    end

    it "should be possible to create tasks without output and fetch them" do
      with_scenario("empty") do
        Task.new("foobar1", id: "t1")
        Task.new("foobar2", id: "t1")
        Task.new("foobar3", id: "t2")

        TaskManager.tasks.keys.should eq ["t1", "t2"]
        # foobar2 is merged into foobar1
        TaskManager.tasks["t1"].@name.should eq "foobar1"
        TaskManager.tasks["t2"].@name.should eq "foobar3"
      end
    end

    it "should allow a task to depend on a task without output referenced by id" do
      with_scenario("empty") do
        Task.new("foobar1", inputs: ["t2"], id: "t1")
        Task.new("foobar3", id: "t2")

        TaskManager.tasks.keys.should eq ["t1", "t2"]
        # Should respect dependencies even if they are just IDs
        TaskManager.sorted_task_graph[1].should eq ["t2", "t1"]
      end
    end

    it "should reject self-cyclical tasks" do
      with_scenario("basic") do
        expect_raises(Exception, "Cycle detected") do
          Task.new("name", "output6", ["input.txt", "output6"])
        end
      end
    end

    it "should allow creating two tasks with the same output" do
      with_scenario("empty") do
        dummy_proc = TaskProc.new { "" }
        t1 = Task.new("name", "output", ["i1"] of String, dummy_proc)
        Task.new("name", "output", ["i2"] of String, dummy_proc)

        # t2 is merged into t1
        TaskManager.tasks["output"].should eq t1
        t1.@inputs == ["i1", "i2"]
      end
    end

    it "should allow creating tasks with more than one output" do
      with_scenario("empty") do
        t1 = Task.new("name", ["output1", "output2"])

        # Should be visible in two places
        TaskManager.tasks["output1"].should eq t1
        TaskManager.tasks["output2"].should eq t1
      end
    end
  end

  describe "merge" do
    it "should not allow merging tasks with different `no_save`" do
      with_scenario("empty") do
        Task.new("name", "output", no_save: true)
        expect_raises(Exception, "different no_save settings") do
          Task.new("name", "output", no_save: false)
        end
      end
    end

    it "should not allow merging tasks with different `always_run`" do
      with_scenario("empty") do
        Task.new("name", "output", always_run: true)
        expect_raises(Exception, "different always_run settings") do
          Task.new("name", "output", always_run: false)
        end
      end
    end

    it "should add the effects of the merged task to the first one" do
      with_scenario("empty") do
        proc1 = TaskProc.new { File.open("1", "w") << ""; "foo" }
        proc2 = TaskProc.new { File.open("2", "w") << ""; "bar" }
        t1 = Task.new("t1", "output", [] of String, proc1)
        Task.new("t2", "output", [] of String, proc2)

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
        t1 = Task.new("t1", ["output", "output2"], [] of String, proc1)
        t2 = Task.new("t2", ["output", "output3"], [] of String, proc2)

        # t2 merges into t1
        TaskManager.tasks["output"].should eq t1
        TaskManager.tasks["output2"].should eq t1
        TaskManager.tasks["output3"].should eq t2
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
          "name",
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
          "name",
          "output2",
          [] of String,
          b,
          no_save: true)
        expect_raises(Exception, "Task name::(output2) did not generate output2") do
          t.run
        end
      end
    end

    it "should record hash for outputs in the TaskManager" do
      with_scenario("empty") do
        t = Task.new(
          "name",
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
  end

  describe "stale?" do
    it "should make a task stale if its input is marked modified" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        t = TaskManager.tasks["output3"]
        t.@stale.should be_false
        t.mark_stale # Mark stale to force recalculation
        t.@stale.should be_true
        TaskManager.clear_modified
        TaskManager.mark_modified("input")
        t.stale?.should be_true
      end
    end

    it "should mark a task stale if a task it depends on is stale" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        t = TaskManager.tasks["output4"]
        TaskManager.clear_modified
        # Force recalculation of stale states
        TaskManager.tasks.values.each do |task|
          task.mark_stale
        end
        # input is not a direct dependency of t, but an indirect one
        TaskManager.mark_modified("input")
        t.stale?.should be_true
      end
    end

    it "should mark all tasks stale on a second run" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Set things up as they should look after running
        File.write("input", "foo")
        File.write("input2", "bar")
        File.write("output1", "")
        File.write("output2", "foo")
        File.write("output3", "")
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
        TaskManager.tasks.size.should eq 5
        TaskManager.tasks.values.select(&.stale?).should be_empty
      end
    end

    it "should mark tasks depending indirectly on a modified file as stale" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure all outputs exists and no files are modified
        tasks = TaskManager.tasks
        tasks.size.should eq 5
        TaskManager.run_tasks
        TaskManager.clear_modified
        tasks.values.each(&.mark_stale)
        # All tasks are marked stale so theit state is recalculated
        tasks.values.count(&.@stale).should eq 5

        # Only input is modified
        TaskManager.mark_modified("input")

        # Only tasks depending on "input" should be stale
        tasks.values.count(&.stale?).should eq 2
        tasks.keys.select { |k| tasks[k].stale? }.should eq ["output3", "output4"]
      end
    end

    it "should mark tasks as stale if the output doesn't exist" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        t = TaskManager.tasks["output1"]
        t.mark_stale # Force recalculation of stale state
        t.@stale.should be_true
        File.delete?("output1")
        t.stale?.should be_true
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
        TaskManager.all_inputs.should eq ["input", "output3", "input2"]
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
        Task.new("name", "input", ["output4"])
        expect_raises(Exception, "Cycle detected") do
          TaskManager.sorted_task_graph
        end
      end
    end
  end

  describe "run_tasks" do
    it "should run all stale tasks when run_all is false" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.tasks["output1"].not_ready # Not stale
        TaskManager.run_tasks(run_all: false)
        TaskManager.tasks.keys.each do |k|
          if k == "output1"
            File.exists?(k).should be_false
          else
            File.exists?(k).should be_true
          end
        end
      end
    end

    it "should run no tasks when dry_run is true" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks(run_all: true, dry_run: true)
        TaskManager.tasks.keys.each do |k|
          File.exists?(k).should be_false
        end
      end
    end

    it "should run all tasks when run_all is true" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks(run_all: true)
        TaskManager.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
      end
    end

    it "should save files but respect the no_save flag" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        File.exists?("output1").should be_false
        File.exists?("output2").should be_false

        TaskManager.run_tasks(run_all: true)

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
        TaskManager.run_tasks(["output4", "output5"])
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
          TaskManager.run_tasks
        end
      end
    end

    it "should handle a no_save task that generates multiple outputs" do
      with_scenario("empty") do
        p = TaskProc.new { File.open("output1", "w") << ""; File.open("output2", "w") << ""; "" }
        Task.new("name", ["output1", "output2"], proc: p, no_save: true)
        TaskManager.run_tasks
      end
    end

    it "should handle a task that generates multiple outputs" do
      with_scenario("empty") do
        p = TaskProc.new { ["foo", "bar"] }
        Task.new("name", ["output1", "output2"], proc: p)

        TaskManager.run_tasks

        # The two files should be created with the right contents
        File.read("output1").should eq "foo"
        File.read("output2").should eq "bar"
      end
    end

    it "should fail if a task generates wrong number of outputs" do
      with_scenario("empty") do
        p = TaskProc.new { ["foo", "bar"] }
        Task.new("name", ["output1", "output2", "output3"], proc: p)

        expect_raises(Exception, "correct number of outputs") do
          TaskManager.run_tasks
        end
      end
    end

    it "should fail if a task generates invalid output" do
      with_scenario("empty") do
        # The proc in a task with multiple outputs should return an array
        p = TaskProc.new { "foo" }
        Task.new("name", ["output1", "output2", "output3"], proc: p)

        expect_raises(Exception, "did not return the correct number of outputs") do
          TaskManager.run_tasks
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
          "t1",
          inputs: ["input"],
          always_run: true,
          proc: counter_proc_1,
          id: "t1"
        )
        Task.new(
          "t2",
          inputs: ["input"],
          always_run: false,
          proc: counter_proc_2,
          id: "t2"
        )
        x1.should eq 0
        x2.should eq 0
        TaskManager.run_tasks
        x1.should eq 1
        x2.should eq 1
        TaskManager.run_tasks
        x1.should eq 2
        x2.should eq 1
      end
    end
  end

  describe "run_tasks_parallel" do
    it "should run all tasks in parallel" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks_parallel
        TaskManager.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
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
    it "should mark all tasks with inputs as stale if there is no .croupier file" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        # Make sure al tasks run, but no files are marked
        # modified and there is no .croupier file
        tasks = TaskManager.tasks
        TaskManager.run_tasks
        TaskManager.tasks.values.each(&.mark_stale)
        TaskManager.clear_modified
        File.delete(".croupier")

        TaskManager.mark_stale_inputs

        # Only tasks with inputs should be stale
        tasks.values.select(&.stale?).flat_map(&.@outputs).should eq ["output3", "output4", "output5"]
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
end
