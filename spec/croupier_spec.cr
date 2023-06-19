require "./spec_helper"

def with_tasks(&)
  Croupier::TaskManager.cleanup
  Dir.glob("spec/files/*").each do |f|
    File.delete?(f)
  end
  # Create a couple of input files
  File.write("spec/files/input", "foo")
  File.write("spec/files/input2", "bar")

  dummy_proc = Croupier::TaskProc.new { "" }
  x = 0
  counter_proc = Croupier::TaskProc.new {
    x += 1
    File.write("output2", "foo")
    ""
  }
  Croupier::Task.new("name", "output1", [] of String, dummy_proc)
  Croupier::Task.new(
    "name",
    "output2",
    [] of String,
    counter_proc,
    no_save: true)
  Croupier::Task.new("name", "output3", ["input"], dummy_proc)
  Croupier::Task.new("name", "output4", ["output3"], dummy_proc)
  Croupier::Task.new("name", "output5", ["input2"], dummy_proc)
  begin
    yield
  rescue ex
    puts "Error: #{ex}"
    raise ex
  ensure
    Croupier::TaskManager.cleanup
    Dir.glob("spec/files/*").each do |f|
      File.delete?(f)
    end
  end
end

describe "Croupier::TaskManager" do
  it "should be able to create a task and fetch it" do
    with_tasks do
      t = Croupier::TaskManager.tasks["output1"]
      t.@name.should eq "name"
      t.@outputs.should eq ["output1"]
      t.@inputs.empty?.should be_true
      t.@stale.should be_true
    end
  end

  it "should be able to create task without output and fetch them" do
    Dir.cd("spec/files") do
      dummy_proc = Croupier::TaskProc.new { "" }
      Croupier::Task.new("foobar1", proc: dummy_proc, id: "t1")
      Croupier::Task.new("foobar2", proc: dummy_proc, id: "t1")
      Croupier::Task.new("foobar3", proc: dummy_proc, id: "t2")
      Croupier::TaskManager.tasks.keys.should eq ["t1", "t2"]
      Croupier::TaskManager.tasks["t1"].@name.should eq "foobar1"
      Croupier::TaskManager.tasks["t1"].@procs.size.should eq 2

      Croupier::TaskManager.tasks["t2"].@name.should eq "foobar3"
      Croupier::TaskManager.tasks["t2"].@procs.size.should eq 1

      # It should run and do nothing
      Croupier::TaskManager.run_tasks
      Dir.glob("*").empty?.should be_true
    end
  end

  it "should allow a task to depend on a task without output referenced by id" do
    Dir.cd("spec/files") do
      dummy_proc = Croupier::TaskProc.new { "" }
      Croupier::Task.new("foobar1", inputs: ["t2"], proc: dummy_proc, id: "t1")
      Croupier::Task.new("foobar3", proc: dummy_proc, id: "t2")
      Croupier::TaskManager.tasks.keys.should eq ["t1", "t2"]

      # Should respect dependencies even if they are just IDs
      Croupier::TaskManager.sorted_task_graph[1].should eq ["t2", "t1"]

      # It should run and do nothing
      Croupier::TaskManager.run_tasks
      Dir.glob("*").empty?.should be_true
    end
  end

  it "should fail when you fetch a task that doesn't exist" do
    with_tasks do
      expect_raises(KeyError) do
        Croupier::TaskManager.tasks("foo")
      end
    end
  end

  it "should have a nice string representation" do
    with_tasks do
      Croupier::TaskManager.tasks["output1"].to_s.should eq "name::(output1)"
    end
  end

  it "should be registered" do
    with_tasks do
      Croupier::TaskManager.tasks.has_key?("output1").should eq true
    end
  end

  it "should reject self-cyclical tasks" do
    with_tasks do
      expect_raises(Exception, "Cycle detected") do
        p = Croupier::TaskProc.new { "" }
        Croupier::Task.new("name", "output6", ["input.txt", "output6"], p)
      end
    end
  end

  it "should execute the task's proc when Task.run is called" do
    Dir.cd "spec/files" do
      y = x = 0
      b = Croupier::TaskProc.new {
        x += 1
        File.write("output2", "foo")
        ""
      }
      t = Croupier::Task.new(
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

  it "should fail if a no_save task doesn't generate the output when Task.run is called" do
    Dir.cd "spec/files" do
      File.delete?("output2") # Make sure this doesn't exist
      Croupier::TaskManager.cleanup
      b = Croupier::TaskProc.new {
        ""
      }
      t = Croupier::Task.new(
        "name",
        "output2",
        [] of String,
        b,
        no_save: true)
      expect_raises(Exception, "Task name::(output2) did not generate output2") do
        t.run
      end
    end
    Croupier::TaskManager.cleanup
  end

  it "should be stale if an input is marked modified" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.run_tasks
        t = Croupier::TaskManager.tasks["output3"]
        t.@stale.should be_false
        t.mark_stale # Mark stale to force recalculation
        t.@stale.should be_true
        Croupier::TaskManager.clear_modified
        Croupier::TaskManager.mark_modified("input")
        t.stale?.should be_true
      end
    end
  end

  it "should be stale if a dependent task is stale" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.run_tasks
        t = Croupier::TaskManager.tasks["output4"]
        Croupier::TaskManager.clear_modified
        Croupier::TaskManager.tasks.values.each do |task|
          task.mark_stale
        end
        t.mark_stale # Force recalculation of stale state
        # input is not a direct dependency of t, but an indirect one
        Croupier::TaskManager.mark_modified("input")
        t.stale?.should be_true
      end
    end
  end

  it "should do nothing on a second run" do
    with_tasks do
      Dir.cd "spec/files" do
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
        Croupier::TaskManager.tasks.size.should eq 5
        Croupier::TaskManager.tasks.values.select(&.stale?).should be_empty
      end
    end
  end

  it "should list all inputs for all tasks" do
    # TODO: check inputs are not repeated
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.all_inputs.should eq ["input", "output3", "input2"]
      end
    end
  end

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
    with_tasks do
      Dir.cd "spec/files" do
        g, s = Croupier::TaskManager.sorted_task_graph
        g.@vertice_dict.should eq expected
        s.size.should eq Croupier::TaskManager.tasks.size
        s.should eq ["output3", "output4", "output5", "output1", "output2"]
      end
    end
  end

  it "should run all tasks when run_all is true" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.run_tasks(run_all: true)
        Croupier::TaskManager.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
      end
    end
  end

  it "should run all stale tasks when run_all is false" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.tasks("output1").not_ready # Not stale
        Croupier::TaskManager.run_tasks(run_all: false)
        Croupier::TaskManager.tasks.keys.each do |k|
          if k == "output1"
            File.exists?(k).should be_false
          else
            File.exists?(k).should be_true
          end
        end
      end
    end
  end

  it "should run all tasks in parallel" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.run_tasks_parallel
        Croupier::TaskManager.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
      end
    end
  end

  it "should calculate hashes for all inputs" do
    # Even though output3 is an input to a task, it's generated by another
    # So when running from scratch it's not there
    expected = {"input"  => "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33",
                "input2" => "62cdb7020ff920e5aa642c3d4066950dd1f01f4d"}
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.scan_inputs.should eq expected
      end
    end
  end

  it "should not hash files that don't exist" do
    with_tasks do
      Dir.cd "spec/files" do
        Dir.glob("*").each do |f|
          File.delete?(f)
        end
        Croupier::TaskManager.scan_inputs.size.should eq 0
      end
    end
  end

  it "should save files but respect the no_save flag" do
    with_tasks do
      Dir.cd "spec/files" do
        File.exists?("output1").should be_false
        File.exists?("output2").should be_false

        Croupier::TaskManager.run_tasks(run_all: true)

        # The output task has no_save = false, so it should be created
        File.exists?("output1").should be_true
        # The output2 task has no_save = true
        # so it's created by the proc, which creates it
        # with "foo" as the contents
        File.exists?("output2").should be_true
        File.read("output2").should eq "foo"
      end
    end
  end

  it "should mark all tasks with inputs as stale if there is no .croupier file" do
    with_tasks do
      Dir.cd "spec/files" do
        # Make sure al tasks run, but no files are marked
        # modified and there is no .croupier file
        tasks = Croupier::TaskManager.tasks
        Croupier::TaskManager.run_tasks
        Croupier::TaskManager.tasks.values.each(&.mark_stale)
        Croupier::TaskManager.clear_modified
        File.delete(".croupier")

        Croupier::TaskManager.mark_stale_inputs

        # Only tasks with inputs should be stale
        tasks.values.select(&.stale?).flat_map(&.@outputs).should eq ["output3", "output4", "output5"]
      end
    end
  end

  it "should mark tasks depending indirectly on a modified file as stale" do
    with_tasks do
      Dir.cd "spec/files" do
        # Make sure all outputs exists and no files are modified
        tasks = Croupier::TaskManager.tasks
        tasks.size.should eq 5
        Croupier::TaskManager.run_tasks
        Croupier::TaskManager.clear_modified
        tasks.values.each(&.mark_stale)
        # All tasks are marked stale so theit state is recalculated
        tasks.values.count(&.@stale).should eq 5

        # Only input is modified
        Croupier::TaskManager.mark_modified("input")

        # Only tasks depending on "input" should be stale
        tasks.values.count(&.stale?).should eq 2
        tasks.keys.select { |k| tasks[k].stale? }.should eq ["output3", "output4"]
      end
    end
  end

  it "should mark tasks as stale if the output doesn't exist" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.run_tasks
        t = Croupier::TaskManager.tasks("output1")
        t.mark_stale # Force recalculation of stale state
        t.@stale.should be_true
        File.delete?("output1")
        t.stale?.should be_true
      end
    end
  end

  it "should mark file with wrong hash as modified" do
    with_tasks do
      Dir.cd "spec/files" do
        # Make sure no files are modified
        Croupier::TaskManager.clear_modified
        Croupier::TaskManager.modified.empty?.should be_true
        File.open(".croupier", "w") do |f|
          f.puts(%({
          "input": "thisiswrong",
          "input2": "62cdb7020ff920e5aa642c3d4066950dd1f01f4d",
          "output3": "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
      }))
        end

        Croupier::TaskManager.mark_stale_inputs

        Croupier::TaskManager.modified.should eq Set{"input"}
      end
    end
  end

  it "should detect cycles in the graph when calling sorted_task_graph" do
    with_tasks do
      Dir.cd "spec/files" do
        b = Croupier::TaskProc.new { "" }
        Croupier::Task.new("name", "input", ["output4"], b)
        expect_raises(Exception, "Cycle detected") do
          Croupier::TaskManager.sorted_task_graph
        end
      end
    end
  end

  it "should consider all tasks without task dependencies as ready" do
    with_tasks do
      Dir.cd("spec/files") do
        Croupier::TaskManager.tasks.values.select(&.ready?).flat_map(&.@outputs).should \
          eq ["output1", "output2", "output3", "output5"]
      end
    end
  end

  it "should consider all tasks with missing file inputs as not ready" do
    with_tasks do
      Dir.cd("spec/files") do
        File.delete("input")
        Croupier::TaskManager.tasks.values.select(&.ready?).flat_map(&.@outputs).should \
          eq ["output1", "output2", "output5"]
      end
    end
  end

  it "should report all tasks required to produce an output" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.dependencies("output4").should eq ["output3", "output4"]
      end
    end
  end

  it "should report all tasks required to produce multiple outputs" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.dependencies(["output4", "output5"]).should eq ["output3", "output4", "output5"]
      end
    end
  end

  it "should run only required tasks to produce specified outputs" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::TaskManager.run_tasks(["output4", "output5"])
        File.exists?("output1").should be_false
        File.exists?("output2").should be_false
        File.exists?("output3").should be_true # Required for output4
        File.exists?("output4").should be_true # Required
        File.exists?("output5").should be_true # Required
      end
    end
  end

  it "should fail if asked for dependencies of an unknown output" do
    with_tasks do
      Dir.cd "spec/files" do
        expect_raises(Exception) do
          Croupier::TaskManager.dependencies("output99")
        end
      end
    end
  end

  it "should fail to run if a task depends on an input that doesn't exist and won't be generated" do
    with_tasks do
      Dir.cd "spec/files" do
        File.delete("input")
        expect_raises(Exception, "Unknown inputs") do
          Croupier::TaskManager.run_tasks
        end
      end
    end
  end

  it "should be possible to create two tasks with the same output" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      dummy_proc = Croupier::TaskProc.new { "" }
      t1 = Croupier::Task.new("name", "output", ["i1"] of String, dummy_proc)
      Croupier::Task.new("name", "output", ["i2"] of String, dummy_proc)

      # t2 is merged into t1
      Croupier::TaskManager.tasks["output"].should eq t1
      t1.@inputs == ["i1", "i2"]
    end
  end

  it "should not allow merging tasks with different `no_save`" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      Croupier::Task.new("name", "output", no_save: true)
      expect_raises(Exception, "different no_save settings") do
        Croupier::Task.new("name", "output", no_save: false)
      end
    end
  end

  it "should be possible to have more than one output" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      t1 = Croupier::Task.new("name", ["output1", "output2"])

      # Should be visible in two places
      Croupier::TaskManager.tasks["output1"].should eq t1
      Croupier::TaskManager.tasks["output2"].should eq t1
    end
  end

  it "running merged tasks should have all effects of running all merged tasks" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      proc1 = Croupier::TaskProc.new { File.open("1", "w") << ""; "foo" }
      proc2 = Croupier::TaskProc.new { File.open("2", "w") << ""; "bar" }
      t1 = Croupier::Task.new("t1", "output", [] of String, proc1)
      Croupier::Task.new("t2", "output", [] of String, proc2)

      # t2 merges into t1
      Croupier::TaskManager.tasks["output"].should eq t1

      Croupier::TaskManager.run_tasks

      # output should have result of t2
      File.read("output").should eq "bar"

      # Files 1 and 2 should exist because both procs ran
      File.exists?("1").should be_true
      File.exists?("2").should be_true
    end
  end

  it "should handle a no_save task that generates multiple outputs" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      p = Croupier::TaskProc.new { File.open("output1", "w") << ""; File.open("output2", "w") << ""; "" }
      Croupier::Task.new("name", ["output1", "output2"], proc: p, no_save: true)
      Croupier::TaskManager.run_tasks
    end
  end

  it "should handle a task that generates multiple outputs" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      p = Croupier::TaskProc.new { ["foo", "bar"] }
      Croupier::Task.new("name", ["output1", "output2"], proc: p)

      Croupier::TaskManager.run_tasks

      # The two files should be created with the right contents
      File.read("output1").should eq "foo"
      File.read("output2").should eq "bar"
    end
  end

  it "should fail if a task generates wrong number of outputs" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      p = Croupier::TaskProc.new { ["foo", "bar"] }
      Croupier::Task.new("name", ["output1", "output2", "output3"], proc: p)

      expect_raises(Exception, "correct number of outputs") do
        Croupier::TaskManager.run_tasks
      end

      # The two files should be created with the right contents
      File.read("output1").should eq "foo"
      File.read("output2").should eq "bar"
    end
  end

  it "should fail if a task generates invalid output" do
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup
      # The proc in a task with multiple outputs should return an array
      p = Croupier::TaskProc.new { "foo" }
      Croupier::Task.new("name", ["output1", "output2", "output3"], proc: p)

      expect_raises(Exception, "did not return an array") do
        Croupier::TaskManager.run_tasks
      end

      # The two files should be created with the right contents
      File.read("output1").should eq "foo"
      File.read("output2").should eq "bar"
    end
  end

  it "should run tasks marked with 'always_run' even if the dependencies are not changed" do
    x1 = 0
    counter_proc_1 = Croupier::TaskProc.new {
      x1 += 1
      ""
    }
    x2 = 0
    counter_proc_2 = Croupier::TaskProc.new {
      x2 += 1
      ""
    }
    Dir.cd "spec/files" do
      Croupier::TaskManager.cleanup

      # Need to have an input file, because tasks without
      # inputs are implicitly always_run
      File.open("input", "w") << ""
      Croupier::Task.new(
        "t1",
        inputs: ["input"],
        always_run: true,
        proc: counter_proc_1,
        id: "t1"
      )
      Croupier::Task.new(
        "t2",
        inputs: ["input"],
        always_run: false,
        proc: counter_proc_2,
        id: "t2"
      )
      x1.should eq 0
      x2.should eq 0
      Croupier::TaskManager.run_tasks
      x1.should eq 1
      x2.should eq 1
      Croupier::TaskManager.run_tasks
      x1.should eq 2
      x2.should eq 1
    end
  end
end
