require "./spec_helper"

def with_tasks(&)
  dummy_proc = ->{ "" }
  x = 0
  counter_proc = ->{
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
    Croupier::Task.cleanup
    Dir.cd("spec/files") do
      File.delete?("output1")
      File.delete?("output2")
      File.delete?("output3")
      File.delete?("output4")
      File.delete?("output5")
      File.delete?(".croupier")
    end
  end
end

describe Croupier::Task do
  it "should be able to create a task and fetch it" do
    with_tasks do
      t = Croupier::Task.task("output1")
      if t.nil?
        fail "Task not found"
      end
      t.@name.should eq "name"
      t.@output.should eq "output1"
      t.@inputs.should eq [] of String
      t.@stale.should be_true
    end
  end

  it "should fail when you fetch a task that doesn't exist" do
    with_tasks do
      expect_raises(KeyError) do
        Croupier::Task.task("foo").should be_nil
      end
    end
  end

  it "should have a nice string representation" do
    with_tasks do
      Croupier::Task.tasks["output1"].to_s.should eq "name::output1"
    end
  end

  it "should be registered" do
    with_tasks do
      Croupier::Task.tasks.has_key?("output1").should eq true
    end
  end

  it "should not allow two tasks with same output" do
    with_tasks do
      expect_raises(Exception, "which is already generated") do
        b = ->{ "" }
        Croupier::Task.new("name", "output1", [] of String, b)
      end
    end
  end

  it "should reject self-cyclical tasks" do
    with_tasks do
      expect_raises(Exception, "Cycle detected") do
        b = ->{ "" }
        Croupier::Task.new("name", "output6", ["input.txt", "output6"], b)
      end
    end
  end

  it "should execute the task's proc when Task.run is called" do
    begin
      y = x = 0
      p = ->{
        x += 1
        File.write("output2", "foo")
        ""
      }
      t = Croupier::Task.new(
        "name",
        "output2",
        [] of String,
        p,
        no_save: true)
      t.run
      x.should eq y + 1
      t.run
      x.should eq y + 2
    ensure
      Croupier::Task.cleanup
      File.delete?("output2")
    end
  end

  it "should fail if a no_save task doesn't generate the output when Task.run is called" do
    p = ->{
      ""
    }
    t = Croupier::Task.new(
      "name",
      "output2",
      [] of String,
      p,
      no_save: true)
    Dir.cd "spec/files" do
      File.delete?("output2")
      expect_raises(Exception, "Task name::output2 did not generate output2") do
        t.run
      end
    end
    Croupier::Task.cleanup
  end

  it "should be stale if an input is marked modified" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.run_tasks
        t = Croupier::Task.tasks["output3"]
        t.mark_stale # Mark stale to force recalculation
        t.@stale.should be_true
        Croupier::Task.clear_modified
        Croupier::Task.mark_modified("input")
        t.stale?.should be_true
      end
    end
  end

  it "should be stale if a dependent task is stale" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.run_tasks
        t = Croupier::Task.tasks["output4"]
        Croupier::Task.clear_modified
        Croupier::Task.tasks.values.each(&.mark_stale)
        t.mark_stale # Force recalculation of stale state
        # input is not a direct dependency of t, but an indirect one
        Croupier::Task.mark_modified("input")
        t.stale?.should be_true
      end
    end
  end

  it "should list all inputs for all tasks" do
    with_tasks do
      Croupier::Task.all_inputs.should eq ["input", "output3", "input2"]
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
      g, s = Croupier::Task.sorted_task_graph
      g.@vertice_dict.should eq expected
      s.size.should eq Croupier::Task.tasks.size
      s.should eq ["output3", "output4", "output5", "output1", "output2"]
    end
  end

  it "should run all tasks" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.run_tasks(run_all: true)
        Croupier::Task.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
      end
    end
  end

  it "should run all tasks in parallel" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.run_tasks_parallel
        Croupier::Task.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
      end
    end
  end

  it "should calculate hashes for all inputs" do
    # Even though output3 is an input to a task, it's generated by another
    # So when running from scratch it's not there
    expected = {
      "input"  => "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15",
      "input2" => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
    }
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.scan_inputs.should eq expected
      end
    end
  end

  it "should not hash files that don't exist" do
    # This is running where the files don't exist
    with_tasks do
      Croupier::Task.scan_inputs.size.should eq 0
    end
  end

  it "should save files but respect the no_save flag" do
    with_tasks do
      Dir.cd "spec/files" do
        File.exists?("output1").should be_false
        File.exists?("output2").should be_false

        Croupier::Task.run_tasks(run_all: true)

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
        tasks = Croupier::Task.tasks
        Croupier::Task.run_tasks
        Croupier::Task.tasks.values.each(&.mark_stale)
        Croupier::Task.clear_modified
        File.delete(".croupier")

        Croupier::Task.mark_stale_inputs

        # Only tasks with inputs should be stale
        tasks.values.select(&.stale?).map(&.@output).should eq ["output3", "output4", "output5"]
      end
    end
  end

  it "should mark tasks depending indirectly on a modified file as stale" do
    with_tasks do
      Dir.cd "spec/files" do
        # Make sure all outputs exists and no files are modified
        tasks = Croupier::Task.tasks
        tasks.size.should eq 5
        Croupier::Task.run_tasks
        Croupier::Task.clear_modified
        tasks.values.each(&.mark_stale)
        # All tasks are marked stale so theit state is recalculated
        tasks.values.count(&.@stale).should eq 5

        # Only input is modified
        Croupier::Task.mark_modified("input")

        # Only tasks depending on "input" should be stale
        tasks.values.count(&.stale?).should eq 2
        tasks.keys.select { |k| tasks[k].stale? }.should eq ["output3", "output4"]
      end
    end
  end

  it "should mark tasks as stale if the output doesn't exist" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.run_tasks
        t = Croupier::Task.task("output1")
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
        Croupier::Task.clear_modified
        Croupier::Task.modified.empty?.should be_true
        File.open(".croupier", "w") do |f|
          f.puts(%({
          "input": "thisiswrong",
          "input2": "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "output3": "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
      }))
        end

        Croupier::Task.mark_stale_inputs

        Croupier::Task.modified.should eq Set{"input"}
      end
    end
  end

  it "should detect cycles in the graph when calling sorted_task_graph" do
    with_tasks do
      b = ->{ "" }
      Croupier::Task.new("name", "input", ["output4"], b)
      expect_raises(Exception, "Cycle detected") do
        Croupier::Task.sorted_task_graph
      end
    end
  end

  it "should consider all tasks without task dependencies as ready" do
    with_tasks do
      Dir.cd("spec/files") do
        Croupier::Task.tasks.values.select(&.ready?).map(&.@output).should \
          eq ["output1", "output2", "output3", "output5"]
      end
    end
  end

  it "should consider all tasks with missing file inputs as not ready" do
    with_tasks do
      Dir.cd("spec/files") do
        begin
          File.rename("input", "foo")
          Croupier::Task.tasks.values.select(&.ready?).map(&.@output).should \
            eq ["output1", "output2", "output5"]
        ensure
          File.rename("foo", "input")
        end
      end
    end
  end

  it "should run all tasks in parallel" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.run_tasks_parallel
        Croupier::Task.tasks.keys.each do |k|
          File.exists?(k).should be_true
        end
      end
    end
  end

  it "should report all tasks required to produce an output" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.dependencies("output4").should eq ["output3", "output4"]
      end
    end
  end

  it "should report all tasks required to produce multiple outputs" do
    with_tasks do
      Dir.cd "spec/files" do
        Croupier::Task.dependencies(["output4", "output5"]).should eq ["output3", "output4", "output5"]
      end
    end
  end

  it "should fail if asked for dependencies of an unknown output" do
    with_tasks do
      Dir.cd "spec/files" do
        expect_raises(Exception) do
          Croupier::Task.dependencies("output99")
        end
      end
    end
  end
end
