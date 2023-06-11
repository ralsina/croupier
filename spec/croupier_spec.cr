require "./spec_helper"

dummy_proc = ->{ "" }
x = 0
counter_proc = ->{
  x += 1
  x.to_s
}

describe Croupier::Task do
  it "should be able to create a task" do
    task = Croupier::Task.new("name", "output", [] of String, dummy_proc)
    task.@name.should eq "name"
    task.@output.should eq "output"
    task.@inputs.should eq [] of String
    task.@stale.should be_false
  end

  it "should have a nice string representation" do
    Croupier::Task.tasks["output"].to_s.should eq "name::output"
  end

  it "should be registered" do
    Croupier::Task.tasks.has_key?("output").should eq true
  end

  it "should not allow two tasks with same output" do
    expect_raises(Exception) do
      Croupier::Task.new("name", "output", [] of String, dummy_proc)
    end
  end

  it "should reject self-cyclical tasks" do
    expect_raises(Exception) do
      Croupier::Task.new("name", "output6", ["output6"], dummy_proc)
    end
  end

  it "should execute the task's proc when Task.run is called" do
    counter_task = Croupier::Task.new(
      "name",
      "output2",
      [] of String,
      counter_proc,
      no_save: true)
    y = x
    counter_task.run
    x.should eq y + 1
    counter_task.run
    x.should eq y + 2
  end

  it "should be stale if an input is marked modified" do
    task = Croupier::Task.new("name", "output3", ["input"], dummy_proc)
    task.stale?.should be_false
    Croupier::Task.mark_modified("input")
    task.stale?.should be_true
  end

  it "should be stale if a dependent task is stale" do
    task = Croupier::Task.new("name", "output4", ["output3"], dummy_proc)
    task.stale?.should be_true
  end

  it "should list all inputs for all tasks" do
    Croupier::Task.all_inputs.should eq ["input", "output3"]
  end

  it "should create a topologically sorted task graph" do
    Croupier::Task.new("name", "output5", ["input2"], dummy_proc)
    expected = {
      "root"    => Set{"output", "output2", "input", "input2"},
      "output"  => Set(String).new,
      "output2" => Set(String).new,
      "output3" => Set{"output4"},
      "input"   => Set{"output3"},
      "output4" => Set(String).new,
      "output5" => Set(String).new,
      "input2"  => Set{"output5"},
    }
    g, s = Croupier::Task.sorted_task_graph
    g.@vertice_dict.should eq expected
    s.size.should eq expected.size
    s.should eq [
      "root",
      "input2",
      "output5",
      "input",
      "output3",
      "output4",
      "output2",
      "output",
    ]
  end

  it "should detect cycles in the graph" do
    expect_raises(Exception) do
      Croupier::Task.new("name", "output4", ["input"], dummy_proc)
      Croupier::Task.sorted_task_graph
    end
  end

  it "should run all tasks" do
    # TODO: improve this test
    Dir.cd "spec/files" do
      y = x
      Croupier::Task.run_tasks(run_all: true)
      x.should eq y + 1
    end
  end

  it "should calculate hashes for all inputs" do
    expected = {
      "input"   => "f1d2d2f924e986ac86fdf7b36c94bcdf32beec15",
      "input2"  => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
      "output3" => "adc83b19e793491b1c6ea0fd8b46cd9f32e592fc",
    }
    Dir.cd "spec/files" do
      Croupier::Task.scan_inputs.should eq expected
    end
  end

  it "should not hash files that don't exist" do
    # This is running where the files don't exist
    Croupier::Task.scan_inputs.size.should eq 0
  end

  it "should save files but respect the no_save flag" do
    Dir.cd "spec/files" do
      File.delete?(".croupier")
      File.delete?("output")
      File.delete?("output2")

      Croupier::Task.run_tasks(run_all: true)

      # The output task has no_save = false, so it should be created
      File.exists?("output").should be_true
      # The output2 task has no_save = true, so it should not be created
      File.exists?("output2").should be_false
    end
  end

  it "should mark all tasks with inputs as stale if there is no .croupier file" do
    Dir.cd "spec/files" do
      File.delete?(".croupier")
      # Make sure no files are modified
      Croupier::Task.clear_modified
      tasks = Croupier::Task.tasks
      tasks.size.should eq 5
      tasks.values.count(&.stale?).should eq 0

      Croupier::Task.mark_stale

      # Only tasks with inputs should be stale
      tasks.values.select(&.stale?).map(&.@output).should eq ["output3", "output4", "output5"]
    end
  end

  it "should mark tasks depending indirectly on a modified file as stale" do
    Dir.cd "spec/files" do
      File.delete?(".croupier")
      # Make sure only "input" is modified
      Croupier::Task.clear_modified
      tasks = Croupier::Task.tasks
      tasks.size.should eq 5
      tasks.values.count(&.stale?).should eq 0

      Croupier::Task.mark_modified("input")

      # Only tasks depending on "input" should be stale
      tasks.values.count(&.stale?).should eq 2
      tasks.keys.select { |k| tasks[k].stale? }.should eq ["output3", "output4"]
    end
  end

  it "should mark file with wrong hash as modified" do
    Dir.cd "spec/files" do
      File.delete?(".croupier")
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

      Croupier::Task.mark_stale

      Croupier::Task.modified.should eq Set{"input"}
    end
  end
end
