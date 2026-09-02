require "./spec_helper"
require "file_utils"
include Croupier

describe "TaskManager" do
  describe "Master/Subtask Tasks" do
    it "should create a master task with master_task flag" do
      with_scenario("empty") do
        master = Task.new(
          id: "test_master",
          inputs: [] of String,
          always_run: true,
          master_task: true,
        ) do
          nil
        end

        master.master_task?.should be_true
        TaskManager.tasks["test_master"].master_task?.should be_true
      end
    end

    it "should allow master task to create subtasks" do
      with_scenario("empty") do
        # Create content directory
        Dir.mkdir_p("content")

        # Create some test content files
        File.write("content/file1.md", "Content 1")
        File.write("content/file2.md", "Content 2")

        # Master task creates subtasks
        master = Task.new(
          id: "content_master",
          inputs: ["content/"],
          always_run: true,
          master_task: true,
        ) do
          current_files = Dir.glob("content/**/*.md").to_set

          # Create subtask for each file
          current_files.each do |file|
            subtask_id = "render_#{Digest::SHA1.hexdigest(file)[0..6]}"
            output_file = "output/#{File.basename(file, ".md")}.html"

            subtask = Task.new(
              id: subtask_id,
              inputs: [file],
              outputs: [output_file],
            ) do
              File.read(file)
            end

            TaskManager.register_subtask("content_master", subtask)
          end

          nil
        end

        # Run the master task
        TaskManager.run_tasks

        # Verify subtasks were registered
        master.subtask_ids.size.should eq 2
        TaskManager.tasks.size.should be > 1 # Master + subtasks

        # Verify subtasks are registered (check by their outputs)
        TaskManager.tasks.has_key?("output/file1.html").should be_true
        TaskManager.tasks.has_key?("output/file2.html").should be_true
      end
    end

    it "should track subtasks in master's subtask_ids set" do
      with_scenario("empty") do
        Dir.mkdir_p("content")
        File.write("content/test.md", "Test content")

        master = Task.new(
          id: "test_master",
          inputs: [] of String,
          always_run: true,
          master_task: true,
        ) do
          subtask = Task.new(
            id: "subtask_1",
            inputs: ["content/test.md"],
            outputs: ["output/test.html"],
          ) do
            "test content"
          end

          TaskManager.register_subtask("test_master", subtask)
          nil
        end

        TaskManager.run_tasks

        # Master should track the subtask
        master.subtask_ids.should contain "subtask_1"
      end
    end

    it "should remove subtasks when remove_subtasks is called" do
      with_scenario("empty") do
        Dir.mkdir_p("content")
        File.write("content/test.md", "Test content")

        # Create master and subtasks
        master = Task.new(
          id: "test_master",
          inputs: [] of String,
          always_run: true,
          master_task: true,
        ) do
          subtask1 = Task.new(
            id: "subtask_1",
            inputs: ["content/test.md"],
            outputs: ["output1.html"],
          ) do
            "content1"
          end

          subtask2 = Task.new(
            id: "subtask_2",
            inputs: ["content/test.md"],
            outputs: ["output2.html"],
          ) do
            "content2"
          end

          TaskManager.register_subtask("test_master", subtask1)
          TaskManager.register_subtask("test_master", subtask2)
          nil
        end

        TaskManager.run_tasks

        # Verify subtasks exist
        master.subtask_ids.size.should eq 2
        TaskManager.tasks.has_key?("output1.html").should be_true
        TaskManager.tasks.has_key?("output2.html").should be_true

        # Remove subtasks
        TaskManager.remove_subtasks("test_master")

        # Subtasks should be gone
        master.subtask_ids.size.should eq 0
        TaskManager.tasks.has_key?("output1.html").should be_false
        TaskManager.tasks.has_key?("output2.html").should be_false
      end
    end

    it "should rebuild graph when subtasks are registered" do
      with_scenario("empty") do
        # Build initial graph
        Task.new(
          inputs: [] of String,
          outputs: ["initial.txt"],
        ) do
          "initial"
        end

        _, initial_sorted = TaskManager.sorted_task_graph
        initial_sorted.size.should eq 1

        # Add master task that creates subtasks
        Task.new(
          id: "test_master",
          inputs: [] of String,
          always_run: true,
          master_task: true,
        ) do
          subtask = Task.new(
            id: "subtask_1",
            inputs: [] of String,
            outputs: ["output1.html"],
          ) do
            "content"
          end

          TaskManager.register_subtask("test_master", subtask)
          nil
        end

        TaskManager.run_tasks

        # Check that subtask was registered
        TaskManager.tasks.has_key?("output1.html").should be_true

        # Graph should be rebuilt with new tasks
        # We have: initial.txt, output1.html, and the master task (test_master)
        _, new_sorted = TaskManager.sorted_task_graph
        new_sorted.size.should eq 3
      end
    end

    it "should not merge master tasks with non-master tasks" do
      with_scenario("empty") do
        # Create a regular task
        Task.new(
          outputs: ["test.txt"],
        ) do
          "regular content"
        end

        # Try to create a master task with same output (should fail)
        expect_raises(Exception, /Cannot merge master task with non-master task/) do
          Task.new(
            outputs: ["test.txt"],
            master_task: true,
          ) do
            nil
          end
        end
      end
    end

    it "should persist subtask list across runs using k/v store" do
      with_scenario("empty") do
        Dir.mkdir_p("content")
        File.write("content/file1.md", "Content 1")

        # First run - create subtasks
        Task.new(
          id: "content_master",
          inputs: ["content/"],
          always_run: true,
          master_task: true,
        ) do
          files = Dir.glob("content/**/*.md").to_set

          files.each do |file|
            subtask_id = "render_#{Digest::SHA1.hexdigest(file)[0..6]}"
            subtask = Task.new(
              id: subtask_id,
              inputs: [file],
              outputs: ["output/#{File.basename(file, ".md")}.html"],
            ) do
              File.read(file)
            end

            TaskManager.register_subtask("content_master", subtask)
          end

          # Store the file list
          TaskManager.set("content_subtasks", files.to_a.join("\n"))
          nil
        end

        TaskManager.run_tasks

        # Verify data was stored
        stored_data = TaskManager.get("content_subtasks")
        stored_data.should_not be_nil
        stored_data.as(String).should contain "content/file1.md"
      end
    end

    it "should invalidate graph cache when subtasks change" do
      with_scenario("empty") do
        Dir.mkdir_p("content")
        File.write("content/test.md", "Test")

        Task.new(
          id: "test_master",
          inputs: [] of String,
          always_run: true,
          master_task: true,
        ) do
          subtask = Task.new(
            id: "dynamic_subtask",
            inputs: [] of String,
            outputs: ["dynamic.html"],
          ) do
            "dynamic content"
          end

          TaskManager.register_subtask("test_master", subtask)
          nil
        end

        TaskManager.run_tasks

        # Check that graph rebuild flag was set
        TaskManager.@graph_invalidated.should be_true
      end
    end

    it "should allow subtasks to depend on master's inputs" do
      with_scenario("empty") do
        Dir.mkdir_p("content")
        Dir.mkdir_p("templates")
        File.write("content/page.md", "Page content")
        File.write("templates/layout.html", "<html>{{content}}</html>")

        Task.new(
          id: "content_master",
          inputs: ["content/"],
          always_run: true,
          master_task: true,
        ) do
          subtask = Task.new(
            id: "render_page",
            inputs: ["content/page.md", "templates/layout.html"],
            outputs: ["output/page.html"],
          ) do
            content = File.read("content/page.md")
            layout = File.read("templates/layout.html")
            layout.sub("{{content}}", content)
          end

          TaskManager.register_subtask("content_master", subtask)
          nil
        end

        TaskManager.run_tasks

        # Subtask should have both inputs
        subtask = TaskManager.tasks["output/page.html"]
        subtask.inputs.should contain "content/page.md"
        subtask.inputs.should contain "templates/layout.html"
      end
    end

    it "should work in auto mode with master tasks" do
      with_scenario("empty") do
        Dir.mkdir_p("content")

        # Create file before defining master task to avoid cycle
        File.write("content/test.md", "Initial content")

        Task.new(
          id: "auto_master",
          always_run: true,
          master_task: true,
        ) do
          files = Dir.glob("content/**/*.md").to_set

          # Get previous files from k/v store
          previous_data = TaskManager.get("auto_content_subtasks")
          previous_files = previous_data ? previous_data.split("\n").to_set : Set(String).new

          # Remove deleted file subtasks
          (previous_files - files).each do |deleted_file|
            subtask_id = "render_#{Digest::SHA1.hexdigest(deleted_file)[0..6]}"
            TaskManager.tasks.each do |key, task|
              TaskManager.tasks.delete(key) if task.id == subtask_id
            end
          end

          # Create new file subtasks
          (files - previous_files).each do |new_file|
            subtask_id = "render_#{Digest::SHA1.hexdigest(new_file)[0..6]}"
            subtask = Task.new(
              id: subtask_id,
              inputs: [new_file],
              outputs: ["output/#{File.basename(new_file, ".md")}.html"],
            ) do
              File.read(new_file)
            end

            TaskManager.register_subtask("auto_master", subtask)
          end

          TaskManager.set("auto_content_subtasks", files.to_a.join("\n"))
          nil
        end

        # Run tasks - master task will create subtask
        TaskManager.run_tasks
        # Run again to execute the newly created subtask
        TaskManager.run_tasks

        # Initial file should have been processed
        TaskManager.tasks.has_key?("output/test.html").should be_true
        File.exists?("output/test.html").should be_true
        File.read("output/test.html").should eq "Initial content"

        # Now test auto mode - start watching for changes to content files
        TaskManager.auto_run
        sleep 0.1.seconds

        # Modify a file
        File.write("content/test.md", "Modified content")
        sleep 0.2.seconds

        # File should be regenerated
        File.read("output/test.html").should eq "Modified content"

        TaskManager.auto_stop
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
        TaskManager.add_mutex("db")
        TaskManager.before_run_hook = ->(_changes : Set(String)) { File.write("hook_fired", "") }
        TaskManager.progress_callback = ->(_id : String) { File.write("progress_fired", "") }

        TaskManager.cleanup

        TaskManager.state_file.should eq ".croupier"
        TaskManager.early_cutoff?.should be_true
        TaskManager.mutexes.empty?.should be_true

        # The hooks are gone: running a task must not fire them
        Task.new(output: "out", inputs: ["seed"]) { "data" }
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
        sleep 0.2.seconds
        # The autorun fiber (plus the watcher's) are running
        during = live_fiber_count
        during.should be > baseline

        TaskManager.cleanup
        TaskManager.@autorun_running.should be_false
        sleep 0.2.seconds
        # The autorun fiber and the watcher's reader are gone. (One
        # inotify event-loop fiber stays parked on the library's own
        # channel forever — an upstream leak croupier can't retire.)
        live_fiber_count.should be < during
      end
    end
  end

  describe "fiber hygiene" do
    it "should not leak worker fibers from parallel runs" do
      seeds = (0...20).map { |i| {"seed_#{i}" => "data"} of String => String }
        .reduce { |acc, hash| acc.merge(hash) }
      with_scenario("empty", to_create: seeds) do
        seeds.each_key { |k| Task.new(output: "out_#{k}", inputs: [k]) { "data" } }

        # First round absorbs the one-time cost of resizing the fiber
        # execution context (its threads show up as extra loop fibers
        # and live as long as the process)
        TaskManager.run_tasks(parallel: true)
        5.times { TaskManager.scan_inputs }
        sleep 0.1.seconds
        baseline = live_fiber_count

        # A second round of the same width must not grow the fiber
        # count: worker fibers that drained their (closed) queue exit,
        # fibers parked on a never-closed channel accumulate forever.
        # (All seeds change so the wave width — and thus the execution
        # context resize — matches the first round.)
        seeds.each_key { |k| File.write(k, "changed") }
        TaskManager.run_tasks(parallel: true)
        5.times { TaskManager.scan_inputs }
        sleep 0.1.seconds

        live_fiber_count.should eq baseline
      end
    end
  end
end
