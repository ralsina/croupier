require "./spec_helper"
require "file_utils"
include Croupier

describe "TaskManager" do
  describe "save_run" do
    it "should save this_run and next_run merged" do
      with_scenario("empty") do
        TaskManager.this_run = {"foo" => "bar"}
        TaskManager.next_run = {"bat" => "quux"}
        TaskManager.save_run
        # __scan_time is a wall-clock timestamp; only check its presence
        state = YAML.parse(File.read(".croupier"))
        state["__version"].to_s.should eq "1"
        state["__scan_time"].to_s.should_not be_empty
        state["foo"].to_s.should eq "bar"
        state["bat"].to_s.should eq "quux"
      end
    end

    it "should save all inputs and outputs on a full run" do
      with_scenario("basic", to_create: {"input" => "foo", "input2" => "bar"}) do
        TaskManager.run_tasks
        state = YAML.parse(File.read(".croupier")).as_h.reject { |key, _| key.to_s == "__scan_time" }
        state.should eq({
          "__version" => "1",
          "input"     => "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33",
          "input2"    => "62cdb7020ff920e5aa642c3d4066950dd1f01f4d",
          "output3"   => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "output4"   => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "output5"   => "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "output1"   => "356a192b7913b04c54574d18c28d46e6395428ab",
          "output2"   => "0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33",
        } of String => String)
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

    it "should compute each node's own closure on a diamond (memoization)" do
      # Diamond: root -> left, root -> right, left -> sink, right -> sink
      # The memo caches each node's transitive closure. Querying several
      # outputs together exercises the shared memo across reconvergent
      # paths; each individual query must still resolve to exactly that
      # node's own transitive dependencies.
      with_scenario("empty", to_create: {"root_in" => "x"}) do
        Task.new(outputs: ["root"], inputs: ["root_in"]) { "root" }
        Task.new(outputs: ["left"], inputs: ["root"]) { "left" }
        Task.new(outputs: ["right"], inputs: ["root"]) { "right" }
        Task.new(outputs: ["sink"], inputs: ["left", "right"]) { "sink" }

        # Querying several outputs at once exercises the shared memo.
        TaskManager._dependencies(["left", "right", "sink"])

        # Each node's own closure must be exact, regardless of query order.
        TaskManager._dependencies(["sink"]).should eq Set.new(["root", "left", "right", "sink"])
        TaskManager._dependencies(["left"]).should eq Set.new(["root", "left"])
        TaskManager._dependencies(["right"]).should eq Set.new(["root", "right"])
        TaskManager._dependencies(["root"]).should eq Set.new(["root"])

        # And the public API returns the full diamond in dependency order.
        TaskManager.dependencies("sink").should eq ["root", "left", "right", "sink"]
      end
    end
  end

  describe "store" do
    it "should cache store reads in memory" do
      with_scenario("empty") do
        TaskManager.use_persistent_store("store")

        # A miss on a fresh cache reads the (empty) store and is
        # remembered, so repeated missing-key lookups don't re-read
        TaskManager.get("missing").should be_nil
        TaskManager.@store_misses.includes?("missing").should be_true

        # set invalidates the miss and caches the value
        TaskManager.set("foo", "bar")
        TaskManager.@store_cache["foo"].should eq "bar"
        TaskManager.@store_misses.includes?("foo").should be_false
        TaskManager.get("foo").should eq "bar"

        # Pre-existing keys on disk are picked up lazily (one path per
        # manager lifetime, so reset first)
        TaskManager.cleanup
        TaskManager.use_persistent_store("store2")
        TaskManager.@_store.as(Kiwi::FileStore).set("preexisting", "42")
        TaskManager.get("preexisting").should eq "42"
        TaskManager.@store_cache["preexisting"].should eq "42"

        # cleanup resets the caches along with the store
        TaskManager.cleanup
        TaskManager.@store_cache.empty?.should be_true
        TaskManager.@store_misses.empty?.should be_true
      end
    end

    it "should only mark kv keys modified when the value changes" do
      with_scenario("empty") do
        TaskManager.set("foo", "bar")
        TaskManager.modified?("kv://foo").should be_true

        TaskManager.modified.clear
        # Setting the same value again is a no-op for staleness
        TaskManager.set("foo", "bar")
        TaskManager.modified?("kv://foo").should be_false

        # A different value still marks it
        TaskManager.set("foo", "baz")
        TaskManager.modified?("kv://foo").should be_true
      end
    end

    it "should swap output hashes atomically" do
      with_scenario("empty") do
        TaskManager.last_run["out"] = "old"
        TaskManager.swap_output_hash("out", "new").should eq "old"
        # The new hash is recorded for the next run's state file...
        TaskManager.next_run["out"].should eq "new"
        # ...and the last-run answer is untouched
        TaskManager.last_run["out"].should eq "old"
      end
    end

    it "should cache known-to-exist files until the next run starts" do
      with_scenario("empty", to_create: {"input" => "foo"}) do
        # First check stats the filesystem and remembers the answer
        TaskManager.file_exists?("input").should be_true

        # A file that appeared later is picked up too (only positive
        # results are cached, missing files are rechecked)
        TaskManager.file_exists?("late").should be_false
        File.write("late", "x")
        TaskManager.file_exists?("late").should be_true

        # Positive answers are cached: even if the file disappears, the
        # answer stays true until a run start clears the cache
        File.delete("input")
        TaskManager.file_exists?("input").should be_true

        # Starting a run resets the cache and re-stats
        TaskManager.run_tasks
        TaskManager.file_exists?("input").should be_false
      end
    end

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
        # The chosen path should be recorded so the same-path guard works
        TaskManager.@_store_path.should eq "store"
        # Calling again with the same path is a no-op and must not crash
        # (previously @_store_path was never set, so the second call would
        # try to cast a FileStore back to MemoryStore and raise)
        TaskManager.use_persistent_store("store")
        TaskManager.get("foo").should eq "bar"
        # Switching to a different path is refused
        expect_raises(Exception, "Can't change persistent k/v store path") do
          TaskManager.use_persistent_store("other")
        end
      end
    end

    it "should call before_run_hook before tasks run" do
      with_scenario("empty") do
        hook_called = false
        hook_changes = Set(String).new

        # Create a task that depends on a file
        File.open("input", "w") { |f| f << "initial" }
        Task.new(inputs: ["input"], output: "output",
          proc: TaskProc.new {
            File.open("output", "w") { |f| f << "done" }
            "output"
          })

        TaskManager.before_run_hook = ->(changes : Set(String)) {
          hook_called = true
          hook_changes = changes.dup
        }

        TaskManager.auto_run
        # Initially no changes, hook not called
        hook_called.should be_false

        # Trigger a change
        File.open("input", "w") << "modified"
        wait_until(message: "before_run_hook never called") { hook_called }

        # Hook should have been called with the changed file
        hook_changes.should contain "input"

        TaskManager.auto_stop
        # Reset hook for other tests
        TaskManager.before_run_hook = ->(_changes : Set(String)) { }
      end
    end
  end

  describe "hash_directory" do
    it "walks the same entries the previous glob-based scan did" do
      with_scenario("empty") do
        Dir.mkdir_p("tree/nested/deeper")
        Dir.mkdir_p("tree/.hiddendir")
        File.write("tree/file.txt", "one")
        File.write("tree/nested/.dotfile", "two")
        File.write("tree/nested/deeper/x[1].txt", "three")
        File.write("tree/.hiddendir/y", "four")

        # In fast_dirs mode the digest is the bare entry list, so this
        # asserts the walked list equals the glob reference exactly
        # (same files, subdirectories, dotfiles and nesting)
        expected = Dir.glob(
          "tree/**/*",
          match: File::MatchOptions.glob_default | File::MatchOptions::DotFiles
        ).sort
        TaskManager.fast_dirs = true
        TaskManager.hash_directory("tree").should eq Digest::SHA1.hexdigest(expected.join("\n"))
      end
    end

    it "treats glob metacharacters in the directory name literally" do
      with_scenario("empty") do
        Dir.mkdir_p("assets[2]")
        File.write("assets[2]/file", "one")
        digest = TaskManager.hash_directory("assets[2]")

        # The digest must reflect the real contents: as a pattern,
        # "assets[2]" only matches a directory named "assets2" (which
        # doesn't exist), so the old glob-based scan hashed an empty
        # entry list and never saw the file change
        File.write("assets[2]/file", "two")
        TaskManager.hash_directory("assets[2]").should_not eq digest
      end
    end

    it "hashes the contents of a directory input reached through a symlink" do
      with_scenario("empty") do
        Dir.mkdir_p("real")
        File.write("real/file", "one")
        File.symlink("real", "link")

        # In fast_dirs mode the digest is the bare entry list, so this
        # pins the walked list: a symlinked root is followed and its
        # real contents hashed (the old glob-based scan saw an empty
        # tree here, because it refused to descend the symlink)
        TaskManager.fast_dirs = true
        TaskManager.hash_directory("link").should eq Digest::SHA1.hexdigest("link/file")
      end
    end
  end
end
