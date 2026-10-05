{% if flag?(:linux) %}
  require "./spec_helper"
  include Croupier

  describe Croupier::LinuxWatcher do
    # The callback fires from the inotify event fiber, so assertions
    # wait for it instead of sleeping fixed amounts
    it "reports changes to a watched file exactly" do
      with_scenario("empty") do
        events = [] of String
        event_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          event_lock.synchronize { events << input }
        })
        File.touch("f.txt")
        watcher.watch("f.txt")
        # Changes before watch are not detected; this one is
        File.write("f.txt", "1")
        wait_until(message: "exact match never fired") do
          event_lock.synchronize { events.includes?("f.txt") }
        end
        watcher.close
      end
    end

    it "reports a directory input when a file inside changes" do
      with_scenario("empty") do
        Dir.mkdir("dir")
        events = [] of String
        event_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          event_lock.synchronize { events << input }
        })
        watcher.watch("dir")
        File.write("dir/child.txt", "1")
        wait_until(message: "prefix match never fired") do
          event_lock.synchronize { events.includes?("dir") }
        end
        watcher.close
      end
    end

    it "re-watches a file after editor replacement" do
      with_scenario("empty") do
        # The callback records the file's CONTENT at event time: the
        # only way a callback can observe content "3" is a write to
        # the REPLACEMENT inode after the rename, i.e. proof the
        # watcher re-armed. (The old inode's IN_IGNORED produces no
        # callback, and the parent directory is not watched for an
        # existing input, so the rename itself fires nothing.)
        contents = [] of String
        contents_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          contents_lock.synchronize { contents << File.read(input) }
        })
        File.write("f.txt", "1") # setup: before watch, no event
        watcher.watch("f.txt")
        File.write("f.txt", "1") # fires IN_MODIFY on the watched inode
        wait_until(message: "first write never fired") do
          contents_lock.synchronize { contents.includes?("1") }
        end

        # Editor replacement: a new inode replaces the watched one
        File.write("f.txt.tmp", "2")
        File.rename("f.txt.tmp", "f.txt")

        # The replacement fires exactly one more callback: the
        # IN_IGNORED falls through to the exact-match branch (that
        # queueing is intentional — it processes a single-save
        # editor's content). Wait for it, then let the queue settle.
        File.write("f.txt.tmp", "2")
        File.rename("f.txt.tmp", "f.txt")
        pre_rename = contents_lock.synchronize { contents.size }
        wait_until(message: "replacement callback never fired") {
          contents_lock.synchronize { contents.size > pre_rename }
        }
        baseline = 0
        wait_until(message: "callbacks never settled") {
          baseline = contents_lock.synchronize { contents.size }
          3.times { Fiber.yield; sleep 5.milliseconds }
          baseline == contents_lock.synchronize { contents.size }
        }

        # A write to the replacement inode fires only if the watcher
        # re-armed on the new inode: without the re-watch, nothing
        # watches it and this wait times out
        File.write("f.txt", "3")
        wait_until(message: "post-replacement write never fired") {
          contents_lock.synchronize { contents.size > baseline }
        }
        watcher.close
      end
    end

    it "covers a missing input through its parent directory" do
      with_scenario("empty") do
        events = [] of String
        event_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          event_lock.synchronize { events << input }
        })
        watcher.watch("later.txt")
        File.write("later.txt", "created")
        wait_until(message: "creation never fired") do
          event_lock.synchronize { events.includes?("later.txt") }
        end
        watcher.close
      end
    end
  end
{% end %}
