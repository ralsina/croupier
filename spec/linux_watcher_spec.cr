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
        # The callback records the file's CONTENT at event time.
        # Sequence: (1) a write on the watched inode fires two
        # callbacks (IN_MODIFY + IN_CLOSE_WRITE); (2) the replacement
        # makes the old watch deliver IN_ATTRIB + the terminal
        # IN_IGNORED, and both fall through to the exact-match branch
        # on purpose (that queueing processes a single-save editor's
        # content) -> 4 callbacks; (3) a write to the replacement
        # inode fires only if the watcher re-armed on the new inode
        # -> 6 callbacks. IN_IGNORED is terminal for the old watch,
        # so step 3 times out if the re-watch is broken.
        contents = [] of String
        contents_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          contents_lock.synchronize { contents << File.read(input) }
        })
        File.write("f.txt", "1") # setup: before watch, no event
        watcher.watch("f.txt")
        File.write("f.txt", "1") # fires IN_MODIFY + IN_CLOSE_WRITE
        wait_until(message: "first write never fired") {
          contents_lock.synchronize { contents.size >= 2 }
        }

        File.write("f.txt.tmp", "2")
        File.rename("f.txt.tmp", "f.txt")
        wait_until(message: "replacement callbacks never fired") {
          contents_lock.synchronize { contents.size >= 4 }
        }

        File.write("f.txt", "3")
        wait_until(message: "post-replacement write never fired") {
          contents_lock.synchronize { contents.size >= 6 }
        }
        # The re-armed watch saw the NEW content, not just any extra
        # event
        contents_lock.synchronize { contents.last.should eq "3" }
        watcher.close
      end
    end

    it "survives the watched file's directory being removed entirely" do
      # rm -rf d while d/f.txt is watched: the IN_IGNORED re-watch
      # used to hit the also-deleted parent and raise inside the
      # shard's event fiber, silently stopping ALL further events
      with_scenario("empty") do
        events = [] of String
        event_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          event_lock.synchronize { events << input }
        })
        Dir.mkdir("d")
        File.write("d/f.txt", "x")
        File.write("other.txt", "x")
        watcher.watch("d/f.txt")
        watcher.watch("other.txt")

        FileUtils.rm_rf("d")
        # The old watch's IN_IGNORED fallthrough may or may not have
        # fired for d/f.txt; what matters is that events keep flowing
        File.write("other.txt", "y")
        wait_until(message: "other.txt change never arrived after the watched directory was removed") {
          event_lock.synchronize { events.includes?("other.txt") }
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
