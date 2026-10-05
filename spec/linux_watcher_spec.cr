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
        events = [] of String
        event_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          event_lock.synchronize { events << input }
        })
        File.touch("f.txt")
        watcher.watch("f.txt")
        File.write("f.txt", "1")
        wait_until(message: "first write never fired") do
          event_lock.synchronize { events.includes?("f.txt") }
        end

        # Editors replace the file: a new inode replaces the watched
        # one, and the watcher must re-arm on its own
        File.write("f.txt.tmp", "2")
        File.rename("f.txt.tmp", "f.txt")
        wait_until(message: "replacement never fired") do
          event_lock.synchronize { events.size >= 2 }
        end
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
