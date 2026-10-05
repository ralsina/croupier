{% if flag?(:linux) %}
  require "./spec_helper"
  include Croupier

  describe "probe" do
    it "logs events across rename with re-watch disabled" do
      with_scenario("empty") do
        # disable the re-watch
        # (patch via subclass not possible; we log events instead)
        contents = [] of String
        c_lock = Sync::Mutex.new
        watcher = LinuxWatcher.new(->(input : String) {
          c_lock.synchronize { contents << File.read(input) }
        })
        File.write("f.txt", "1")
        watcher.watch("f.txt")
        File.write("f.txt", "1")
        wait_until(message: "baseline") { c_lock.synchronize { contents.size >= 2 } }
        File.write("f.txt.tmp", "2")
        File.rename("f.txt.tmp", "f.txt")
        sleep 1.seconds
        puts "after rename: #{c_lock.synchronize { contents.size }} callbacks"
        # disable re-watch by closing+rewatching? can't; just observe
        File.write("f.txt", "3")
        sleep 1.seconds
        puts "after write: #{c_lock.synchronize { contents.size }} callbacks"
        watcher.close
      end
    end
  end
{% end %}
