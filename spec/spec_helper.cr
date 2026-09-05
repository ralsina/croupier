require "spec"
require "../src/croupier"
require "file_utils"

# Count live fibers via the stdlib's registry: terminated fibers are
# removed from it, so worker fibers that exit after their work queue is
# drained don't count, while fibers parked forever on a channel that is
# never closed do.
def live_fiber_count : Int32
  count = 0
  Fiber.each { count += 1 }
  count
end

# Wait until the block returns true, yielding to other fibers while
# waiting (that is how the watcher and autorun fibers get scheduled)
# and raising after `timeout`. Replaces fixed sleeps in asynchronous
# specs: how long a change takes to be detected depends on machine
# load, but the condition itself is deterministic.
def wait_until(timeout : Time::Span = 5.seconds, message : String = "condition never became true", &) : Nil
  deadline = Time.instant + timeout
  until yield
    Fiber.yield
    sleep 1.millisecond
    if Time.instant > deadline
      raise "#{message} within #{timeout.total_milliseconds}ms"
    end
  end
end

# True when an auto mode cycle has nothing left to process: no queued
# watcher events and an empty modified set means any cycle in flight
# finished its run, and nothing more can happen until a new filesystem
# event or kv:// write arrives.
def auto_cycle_settled? : Bool
  TaskManager.@queued_changes.empty? && TaskManager.modified.empty?
end

# Sets up a test scenario: enters the scenario directory, cleans up any
# previous state (state file, generated files, TaskManager), creates the
# requested files and tasks from the scenario's tasks.yml, runs the block,
# and cleans up afterwards.
def with_scenario(
  name,
  keep = [] of String,
  to_create = {} of String => String,
  procs = {} of String => TaskProc, &
)
  # Setup logging, helps coverage
  logs = IO::Memory.new
  Log.setup(:trace, Log::IOBackend.new(io: logs))

  # Library of procs - matching the original croupier_spec
  x = 0
  _procs = {
    "dummy"   => TaskProc.new { "" },
    "counter" => TaskProc.new {
      x += 1
      x.to_s
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
      FileUtils.rm_rf(f) unless keep.includes?(f) || f == "tasks.yml"
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
          outputs: t["outputs"].as_a.map(&.to_s),
          inputs: t["inputs"].as_a.map(&.to_s),
          proc: _procs[t["procs"]],
          always_run: t["always_run"]?.try(&.as_bool) || false,
          no_save: t["no_save"]?.try(&.as_bool) || false,
          id: t["id"]?.try(&.to_s),
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
