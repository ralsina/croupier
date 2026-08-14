require "spec"
require "../src/croupier"
require "file_utils"

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
          always_run: t["always_run"].as_bool,
          no_save: t["no_save"].as_bool,
          id: t["id"].to_s,
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
