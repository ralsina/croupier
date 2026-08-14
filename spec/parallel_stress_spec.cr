require "./spec_helper"
include Croupier

# Regression specs for the data races in the parallel runner.
#
# Parallel task workers run on multiple OS threads, so all shared
# bookkeeping (failed/finished/error collections, next_run and k/v store
# writes, early-cutoff notifications) must be synchronized. Before it
# was, enough failing tasks would abort the process with a Boehm GC
# "Duplicate large block deallocation" or segfault. These specs stress
# those paths hard enough to catch the races; they must always pass
# cleanly, with no crashes and no dropped results.
describe "Parallel stress" do
  it "survives thousands of failing tasks" do
    with_scenario("empty") do
      task_count = 4000
      task_count.times do |i|
        Task.new(output: "failing_#{i}", inputs: [] of String) { raise "boom #{i}" }
      end

      # Every failure must be collected and reported, not crash the GC.
      expect_raises(Exception, /failed: boom /) do
        TaskManager.run_tasks(parallel: true, run_all: true)
      end
    end
  end

  it "survives thousands of successful tasks writing files and k/v data" do
    with_scenario("empty") do
      file_tasks = 2000
      kv_tasks = 2000
      file_tasks.times do |i|
        Task.new(output: "out_#{i}", inputs: [] of String) { "content_#{i}" }
      end
      kv_tasks.times do |i|
        Task.new(output: "kv://key_#{i}", inputs: [] of String) { "value_#{i}" }
      end

      TaskManager.run_tasks(parallel: true, run_all: true)

      # No writes may be lost to racing hash/set growth.
      file_tasks.times { |i| File.read("out_#{i}").should eq("content_#{i}") }
      kv_tasks.times { |i| TaskManager.get("key_#{i}").should eq("value_#{i}") }
    end
  end

  it "survives early-cutoff notifications across many workers" do
    # Roots read a seed file so their staleness can flip to fresh once run
    # (input-less tasks report stale forever — a separate pre-existing
    # issue that would make the dependents unreachable).
    with_scenario("empty", to_create: {"seed" => "seed content"}) do
      root_count = 25
      dependent_count = 200
      # Every dependent reads every root output, so one unchanged root
      # triggers dependent_count mark_dependency_fresh notifications.
      root_outputs = (0...root_count).map { |i| "root_#{i}" }
      root_outputs.each do |output|
        Task.new(output: output, inputs: ["seed"]) { "stable content" }
      end
      dependent_count.times do |i|
        Task.new(output: "dependent_#{i}", inputs: root_outputs, always_run: true) { "stable content" }
      end

      # First run: outputs are new, so no early cutoff fires.
      TaskManager.run_tasks(parallel: true, run_all: true)
      # Second run: outputs are unchanged, so finishing root tasks notify
      # all dependents from their worker fibers while siblings run.
      TaskManager.run_tasks(parallel: true, run_all: true)

      dependent_count.times { |i| File.exists?("dependent_#{i}").should be_true }
    end
  end
end
