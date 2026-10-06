module Croupier
  # :nodoc:
  # Dependency bookkeeping for one run: which planned tasks can start.
  #
  # Each task counts its unfinished producers. A task whose count
  # reaches zero is skipped if fresh (which releases its consumers in
  # turn), parked if an input that no task produces is still missing
  # (a file or kv:// key another task creates as a side effect), and
  # queued otherwise. A failed task never releases its consumers, so
  # they don't run against a missing or half-written output.
  #
  # Queued tasks come out earliest-in-dependency-order first, so a
  # serial run executes them in exactly the planned order.
  #
  # Used only by the run's coordinating fiber, so it takes no locks.
  class RunPlan
    @tasks = [] of Task
    @position = {} of Task => Int32
    @pending : Array(Int32)
    @consumers : Array(Array(Int32))
    # Count reached zero, not yet sorted into skipped/parked/queued
    @released : Array(Int32)
    # Descending, so pop returns the earliest task
    @queued = [] of Int32
    @parked = [] of Int32
    # Finished, failed or skipped
    @settled : Array(Bool)

    # `task_names` is a dependency order; `registry` resolves names
    # (TaskManager.tasks). A multi-output task appears once.
    def initialize(task_names : Array(String), registry : Hash(String, Task), @run_all : Bool, @dry_run : Bool)
      task_names.each do |name|
        next unless task = registry[name]?
        next if @position.has_key?(task)
        @position[task] = @tasks.size
        @tasks << task
      end
      @pending = Array.new(@tasks.size, 0)
      @consumers = Array.new(@tasks.size) { [] of Int32 }
      @tasks.each_with_index do |task, index|
        producers = task.inputs.compact_map { |input| registry[input]?.try { |producer| @position[producer]? } }
        producers.uniq!.each do |producer|
          @pending[index] += 1
          @consumers[producer] << index
        end
      end
      @settled = Array.new(@tasks.size, false)
      @released = (0...@tasks.size).select { |index| @pending[index] == 0 }
      sort_released
    end

    def size : Int32
      @tasks.size
    end

    # The earliest queued task, or nil when none can start now
    def next_task : Task?
      @queued.pop?.try { |index| @tasks[index] }
    end

    # Record that `task` ran. A success releases its consumers, and
    # may have created a parked task's missing input.
    def finished(task : Task, success : Bool) : Nil
      index = @position[task]
      @settled[index] = true
      return unless success
      release_consumers(index)
      @parked.reject! do |parked|
        next false if @tasks[parked].waiting?
        enqueue(parked)
        true
      end
      sort_released
    end

    # Tasks that never ran and needed to: parked, or blocked behind a
    # failure or a parked task
    def left_over : Array(Task)
      left = [] of Task
      @tasks.each_with_index do |task, index|
        left << task if !@settled[index] && (@run_all || task.stale?)
      end
      left
    end

    # Sort released tasks into skipped (fresh: released consumers are
    # handled in the same loop), parked and queued. Staleness is read
    # here, after early cutoff had its chance to freshen the task.
    private def sort_released : Nil
      while index = @released.pop?
        task = @tasks[index]
        if !@run_all && !task.stale?
          @settled[index] = true
          release_consumers(index)
        elsif @dry_run || !task.waiting?
          enqueue(index)
        else
          @parked << index
        end
      end
    end

    private def release_consumers(index : Int32) : Nil
      @consumers[index].each do |consumer|
        @released << consumer if (@pending[consumer] -= 1) == 0
      end
    end

    private def enqueue(index : Int32) : Nil
      @queued.insert(@queued.bsearch_index { |queued| queued < index } || @queued.size, index)
    end
  end
end
