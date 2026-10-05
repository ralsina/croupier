module Croupier
  # TaskManagerType methods for running tasks, serially and in parallel.
  class TaskManagerType
    # Run the stale tasks needed to create or update `targets` (every
    # task when nil), in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    # If `early_cutoff` is true, skip tasks when upstream outputs are
    # unchanged (defaults to TaskManager.early_cutoff?)
    def run_tasks(
      targets : Array(String)? = nil,
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false,
      early_cutoff : Bool? = nil,
    )
      task_names = targets ? dependencies(targets) : sorted_task_keys

      # Outside auto mode, a missing input fails up front as "Unknown
      # inputs" rather than mid-run as "Waiting for". Auto mode skips
      # the check: inputs that don't exist yet are normal there, runs
      # build whatever is buildable, and the retry backoff keeps
      # failed attempts cheap.
      check_dependencies(targets) unless @auto_mode

      early_cutoff = @early_cutoff if early_cutoff.nil?

      # Counts overlapping runs: task creation and removal are
      # rejected until every run ends (see TaskManager.register_task).
      # Real runs hold the cross-process state lock, so concurrent
      # croupier processes don't overwrite each other's state file.
      @data_mutex.synchronize { @run_active += 1 }
      begin
        with_state_lock(dry_run) do
          if parallel
            _run_tasks_parallel(task_names, run_all, dry_run, keep_going, early_cutoff)
          else
            _run_tasks(task_names, run_all, dry_run, keep_going, early_cutoff)
          end
        end
      ensure
        @data_mutex.synchronize { @run_active -= 1 }
      end
    end

    # Run tasks serially. Private: the run_active guard on task
    # creation and removal holds only if run_tasks is the only way
    # into a run.
    private def _run_tasks(
      task_names,
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false,
      early_cutoff : Bool = true,
    )
      mark_stale_inputs(run_all, task_names)
      propagate_staleness(run_all)

      finished = Set(Task).new
      succeeded = Set(Task).new
      failures = [] of Exception

      # Staleness is checked at visit time, so tasks freshened by early
      # cutoff are skipped. always_run needs no explicit check:
      # propagate_staleness marks those tasks stale and early cutoff
      # never freshens them.
      task_names.each do |name|
        next unless task = tasks.fetch(name, nil)
        next if finished.includes?(task)
        next unless task.stale? || run_all
        Log.debug { "Running task for #{task.outputs}" }
        next unless ensure_runnable(task, keep_going, dry_run)
        failure = run_one(task, dry_run, succeeded)
        if failure
          failures << failure
          # Without keep_going a failure ends the run right away
          # (state is not saved), reporting the failure itself rather
          # than the dependents blocked behind it. Both runners raise
          # RunFailure.
          raise RunFailure.new(failures) unless keep_going
        end
        finished << task

        # Early cutoff: if a successful task's outputs didn't change,
        # notify its dependents (a failed task's dependents must stay
        # blocked)
        if failure.nil? && early_cutoff && !task.outputs_changed?
          notify_dependents_unchanged(task)
        end
      end

      finish_run(task_names, succeeded, failures, keep_going, dry_run)
    end

    # Shared epilogue of both runners. A dry run saves nothing (saving
    # the scanned hashes would hide the changes from the next real
    # run). A real run saves its state, then raises the collected
    # failures so callers can tell the run failed.
    private def finish_run(
      task_names,
      succeeded : Set(Task),
      failures : Array(Exception),
      keep_going : Bool,
      dry_run : Bool,
    ) : Nil
      return if dry_run
      drop_unfinished_inputs(task_names, succeeded)
      save_run
      raise RunFailure.new(failures) if keep_going && !failures.empty?
    end

    # Whether `task` can run now: true when every input is satisfied
    # or this is a dry run. A blocked task is skipped with a warning
    # under keep_going, and raises UnknownInputsError otherwise (hence
    # no `?` in the name).
    private def ensure_runnable(task : Task, keep_going : Bool, dry_run : Bool) : Bool
      return true if task.waiting_for.empty? || dry_run
      raise UnknownInputsError.new("Can't run task for #{task.outputs}: Waiting for #{task.waiting_for}") unless keep_going
      # Blocked behind a failure: skip it and keep going
      Log.warn { "Skipping task for #{task.outputs}: Waiting for #{task.waiting_for}" }
      false
    end

    # Run one task and return its failure instead of raising it, so
    # the runner decides whether to collect or raise. Successful tasks
    # join `succeeded`.
    private def run_one(task : Task, dry_run : Bool, succeeded : Set(Task)) : Exception?
      task.run unless dry_run
      succeeded << task unless dry_run
      nil
    rescue ex
      Log.error { "Error running task for #{task.outputs}: #{ex}" }
      ex
    end

    # Run tasks in parallel, in waves: each wave takes every task
    # that is ready, runs it on a worker pool, and waits for the
    # whole batch before planning the next wave. On Crystal >= 1.21
    # (without -Dpreview_mt) the workers run on several OS threads;
    # elsewhere they share one thread cooperatively.
    #
    # Worker fibers only run tasks and report outcomes over a channel.
    # This coordinating fiber owns all bookkeeping (finished, failed,
    # errors, staleness updates, early cutoff), so none of it needs a
    # lock. Private for the same reason as _run_tasks.
    private def _run_tasks_parallel(
      task_names : Array(String),
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false,
      early_cutoff : Bool = true,
    )
      mark_stale_inputs(run_all, task_names)
      propagate_staleness(run_all)
      _tasks = task_names.map { |name| tasks[name] }
      finished_tasks = Set(Task).new
      failed_tasks = Set(Task).new
      errors = [] of Exception

      loop do
        # The task set can't change mid-run, so the candidate list
        # stays valid across waves
        batch = next_batch(_tasks, run_all, finished_tasks, failed_tasks, keep_going)
        if batch.nil?
          break
        end

        errors.concat(run_wave(batch, dry_run, early_cutoff, finished_tasks, failed_tasks))

        # Without keep_going a failure ends the run right away,
        # reporting the failure itself
        raise RunFailure.new(errors) unless errors.empty? || keep_going
      end
      finish_run(task_names, finished_tasks - failed_tasks, errors, keep_going, dry_run)
    end

    # The next batch of runnable tasks, or nil when the run is over:
    # every stale task finished, or (with keep_going) everything left
    # is blocked behind a failure. Raises when tasks remain but none
    # can run and failures are not being absorbed.
    private def next_batch(
      candidates : Array(Task),
      run_all : Bool,
      finished_tasks : Set(Task),
      failed_tasks : Set(Task),
      keep_going : Bool,
    ) : Array(Task)?
      done = finished_tasks | failed_tasks
      stale_tasks = candidates.reject { |task| done.includes?(task) }
      stale_tasks.select!(&.stale?) unless run_all
      return if stale_tasks.empty?

      # A task with several outputs appears once per output; run it
      # once
      batch = stale_tasks.select(&.ready?(run_all)).uniq!.shuffle
      return batch unless batch.empty?

      if keep_going
        # Everything left is blocked behind a failure (failed tasks
        # stay stale, so their dependents never become ready)
        Log.warn { "No runnable tasks left: #{stale_tasks.map(&.waiting_for).uniq!.join(", ")}" }
        return
      end
      raise UnknownInputsError.new("Can't run tasks: Waiting for #{stale_tasks.map(&.waiting_for).uniq!.join(", ")}")
    end

    # One parallel wave: run `batch` on a worker pool and collect every
    # outcome before returning. Workers touch no bookkeeping: task
    # staleness is an atomic field, and the TaskManager data they write
    # goes through locked accessors (@store_lock, @modified_lock,
    # @hashes_lock).
    private def run_wave(
      batch : Array(Task),
      dry_run : Bool,
      early_cutoff : Bool,
      finished_tasks : Set(Task),
      failed_tasks : Set(Task),
    ) : Array(Exception)
      errors = [] of Exception
      # At most one worker per CPU: each fiber is a stack the GC must
      # scan, so thousands of fibers are counterproductive
      num_workers = Math.min(System.cpu_count, batch.size)
      enable_parallelism(num_workers)
      task_queue = Channel(Task).new(batch.size)
      results = Channel({Task, Exception?}).new(batch.size)

      batch.each { |task| task_queue.send(task) }
      # Closed so workers exit once it's drained
      task_queue.close

      Log.debug { "Starting work-stealing execution of #{batch.size} tasks with #{num_workers} workers" }

      @data_mutex.synchronize { @parallel_wave_active = true }
      begin
        num_workers.times do |worker_index|
          # Named so specs can tell croupier's fibers from runtime
          # ones (GC markers, scheduler loops)
          spawn(name: "croupier-worker-#{worker_index}") do
            loop do
              task = task_queue.receive?
              break unless task # Queue is empty, exit worker

              error : Exception? = nil
              begin
                task.run unless dry_run
              rescue ex
                error = ex
              end
              results.send({task, error})
            end
          end
        end

        # Collect every outcome: this loop is the wave barrier
        batch.size.times do
          task, error = results.receive
          if failure = error
            failed_tasks << task
            # Keep the exception itself so RunFailure#errors has every
            # cause
            errors << failure
            Log.error { "Task #{task.outputs} failed: #{failure.message}" }
          end
          # Only successful tasks turn fresh: a failed one stays stale
          # so its dependents wait instead of running against a missing
          # or half-written output
          task.stale = !error.nil?
          finished_tasks << task

          if error.nil? && early_cutoff && !task.outputs_changed?
            notify_dependents_unchanged(task)
          end
        end
      ensure
        # Workers are done, so queued add_input calls can be applied
        # here. Clearing the flag, applying the queue and invalidating
        # happen under one lock, so an add_input arriving from another
        # thread can't be applied ahead of the queued ones.
        @data_mutex.synchronize do
          @parallel_wave_active = false
          pending = @pending_inputs
          @pending_inputs = [] of {String, String}
          pending.each do |task_key, input|
            if task = tasks[task_key]?
              # Set#<< ignores duplicates queued during the wave
              task.inputs << input
            end
          end
          # Skip invalidating when nothing was queued, to avoid a
          # pointless graph rebuild
          invalidate_graph_cache unless pending.empty?
        end
      end
      errors
    end

    # Early cutoff: a task's outputs didn't change, so let its stale
    # dependents recompute their staleness (they may be skipped).
    # Dependents come from the reverse-deps map built by
    # propagate_staleness.
    private def notify_dependents_unchanged(task : Task)
      notified = false
      task.outputs.each do |output|
        @reverse_deps.fetch(output, nil).try &.each do |dependent_key|
          if other_task = tasks[dependent_key]?
            next unless other_task.stale?
            unless notified
              Log.debug { "Early cutoff: #{task.id} outputs unchanged, notifying dependents" }
              notified = true
            end
            Log.debug { "Notifying #{other_task.id} that #{output} is unchanged" }
            other_task.recompute_staleness
          end
        end
      end
    end

    # Revert the scanned input hashes of tasks that didn't complete
    # this run (failed, or blocked behind a failure) to the previous
    # run's values, so the next run still sees their inputs as changed
    # and retries them. Inputs never recorded before are dropped, so
    # they read as modified. Fresh tasks that didn't run revert to
    # identical values and stay fresh. Tasks sharing a reverted input
    # may re-run too: extra work, but safe.
    private def drop_unfinished_inputs(task_names, succeeded : Set(Task))
      task_names.each do |name|
        next unless task = tasks[name]?
        next if succeeded.includes?(task)
        task.@inputs.each do |input|
          if previous = last_run[input]?
            this_run[input] = previous
          else
            this_run.delete(input)
          end
        end
      end
    end
  end
end
