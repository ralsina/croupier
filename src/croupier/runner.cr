module Croupier
  # TaskManagerType methods for running tasks, serially and in parallel.
  class TaskManagerType
    # Run all stale tasks in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    # If `early_cutoff` is true, skip tasks when upstream outputs are unchanged
    def run_tasks(
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false,
      early_cutoff : Bool? = nil,
    )
      _, tasks = sorted_task_graph
      # Input checking happens in the targeted overload this delegates
      # to, scoped to the full closure
      # Use TaskManager.early_cutoff if not explicitly specified
      early_cutoff = @early_cutoff if early_cutoff.nil?
      run_tasks(tasks, run_all, dry_run, parallel, keep_going, early_cutoff)
    end

    # Run the tasks needed to create or update the requested targets
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    # If `early_cutoff` is true, skip tasks when upstream outputs are unchanged
    def run_tasks(
      targets : Array(String),
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false,
      early_cutoff : Bool? = nil,
    )
      # Optimization: if targets already contains all tasks in sorted-graph
      # order, skip the expensive dependencies() call. The element-wise
      # comparison matters: matching only sizes would let an unsorted list
      # of the same length (e.g. tasks.keys, which is registration order)
      # bypass the topological sort, and unknown targets would be silently
      # dropped instead of raising.
      if targets == sorted_task_graph[1]
        Log.debug { "Skipping dependencies() call, targets already contain all #{targets.size} tasks" }
        task_names = targets
      else
        task_names = dependencies(targets)
      end

      # Outside auto mode, a missing input must surface as "Unknown
      # inputs" up front rather than as a "Waiting for" failure
      # mid-run. Auto mode skips the check on purpose: inputs that
      # don't exist yet are normal there (created later by procs or
      # the user), and runs must build whatever is buildable — the
      # retry backoff keeps the failed attempts cheap.
      check_dependencies(targets) unless @auto_mode

      # Use TaskManager.early_cutoff if not explicitly specified
      early_cutoff = @early_cutoff if early_cutoff.nil?

      if parallel
        _run_tasks_parallel(task_names, run_all, dry_run, keep_going, early_cutoff)
      else
        _run_tasks(task_names, run_all, dry_run, keep_going, early_cutoff)
      end
    end

    # Internal helper to run tasks serially
    def _run_tasks(
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

      # Single pass: no intermediate name→task arrays, and staleness is
      # decided at visit time so tasks marked fresh by early cutoff are
      # skipped. Like the parallel runner, run_all re-runs fresh tasks.
      # (Supersedes the 4793a67 re-check patch: the visit-time check
      # t.stale? || run_all honors run_all the same way, and always_run
      # needs no explicit check because propagate_staleness marks those
      # tasks stale and early cutoff cannot freshen them.)
      task_names.each do |name|
        next unless task = tasks.fetch(name, nil)
        next if finished.includes?(task)
        next unless task.stale? || run_all
        Log.debug { "Running task for #{task.outputs}" }
        unless task.waiting_for.empty? || dry_run
          if keep_going
            # Blocked behind a failure: skip it and keep going with
            # the rest, like the parallel runner does
            Log.warn { "Skipping task for #{task.outputs}: Waiting for #{task.waiting_for}" }
            next
          end
          raise "Can't run task for #{task.outputs}: Waiting for #{task.waiting_for}"
        end
        failed = false
        begin
          task.run unless dry_run
          succeeded << task unless dry_run
        rescue ex
          failed = true
          failures << ex
          Log.error { "Error running task for #{task.outputs}: #{ex}" }
          raise ex unless keep_going
        end
        finished << task

        # Early cutoff: if a SUCCESSFUL task's outputs didn't change,
        # notify dependent tasks (a failed task's outputs didn't
        # change either, but its dependents must stay blocked)
        if !failed && early_cutoff && !task.outputs_changed?
          notify_dependents_unchanged(task)
        end
      end

      # A dry run reports what would happen without doing it: persisting
      # the scanned input hashes would consume the changes so the next
      # real run would find nothing to do.
      return if dry_run
      drop_unfinished_inputs(task_names, succeeded)
      save_run
      # keep_going collected the failures instead of aborting; now that
      # the run finished and its state is saved, report them so callers
      # can tell the run failed (e.g. set an exit code)
      raise RunFailure.new(failures) if keep_going && !failures.empty?
    end

    # Internal helper to run tasks concurrently.
    #
    # Whenever a task is ready, launch it in a separate fiber. On
    # Crystal >= 1.18 the default execution context is resized to the
    # worker count, so ready tasks run with real multi-core parallelism;
    # on older Crystal this degrades to cooperative concurrency.
    #
    # Worker fibers only execute tasks and report each outcome over the
    # results channel; this coordinating fiber owns all shared
    # bookkeeping (finished / failed / error collections, stale
    # transitions, early-cutoff notifications), so none of it needs a
    # lock. Receiving batch.size results is the wave barrier.
    def _run_tasks_parallel(
      task_names : Array(String) = [] of String,
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false,
      early_cutoff : Bool = true,
    )
      task_names = tasks.keys if task_names.empty?
      mark_stale_inputs(run_all, task_names)
      propagate_staleness(run_all)
      _tasks = task_names.map { |name| tasks[name] }
      finished_tasks = Set(Task).new
      failed_tasks = Set(Task).new
      errors = [] of Exception

      loop do
        if run_all
          stale_tasks = _tasks.reject { |t|
            finished_tasks.includes?(t) || failed_tasks.includes?(t)
          }
        else
          stale_tasks = _tasks.select(&.stale?).reject { |t|
            finished_tasks.includes?(t) || failed_tasks.includes?(t)
          }
        end

        break if stale_tasks.empty?

        # The uniq is because a task may be repeated in the
        # task graph because of multiple outputs. We don't
        # want to run it twice.
        batch = stale_tasks.select(&.ready?(run_all)).uniq!.shuffle

        if batch.size == 0
          if keep_going
            # Everything left is blocked behind a failure (failed
            # tasks stay stale, so their dependents never become
            # ready): nothing more this run can do
            Log.warn { "No runnable tasks left: #{stale_tasks.map(&.waiting_for).uniq!.join(", ")}" }
            break
          end
          # No tasks are ready
          raise "Can't run tasks: Waiting for #{stale_tasks.map(&.waiting_for).uniq!.join(", ")}"
        end

        errors.concat(run_wave(batch, dry_run, early_cutoff, finished_tasks, failed_tasks))

        # Without keep_going a failure ends the run right away, with
        # the failure itself — not a "waiting for" message about the
        # dependents now blocked behind it
        raise RunFailure.new(errors) unless errors.empty? || keep_going
      end
      # See _run_tasks: a dry run must not consume the input changes
      return if dry_run
      drop_unfinished_inputs(task_names, finished_tasks - failed_tasks)
      save_run
      # keep_going collected the failures instead of aborting; report
      # them once the run finished and its state is saved
      raise RunFailure.new(errors) if keep_going && !errors.empty?
    end

    # One parallel wave: run `batch` on a small worker pool and collect
    # outcomes at the wave barrier (this coordinating fiber is the only
    # writer of the bookkeeping state, so none of it needs a lock).
    # Worker fibers touch no shared bookkeeping: task staleness is a
    # single atomic field, and the TaskManager data they write goes
    # through @data_mutex-guarded accessors.
    private def run_wave(
      batch : Array(Task),
      dry_run : Bool,
      early_cutoff : Bool,
      finished_tasks : Set(Task),
      failed_tasks : Set(Task),
    ) : Array(Exception)
      errors = [] of Exception
      # Keep a small worker pool (each fiber is a stack the GC must
      # scan, so thousands of fibers are counterproductive)
      num_workers = Math.min(System.cpu_count, batch.size)
      enable_parallelism(num_workers)
      task_queue = Channel(Task).new(batch.size)
      results = Channel({Task, Exception?}).new(batch.size)

      # Add all tasks to the shared queue
      batch.each { |task| task_queue.send(task) }
      # Close the queue so workers exit (receive? returns nil) instead
      # of parking forever on the drained channel
      task_queue.close

      Log.debug { "Starting work-stealing execution of #{batch.size} tasks with #{num_workers} workers" }

      @data_mutex.synchronize { @parallel_wave_active = true }
      begin
        num_workers.times do
          spawn do
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

        # Collect every outcome. This loop is the wave barrier and the
        # only writer of the bookkeeping state.
        batch.size.times do
          task, error = results.receive
          if failure = error
            failed_tasks << task
            # Keep the exception itself (not just its message) so the
            # RunFailure raised at the end of a keep_going run can
            # chain every cause
            errors << failure
            Log.error { "Task #{task.outputs} failed: #{failure.message}" }
          end
          # Task is done, do not run again. Only successful tasks
          # turn fresh: a failed one stays stale so its dependents
          # keep waiting for it instead of running against a missing
          # or half-written output
          task.stale = !error.nil?
          finished_tasks << task

          # Early cutoff: if outputs didn't change, notify dependent tasks
          if error.nil? && early_cutoff && !task.outputs_changed?
            notify_dependents_unchanged(task)
          end
        end
      ensure
        # Workers are done: queued add_input calls can be applied on
        # this fiber, where nothing iterates the input sets concurrently
        @data_mutex.synchronize { @parallel_wave_active = false }
        apply_pending_inputs
      end
      errors
    end

    # Early cutoff: if a task's outputs didn't change, notify dependent
    # tasks so they can be skipped. Dependents are looked up via the
    # cached reverse-deps map instead of scanning every task per output.
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
            other_task.mark_dependency_fresh(output)
          end
        end
      end
    end

    # Revert the scanned input hashes of tasks that did not complete
    # this run (failed, or blocked behind a failure) to what the
    # previous run recorded: keeping their new hashes would tell the
    # next run nothing changed, so they would never retry. Unchanged
    # inputs of fresh-but-not-run tasks revert to identical values, so
    # this never spuriously re-stales them; an input never recorded
    # before is dropped so it reads as modified next time. Inputs
    # shared with successfully-completed tasks may re-run those too:
    # safe, just extra work.
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
