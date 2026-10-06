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
          schedule(task_names, run_all, dry_run, parallel, keep_going, early_cutoff)
        end
      ensure
        @data_mutex.synchronize do
          @run_active -= 1
          apply_pending_inputs_locked if @run_active == 0
        end
      end
    end

    # Run the stale tasks among `task_names` (a dependency order) as
    # their producers finish; RunPlan decides what can start. Private:
    # the run_active guard on task creation and removal holds only if
    # run_tasks is the only way into a run.
    #
    # Serial runs execute tasks inline on this fiber. Parallel runs
    # keep up to one task per CPU on a WorkerPool and start the next
    # as soon as any one finishes. Either way this fiber owns all
    # bookkeeping (staleness, early cutoff, failures), so none of it
    # needs a lock.
    private def schedule(
      task_names : Array(String),
      run_all : Bool,
      dry_run : Bool,
      parallel : Bool,
      keep_going : Bool,
      early_cutoff : Bool,
    ) : Nil
      mark_stale_inputs(run_all, task_names)
      propagate_staleness(run_all)
      plan = RunPlan.new(task_names, tasks, run_all, dry_run)
      succeeded = Set(Task).new
      failures = [] of Exception
      slots = parallel ? Math.min(System.cpu_count, plan.size) : 1
      pool = nil
      completed = [] of {Task, Exception?}
      in_flight = 0
      stopping = false

      begin
        loop do
          while in_flight < slots && !stopping && (task = plan.next_task)
            in_flight += 1
            Log.debug { "Running task for #{task.outputs}" }
            if parallel
              pool ||= WorkerPool(Task, Nil).new("croupier-worker", slots, slots) do |queued|
                queued.run unless dry_run
              end
              pool.submit(task)
            else
              completed << {task, run_inline(task, dry_run)}
            end
          end
          break if in_flight == 0

          task, result = pool ? pool.receive : completed.pop
          in_flight -= 1
          error = result.as?(Exception)
          stopping = true if error && !keep_going
          record_result(task, error, succeeded, failures, dry_run, early_cutoff)
          plan.finished(task, error.nil?)
        end
      ensure
        pool.try &.close
      end

      # Without keep_going a failure ends the run once the tasks
      # already started have finished. It reports the failure itself
      # rather than the dependents blocked behind it; state is not
      # saved.
      raise RunFailure.new(failures) if stopping
      report_left_over(plan.left_over, keep_going)
      finish_run(task_names, succeeded, failures, keep_going, dry_run)
    end

    # Bookkeeping for one finished task. A failed task stays stale, so
    # its consumers never run against a missing or half-written
    # output. Early cutoff: a successful task whose outputs didn't
    # change lets its consumers turn fresh (and be skipped).
    private def record_result(
      task : Task,
      error : Exception?,
      succeeded : Set(Task),
      failures : Array(Exception),
      dry_run : Bool,
      early_cutoff : Bool,
    ) : Nil
      if error
        failures << error
        Log.error { "Task #{task.outputs} failed: #{error.message}" }
        task.stale = true
      else
        succeeded << task unless dry_run
        task.stale = false
        notify_dependents_unchanged(task) if early_cutoff && !dry_run && !task.outputs_changed?
      end
    end

    # Tasks that never ran: an error, or a warning with keep_going
    # (they are blocked behind a failure, or an input never appeared)
    private def report_left_over(left : Array(Task), keep_going : Bool) : Nil
      return if left.empty?
      waiting = left.map(&.waiting_for).uniq!.join(", ")
      raise UnknownInputsError.new("Can't run tasks: Waiting for #{waiting}") unless keep_going
      Log.warn { "No runnable tasks left: #{waiting}" }
    end

    # Run `task` on this fiber, returning its failure instead of
    # raising it
    private def run_inline(task : Task, dry_run : Bool) : Exception?
      task.run unless dry_run
      nil
    rescue ex
      ex
    end

    # Epilogue of a run. A dry run saves nothing (saving the scanned
    # hashes would hide the changes from the next real run). A real
    # run saves its state, then raises the collected failures so
    # callers can tell the run failed.
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
