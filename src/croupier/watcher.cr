module Croupier
  # TaskManagerType methods for auto_run and the inotify watcher.
  class TaskManagerType
    # Files with changes detected in auto_run
    @queued_changes : Set(String) = Set(String).new
    # Guards @queued_changes. The inotify callback runs on the library's
    # own fiber, and hashing inputs resizes the default execution
    # context to several OS threads, so the callback fiber and the
    # autorun fiber can genuinely run in parallel even though auto mode
    # executes tasks serially. A Set is not thread-safe: every access
    # from either fiber goes through this lock.
    @queued_changes_lock = Sync::Mutex.new

    @autorun_control = Channel(Bool).new
    # Whether the autorun fiber is live: auto_stop's send would block
    # forever on the unbuffered control channel if nothing is running
    # (e.g. cleanup without auto_run)
    @autorun_running = false

    def auto_stop
      return unless @autorun_running
      @autorun_control.send true
      @autorun_control.receive?
      @autorun_control = Channel(Bool).new
      @autorun_running = false
    end

    # Snapshot of the queued changes, safe to call from the inotify
    # callback fiber or the autorun fiber.
    private def queued_changes_snapshot : Set(String)
      @queued_changes_lock.synchronize { @queued_changes.dup }
    end

    # Queue one changed path (called from the inotify callback fiber).
    private def queue_change(path : String) : Nil
      @queued_changes_lock.synchronize { @queued_changes << path }
    end

    # Remove the paths processed this cycle from the queue, so events
    # that arrived while the run executed stay queued for the next one.
    private def unqueue_changes(paths : Set(String)) : Nil
      @queued_changes_lock.synchronize { paths.each { |path| @queued_changes.delete(path) } }
    end

    private def clear_queued_changes : Nil
      @queued_changes_lock.synchronize { @queued_changes.clear }
    end

    {% if flag?(:linux) %}
      def auto_run(targets : Array(String) = [] of String)
        @auto_mode = true
        targets = tasks.keys if targets.empty?
        # Only want dependencies that are not tasks
        inputs = inputs(targets)
        Log.info { "Auto_run: targets=#{targets.inspect}, inputs=#{inputs.inspect}" }
        raise "No inputs to watch, can't auto_run" if inputs.empty?

        # Auto_run always runs serially to avoid inotify thread safety issues
        # File watching and parallel execution don't mix well due to
        # shared state and nonblocking inotify library limitations
        Log.info { "Auto_run mode: forcing serial execution (parallel disabled)" }

        watch(targets)
        @autorun_running = true
        # Retry backoff: consecutive failures slow the loop down from
        # the 10ms change-poll to at most one attempt per second, so a
        # persistent problem (e.g. a deleted input) can't spin the CPU
        # and the log; any success resets it
        retry_delay = 0.01
        spawn do
          loop do
            select
            when @autorun_control.receive
              stop_autorun
              break
            else
              retry_delay, targets = autorun_cycle(targets, retry_delay)
            end
          end
        end
      end

      # Handle the stop order (runs on the autorun fiber): close the
      # control channel so the stopping fiber's receive? returns, and
      # shut the watcher down.
      private def stop_autorun : Nil
        Log.info { "Stopping automatic run" }
        @autorun_control.close
        @autorun_running = false
        if watcher = @@watcher
          watcher.close # Stop watchers
        end
      end

      # One iteration of the autorun loop: process queued changes,
      # re-run tasks, expand the graph if master tasks added subtasks,
      # and fold the cycle's hashes into last_run. The non-auto path
      # reloads those hashes from the state file every run, but the
      # auto branch never refreshes last_run, so without the fold every
      # cycle looks like the first one (no early cutoff, and unchanged
      # rewrites keep re-staling their dependents). this_run holds the
      # scanned input hashes, next_run the recorded output hashes.
      #
      # Returns the updated retry delay and target list (the targets
      # may grow when the graph grew).
      private def autorun_cycle(targets : Array(String), retry_delay : Float64) : {Float64, Array(String)}
        # Sleep early is better for race conditions in tests
        # If we sleep late, it's likely that we'll get the
        # stop order and break the loop without running, so we
        # can't see the side effects without sleeping in the
        # tests.
        sleep retry_delay.seconds
        changes = queued_changes_snapshot
        return {retry_delay, targets} if changes.empty? && @modified.empty?
        begin
          Log.info { "Detected changes in #{changes}" }
          # No need to mark targets stale here: propagate_staleness,
          # called at the start of every run, resets every task's
          # staleness from scratch.
          @modified += changes
          Log.debug { "Modified: #{@modified}" }
          # Call the before_run_hook if set, passing the changed files
          before_run_hook.call(@modified.dup) unless @modified.empty?
          # Run tasks - if master tasks create new subtasks, the graph
          # will be invalidated and we need to run again to execute them
          initial_task_count = tasks.size
          run_tasks(targets: targets, parallel: false)
          # If new tasks were created (graph was invalidated), run
          # again with the expanded graph
          if @graph_invalidated || tasks.size > initial_task_count
            targets = tasks.keys
            # And re-watch: the new subtasks' inputs were not
            # known when watch() was last called, so changes to
            # them would be invisible to the watcher
            watch(targets)
            run_tasks(targets: targets, parallel: false)
          end
          # Drop only the changes processed this cycle, then the
          # modified set: a successful run consumed them
          unqueue_changes(changes)
          @modified.clear
          # In auto mode this fiber is the only writer of the run-hash
          # trio (tasks run serially on it), so the merge needs no lock
          last_run.merge!(this_run).merge!(next_run)
          {0.01, targets}
        rescue ex
          # Sometimes we can't run because not all dependencies
          # are there yet or whatever. We'll try again later
          delay = Math.min(retry_delay * 2, 1.0)
          unless ex.is_a?(UnknownInputsError)
            Log.warn { "Automatic run failed (will retry): #{ex.message}" }
          end
          {delay, targets}
        end
      end

      # Filesystem watcher
      @@watcher : Inotify::Watcher | Nil = nil

      # Watch for changes in inputs.
      # If an input has been changed BEFORE calling this method,
      # it will NOT be detected as a change.
      #
      # Changes are added to queued_changes

      def watch(targets : Array(String) = [] of String)
        if current_watcher = @@watcher
          current_watcher.close
        end

        @@watcher = Inotify::Watcher.new(recursive: true)
        targets = tasks.keys if targets.empty?
        target_inputs = inputs(targets)

        return unless watcher = @@watcher

        # Prefix matching runs on every filesystem event, so the
        # normalized forms (trailing slash, "dir/" matches everything
        # under dir) are computed once here instead of allocating a
        # string per watched input per event. Exact matches need no
        # preprocessing: target_inputs is a Set, already O(1).
        prefix_inputs = target_inputs.map do |input|
          normalized = input.ends_with?("/") ? input : "#{input}/"
          {normalized, input}
        end

        watcher.on_event(&event_handler(watcher, target_inputs, prefix_inputs))
        watch_inputs(watcher, target_inputs)
      end

      # inotify flags shared by every watched path.
      #
      # NOT watching IN_DELETE_SELF, IN_MOVE_SELF because
      # when those are triggered we have no input file to
      # process.
      private def watch_flags
        LibInotify::IN_DELETE |
          LibInotify::IN_CREATE |
          LibInotify::IN_MODIFY |
          LibInotify::IN_MOVED_TO |
          LibInotify::IN_CLOSE_WRITE |
          LibInotify::IN_ATTRIB
      end

      # Attach the event handler, then watch every input; an input
      # that doesn't exist yet is covered by watching its parent
      # directory, so its creation is seen.
      private def watch_inputs(watcher : Inotify::Watcher, target_inputs : Set(String)) : Nil
        target_inputs.each do |input|
          # Don't watch for changes in k/v store
          next if input.lchop?("kv://")
          if File.exists? input
            watcher.watch input, watch_flags
            Log.info { "Watching: #{input}" }
          else
            # It's a file that doesn't exist. To detect it
            # being created, we watch the parent directory
            # if we are not already watching it.
            path = (Path[input].parent).to_s
            if !watcher.watching.includes?(path)
              watcher.watch path, watch_flags
              Log.info { "Watching parent: #{path}" }
            end
          end
        end

        Log.info { "Watching: #{watcher.watching.inspect}" }
      end

      # The inotify event handler: re-watch files replaced by editors
      # (IN_IGNORED), and queue changes matching a watched input,
      # exactly or by directory prefix.
      private def event_handler(
        watcher : Inotify::Watcher,
        target_inputs : Set(String),
        prefix_inputs : Array({String, String}),
      ) : Proc(Inotify::Event, Nil)
        ->(event : Inotify::Event) do
          # Path of the changed file; when the event carries no name
          # there is nothing to match, so fall back to the bare path
          # ("" if absent) which matches no input below
          path = if event.path && event.name
                   Path["#{event.path}/#{event.name}"].normalize.to_s
                 else
                   event.path || ""
                 end

          # Debug logging
          Log.debug do
            "inotify event: path=#{event.path.inspect}, name=#{event.name.inspect}, " \
            "mask=#{event.mask.inspect}, constructed=#{path.inspect}, " \
            "target_inputs=#{target_inputs.inspect}"
          end

          # If watch was removed (e.g., editor deleted/replaced the file), re-add it
          if event.type_is?(LibInotify::IN_IGNORED)
            if ep = event.path
              if target_inputs.includes?(ep)
                if File.exists?(ep)
                  watcher.watch ep, watch_flags
                  Log.debug { "Re-watched file after editor replacement: #{ep}" }
                else
                  # File doesn't exist, watch parent directory for creation
                  parent = Path[ep].parent.to_s
                  unless watcher.watching.includes?(parent)
                    watcher.watch parent, watch_flags
                  end
                end
              end
            end
          end

          # If path matches a watched path, add it to the queue
          matched = false
          if target_inputs.includes? path
            queue_change(path)
            Log.debug { "Detected change in #{path} (exact match)" }
            matched = true
          else
            prefix_inputs.each do |normalized, input|
              # If we are watching a folder in path, add the folder to
              # the queue. A path equal to an input was already caught
              # by the exact match above.
              if path.starts_with?(normalized)
                queue_change(input)
                Log.debug { "Detected change in #{input} (prefix match: #{path} starts with #{normalized})" }
                matched = true
                break
              end
            end
          end

          Log.debug { "Event NOT matched for path=#{path}, target_inputs=#{target_inputs.inspect}" } unless matched
        end
      end
    {% end %}
  end
end
