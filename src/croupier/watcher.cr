module Croupier
  # TaskManagerType methods for auto_run and the inotify watcher.
  class TaskManagerType
    # Files with changes detected in auto_run
    @queued_changes : Set(String) = Set(String).new

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
              Log.info { "Stopping automatic run" }
              @autorun_control.close
              @autorun_running = false
              if watcher = @@watcher
                watcher.close # Stop watchers
              end
              break
            else
              begin
                # Sleep early is better for race conditions in tests
                # If we sleep late, it's likely that we'll get the
                # stop order and break the loop without running, so we
                # can't see the side effects without sleeping in the
                # tests.
                sleep retry_delay.seconds
                next if @queued_changes.empty? && @modified.empty?
                Log.info { "Detected changes in #{@queued_changes}" }
                # No need to mark targets stale here: propagate_staleness,
                # called at the start of every run, resets every task's
                # staleness from scratch.
                @modified += @queued_changes
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
                # Only clean queued changes after a successful run
                @modified.clear
                @queued_changes.clear
                # Fold this cycle's hashes into @last_run: the non-auto
                # path reloads them from the state file every run, but
                # the auto branch never refreshes @last_run, so without
                # this every cycle looks like the first one (no early
                # cutoff, and unchanged rewrites keep re-staling their
                # dependents). this_run holds the scanned input hashes,
                # next_run the recorded output hashes
                @data_mutex.synchronize { last_run.merge!(this_run).merge!(next_run) }
                retry_delay = 0.01
              rescue ex
                # Sometimes we can't run because not all dependencies
                # are there yet or whatever. We'll try again later
                retry_delay = Math.min(retry_delay * 2, 1.0)
                unless ex.message.to_s.starts_with?("Can't run: Unknown inputs")
                  Log.warn { "Automatic run failed (will retry): #{ex.message}" }
                end
              end
            end
          end
        end
      end
    {% else %}
      # Non-Linux stub for auto_run
      def auto_run(targets : Array(String) = [] of String)
        raise "auto_run is only supported on Linux. File watching requires inotify, which is Linux-specific."
      end
    {% end %}

    {% if flag?(:linux) %}
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

        # Define watch flags before event handler so it's accessible in the closure
        watch_flags = LibInotify::IN_DELETE |
                      LibInotify::IN_CREATE |
                      LibInotify::IN_MODIFY |
                      LibInotify::IN_MOVED_TO |
                      LibInotify::IN_CLOSE_WRITE |
                      LibInotify::IN_ATTRIB
        # NOT watching IN_DELETE_SELF, IN_MOVE_SELF because
        # when those are triggered we have no input file to
        # process.

        event_handler = ->(event : Inotify::Event) do
          # It's a file we care about, add it to the queue
          path = event.path && event.name ? Path["#{event.path}/#{event.name}"].normalize.to_s : (event.path || "nil")

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
            @queued_changes << path
            Log.debug { "Detected change in #{path} (exact match)" }
            matched = true
          else
            prefix_inputs.each do |normalized, input|
              # If we are watching a folder in path, add the folder to
              # the queue. A path equal to an input was already caught
              # by the exact match above.
              if path.starts_with?(normalized)
                @queued_changes << input
                Log.debug { "Detected change in #{input} (prefix match: #{path} starts with #{normalized})" }
                matched = true
                break
              end
            end
          end

          Log.debug { "Event NOT matched for path=#{path}, target_inputs=#{target_inputs.inspect}" } unless matched
        end
        watcher.on_event(&event_handler)

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
    {% end %}
  end
end
