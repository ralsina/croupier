module Croupier
  # TaskManagerType methods for auto_run and the platform filesystem watcher.
  class TaskManagerType
    # Files with changes detected in auto_run
    @queued_changes : Set(String) = Set(String).new
    # Guards @queued_changes. The watcher callback and the autorun
    # fiber can run on different OS threads (input hashing resizes the
    # default execution context), even though auto mode runs tasks
    # serially.
    @queued_changes_lock = Sync::Mutex.new

    @autorun_control = Channel(Bool).new
    # Whether the autorun fiber is live. auto_stop checks it because
    # sending on the unbuffered control channel would block forever
    # with nothing receiving. Atomic: it is written by the autorun
    # fiber and read by whichever fiber calls auto_stop.
    @autorun_running = Atomic(Bool).new(false)

    # Autorun retry backoff bounds, in seconds: consecutive failures
    # slow the loop from the change-poll minimum to at most one
    # attempt per second; any success resets to the minimum.
    AUTORUN_RETRY_MIN_DELAY = 0.01
    AUTORUN_RETRY_MAX_DELAY =  1.0

    # Guards @@watcher: the autorun fiber may re-watch while cleanup's
    # close_watcher runs on another fiber, and an unguarded swap could
    # leak a watcher nobody closes.
    @@watcher_lock = Sync::Mutex.new

    # Serializes auto_stop callers through the whole shutdown
    # handshake. The first caller performs it; the others wait on the
    # mutex and then find the running flag down. Either way, when
    # auto_stop returns the autorun fiber has stopped.
    @@stop_mutex = Sync::Mutex.new

    def auto_stop
      @@stop_mutex.synchronize do
        return unless @autorun_running.get
        @autorun_control.send true
        @autorun_control.receive?
        @autorun_control = Channel(Bool).new
        @autorun_running.set(false)
      end
    end

    # Snapshot of the queued changes, safe to call from the watcher callback
    # or the autorun fiber.
    private def queued_changes_snapshot : Set(String)
      @queued_changes_lock.synchronize { @queued_changes.dup }
    end

    # Queue one changed path (called from the filesystem watcher).
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

    def auto_run(targets : Array(String) = [] of String)
      @auto_mode = true
      targets = tasks.keys if targets.empty?
      # Every input of the targets and their dependencies
      inputs = inputs(targets)
      Log.info { "Auto_run: targets=#{targets.inspect}, inputs=#{inputs.inspect}" }
      raise UsageError.new("No inputs to watch, can't auto_run") if inputs.empty?

      # Auto mode always runs tasks serially, on the autorun fiber
      Log.info { "Auto_run mode: forcing serial execution (parallel disabled)" }

      watch(targets)
      @autorun_running.set(true)
      retry_delay = AUTORUN_RETRY_MIN_DELAY
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

    # Handle the stop order (runs on the autorun fiber). Closing the
    # control channel is the acknowledgement auto_stop waits for, so it
    # happens last: otherwise auto_stop could return, and a new
    # auto_run install a watcher, while the old one is still closing.
    private def stop_autorun : Nil
      Log.info { "Stopping automatic run" }
      close_watcher
      @autorun_running.set(false)
      @autorun_control.close
    end

    # One iteration of the autorun loop: process queued changes, run
    # the tasks (again if a proc's add_input changed the graph), and
    # fold the cycle's hashes into last_run. Auto mode never reloads
    # the state file, so without the fold every cycle would compare
    # against the first one (no early cutoff, and unchanged rewrites
    # would keep re-staling dependents). this_run holds the scanned
    # input hashes, next_run the recorded output hashes.
    #
    # Returns the next retry delay and the (unchanged) targets.
    private def autorun_cycle(targets : Array(String), retry_delay : Float64) : {Float64, Array(String)}
      # Sleep first: sleeping at the end would make it likely that a
      # stop order arrives before the run, so tests couldn't observe
      # its side effects
      sleep retry_delay.seconds
      changes = queued_changes_snapshot
      # set() may mark kv keys modified from other fibers
      modified_pending = @modified_lock.synchronize { !@modified.empty? }
      return {retry_delay, targets} if changes.empty? && !modified_pending
      begin
        Log.info { "Detected changes in #{changes}" }
        # No need to mark targets stale: propagate_staleness resets
        # every task's staleness at the start of each run
        hook_changes = @modified_lock.synchronize do
          # Mutate in place: reassigning would leave readers of the
          # `modified` property holding a stale set
          changes.each { |change| @modified << change }
          @modified.dup
        end
        Log.debug { "Modified: #{hook_changes}" }
        # User code must not run under a library lock
        before_run_hook.call(hook_changes) unless hook_changes.empty?
        run_tasks(targets: targets, parallel: false)
        if @graph_invalidated
          # A proc added a dependency with add_input: watch the new
          # inputs and run again with the same targets
          watch(targets)
          run_tasks(targets: targets, parallel: false)
        end
        # Drop only what this cycle consumed. set() may re-mark a
        # kv:// key while the run executes, so a kv:// entry is
        # dropped only when the store's current value matches what
        # was recorded for it. Reading the store under this lock is
        # safe: set() never holds @store_lock while taking
        # @modified_lock.
        unqueue_changes(changes)
        @modified_lock.synchronize do
          consumed = @modified.select do |path|
            next false unless hook_changes.includes?(path)
            next true unless key = path.lchop?("kv://")
            value = get(key)
            Digest::SHA1.hexdigest(value || "") == last_run.fetch(path, "")
          end
          consumed.each { |path| @modified.delete(path) }
        end
        # Tasks run serially on this fiber, so it is the only writer
        # of the run hashes and the merge needs no lock
        last_run.merge!(this_run).merge!(next_run)
        {AUTORUN_RETRY_MIN_DELAY, targets}
      rescue ex
        # Every failure retries with backoff: stopping on the first
        # error would leave a long-lived watcher blind. Only the log
        # level differs.
        delay = Math.min(retry_delay * 2, AUTORUN_RETRY_MAX_DELAY)
        case ex
        when UnknownInputsError
          # Not all inputs exist yet: routine in auto mode, retry
          # quietly
        when RunFailure
          # A task failed (a half-edited source, a broken command)
          Log.warn { "Automatic run failed (will retry): #{ex.message}" }
        else
          # Anything else is a bug, in a before_run_hook or in
          # croupier (task failures arrive wrapped in RunFailure)
          Log.error { "Automatic run crashed (bug, will retry): #{ex.inspect_with_backtrace}" }
        end
        {delay, targets}
      end
    end

    {% if flag?(:linux) %}
      # Linux filesystem watcher
      @@watcher : Inotify::Watcher | Nil = nil

      private def close_watcher : Nil
        @@watcher_lock.synchronize do
          return unless watcher = @@watcher
          begin
            watcher.close
          rescue ex : Inotify::Error
            # Closing an already-closed inotify descriptor is harmless here.
          ensure
            @@watcher = nil
          end
        end
      end

      # Watch the inputs of `targets` (all tasks by default) and queue
      # changed paths in @queued_changes. Changes made before this call
      # are not detected.
      def watch(targets : Array(String) = [] of String)
        targets = tasks.keys if targets.empty?
        watcher, target_inputs = @@watcher_lock.synchronize do
          # Events in the close/re-watch window are lost; the next
          # cycle's input scan catches what they changed
          @@watcher.try(&.close)
          new_watcher = Inotify::Watcher.new(recursive: true)
          @@watcher = new_watcher
          {new_watcher, inputs(targets)}
        end

        # Directory prefixes ("dir/" matches everything under dir) are
        # computed once here, not per event
        prefix_inputs = target_inputs.map do |input|
          normalized = input.ends_with?("/") ? input : "#{input}/"
          {normalized, input}
        end

        watcher.on_event(&event_handler(watcher, target_inputs, prefix_inputs))
        watch_inputs(watcher, target_inputs)
      end

      # inotify flags shared by every watched path. IN_DELETE_SELF and
      # IN_MOVE_SELF are left out: they carry no input path to queue.
      private def watch_flags
        LibInotify::IN_DELETE |
          LibInotify::IN_CREATE |
          LibInotify::IN_MODIFY |
          LibInotify::IN_MOVED_TO |
          LibInotify::IN_CLOSE_WRITE |
          LibInotify::IN_ATTRIB
      end

      # Watch every input. An input that doesn't exist yet is covered
      # by watching its parent directory, so its creation is seen.
      private def watch_inputs(watcher : Inotify::Watcher, target_inputs : Set(String)) : Nil
        target_inputs.each do |input|
          # k/v keys are not files
          next if input.lchop?("kv://")
          if File.exists? input
            watcher.watch input, watch_flags
            Log.info { "Watching: #{input}" }
          else
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
          # Path of the changed file. Without a name, fall back to the
          # bare path ("" if absent), which matches no input.
          path = if event.path && event.name
                   Path["#{event.path}/#{event.name}"].normalize.to_s
                 else
                   event.path || ""
                 end

          Log.debug do
            "inotify event: path=#{event.path.inspect}, name=#{event.name.inspect}, " \
            "mask=#{event.mask.inspect}, constructed=#{path.inspect}, " \
            "target_inputs=#{target_inputs.inspect}"
          end

          # The watch was removed (an editor deleted or replaced the
          # file): watch it again, or its parent if it's gone
          if event.type_is?(LibInotify::IN_IGNORED)
            if ep = event.path
              if target_inputs.includes?(ep)
                if File.exists?(ep)
                  watcher.watch ep, watch_flags
                  Log.debug { "Re-watched file after editor replacement: #{ep}" }
                else
                  parent = Path[ep].parent.to_s
                  unless watcher.watching.includes?(parent)
                    watcher.watch parent, watch_flags
                  end
                end
              end
            end
          end

          matched = false
          if target_inputs.includes? path
            queue_change(path)
            Log.debug { "Detected change in #{path} (exact match)" }
            matched = true
          else
            prefix_inputs.each do |normalized, input|
              # A change inside a watched directory queues the directory
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
    {% elsif flag?(:darwin) %}
      # macOS filesystem watcher. Same API and queued paths as Linux;
      # only the kernel event backend differs.
      @@watcher : KqueueWatcher | Nil = nil

      private def close_watcher : Nil
        @@watcher_lock.synchronize do
          if watcher = @@watcher
            watcher.close
            @@watcher = nil
          end
        end
      end

      def watch(targets : Array(String) = [] of String) : Nil
        targets = tasks.keys if targets.empty?
        watcher, target_inputs = @@watcher_lock.synchronize do
          # Events in the close/re-watch window are lost; the next
          # cycle's input scan catches what they changed
          if old_watcher = @@watcher
            old_watcher.close
            @@watcher = nil
          end
          new_watcher = KqueueWatcher.new(->(input : String) {
            queue_change(input)
            Log.debug { "Detected change in #{input}" }
          })
          @@watcher = new_watcher
          {new_watcher, inputs(targets)}
        end

        target_inputs.each do |input|
          next if input.lchop?("kv://")
          watcher.watch(input)
          Log.info { "Watching: #{input}" }
        end
      end
    {% else %}
      private def close_watcher : Nil
      end

      def watch(targets : Array(String) = [] of String) : Nil
        raise UsageError.new("auto_run is supported only on Linux and macOS")
      end
    {% end %}
  end
end
