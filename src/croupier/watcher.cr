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
            retry_delay = autorun_cycle(targets, retry_delay)
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
    # Returns the delay before the next cycle.
    private def autorun_cycle(targets : Array(String), retry_delay : Float64) : Float64
      # Sleep first: sleeping at the end would make it likely that a
      # stop order arrives before the run, so tests couldn't observe
      # its side effects
      sleep retry_delay.seconds
      changes = queued_changes_snapshot
      # set() may mark kv keys modified from other fibers
      modified_pending = @modified_lock.synchronize { !@modified.empty? }
      return retry_delay if changes.empty? && !modified_pending
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
        AUTORUN_RETRY_MIN_DELAY
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
        delay
      end
    end

    {% if flag?(:linux) %}
      private alias FileSystemWatcher = LinuxWatcher
    {% elsif flag?(:darwin) %}
      private alias FileSystemWatcher = KqueueWatcher
    {% end %}

    {% if flag?(:linux) || flag?(:darwin) %}
      # The platform filesystem watcher (LinuxWatcher or KqueueWatcher)
      @@watcher : FileSystemWatcher | Nil = nil

      private def close_watcher : Nil
        @@watcher_lock.synchronize do
          @@watcher.try(&.close)
          @@watcher = nil
        end
      end

      # Watch the inputs of `targets` (all tasks by default) and queue
      # changed paths in @queued_changes. Changes made before this call
      # are not detected.
      def watch(targets : Array(String) = [] of String) : Nil
        targets = tasks.keys if targets.empty?
        watcher, target_inputs = @@watcher_lock.synchronize do
          # Events in the close/re-watch window are lost; the next
          # cycle's input scan catches what they changed
          @@watcher.try(&.close)
          new_watcher = FileSystemWatcher.new(->(input : String) {
            queue_change(input)
            Log.debug { "Detected change in #{input}" }
          })
          @@watcher = new_watcher
          {new_watcher, inputs(targets)}
        end

        target_inputs.each do |input|
          # k/v keys are not files
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
