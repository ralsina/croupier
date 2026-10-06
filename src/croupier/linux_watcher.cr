{% if flag?(:linux) %}
  module Croupier
    # Recursive filesystem watcher backed by Linux inotify.
    #
    # Same shape as KqueueWatcher: construct with a change callback,
    # `watch` each task input, `close` when done. The class owns the
    # inotify specifics — event-to-input matching (exact, or directory
    # prefix), re-watching files replaced by editors (IN_IGNORED), and
    # covering missing inputs through their parent directory.
    class LinuxWatcher
      @lock = Sync::Mutex.new
      @inotify : Inotify::Watcher
      # Every watched task input, and the "dir/" prefixes used to
      # match events on files inside a watched directory
      @target_inputs = Set(String).new
      @prefix_inputs = [] of {String, String}
      @closed = false

      def initialize(@on_event : Proc(String, Nil))
        @inotify = Inotify::Watcher.new(recursive: true)
        @inotify.on_event(&event_handler)
      end

      def watch(path : String) : Nil
        normalized = Path[path].normalize.to_s
        # State update and kernel registration in one critical
        # section: a concurrent close could otherwise pass the @closed
        # check and leave register_input watching a closed descriptor
        # (same shape as KqueueWatcher#watch)
        @lock.synchronize do
          return if @closed || @target_inputs.includes?(normalized)
          @target_inputs << normalized
          prefix = normalized.ends_with?("/") ? normalized : "#{normalized}/"
          @prefix_inputs << {prefix, normalized}
          register_input(normalized)
        end
      end

      def close : Nil
        @lock.synchronize do
          return if @closed
          @closed = true
        end
        begin
          @inotify.close
        rescue ex : Inotify::Error
          # Closing an already-closed inotify descriptor is harmless here
        end
      end

      # Watch the input itself, or the nearest existing ancestor when
      # it doesn't exist yet (walking up, since the immediate parent
      # may also be missing), so its creation is seen.
      private def register_input(input : String) : Nil
        if File.exists? input
          # The caller logs "Watching: #{input}" for every input
          @inotify.watch input, watch_flags
        else
          parent = nearest_existing_ancestor(input)
          if !@inotify.watching.includes?(parent)
            @inotify.watch parent, watch_flags
            Log.info { "Watching parent: #{parent}" }
          end
        end
      end

      # The closest ancestor of `path` that exists (KqueueWatcher's
      # equivalent, so nested missing inputs don't fail registration)
      private def nearest_existing_ancestor(path : String) : String
        parent = Path[path].parent
        until File.exists?(parent)
          next_parent = parent.parent
          break if next_parent == parent
          parent = next_parent
        end
        parent.to_s
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

      # Re-watch files replaced by editors (IN_IGNORED), and queue
      # changes matching a watched input, exactly or by directory
      # prefix.
      private def event_handler : Proc(Inotify::Event, Nil)
        ->(event : Inotify::Event) do
          # The shard dispatches events from its own fiber: a raise
          # here (anywhere — including a user callback) would kill it,
          # and every future input change would be silently lost.
          begin
            handle_event(event)
          rescue ex
            Log.error { "inotify event handler crashed: #{ex.inspect_with_backtrace}" }
          end
        end
      end

      private def handle_event(event : Inotify::Event) : Nil
        # Path of the changed file. Without a name, fall back to the
        # bare path ("" if absent), which matches no input.
        path = if event.path && event.name
                 Path["#{event.path}/#{event.name}"].normalize.to_s
               else
                 event.path || ""
               end

        Log.debug do
          "inotify event: path=#{event.path.inspect}, name=#{event.name.inspect}, " \
          "mask=#{event.mask.inspect}, constructed=#{path.inspect}"
        end

        # The watch was removed (an editor deleted or replaced the
        # file): watch it again, or the nearest existing ancestor if
        # it and its parent are gone. The whole re-registration
        # happens under the lock, so it either completes before
        # close or is skipped; the rescue keeps a lost race (the
        # ancestor vanishing between the check and add_watch) from
        # killing the shard's event fiber
        if event.type_is?(LibInotify::IN_IGNORED)
          if ep = event.path
            @lock.synchronize do
              next if @closed
              next unless @target_inputs.includes?(ep)
              begin
                if File.exists?(ep)
                  @inotify.watch ep, watch_flags
                  Log.debug { "Re-watched file after editor replacement: #{ep}" }
                else
                  parent = nearest_existing_ancestor(ep)
                  unless @inotify.watching.includes?(parent)
                    @inotify.watch parent, watch_flags
                  end
                end
              rescue ex : Inotify::Error
                Log.warn { "Could not re-watch #{ep}: #{ex.message}" }
              end
            end
          end
        end

        matched = false
        if @lock.synchronize { @target_inputs.includes? path }
          @on_event.call(path)
          Log.debug { "Detected change in #{path} (exact match)" }
          matched = true
        else
          # A change inside a watched directory queues the directory
          if match = @lock.synchronize { @prefix_inputs.find { |prefix, _| path.starts_with?(prefix) } }
            normalized, input = match
            @on_event.call(input)
            Log.debug { "Detected change in #{input} (prefix match: #{path} starts with #{normalized})" }
            matched = true
          end
        end

        Log.debug { "Event NOT matched for path=#{path}" } unless matched
      end
    end
  end
{% end %}
