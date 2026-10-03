{% if flag?(:darwin) %}
  module Croupier
    # Recursive filesystem watcher backed by macOS kqueue vnode events.
    #
    # kqueue reports which watched vnode changed, but not the child name for a
    # directory event. Croupier only needs to know which task input changed,
    # so each descriptor is associated with its root input. After a structural
    # event the root is registered again, picking up newly-created files and
    # replacement files atomically written by editors.
    class KqueueWatcher
      EVFILT_VNODE = -4_i16

      NOTE_DELETE  = 0x00000001_u32
      NOTE_WRITE   = 0x00000002_u32
      NOTE_EXTEND  = 0x00000004_u32
      NOTE_ATTRIB  = 0x00000008_u32
      NOTE_LINK    = 0x00000010_u32
      NOTE_RENAME  = 0x00000020_u32
      NOTE_REVOKE  = 0x00000040_u32
      VNODE_EVENTS = NOTE_DELETE | NOTE_WRITE | NOTE_EXTEND | NOTE_ATTRIB |
                     NOTE_LINK | NOTE_RENAME | NOTE_REVOKE

      # Darwin's event-only open mode avoids requiring read permission and
      # works for both files and directories. Crystal's LibC does not expose
      # this Darwin-specific constant.
      O_EVTONLY  = 0x00008000
      WAKE_IDENT =      1_u64

      @kqueue : Int32
      @lock = Sync::Mutex.new
      @roots = Set(String).new
      # Descriptor -> {root input, descriptor belongs to the root's current
      # tree, descriptor is a directory}. A missing input is watched through
      # an ancestor directory; such an event is relevant only if the input's
      # existence changed.
      @fd_roots = {} of Int32 => {String, Bool, Bool}
      @root_exists = {} of String => Bool
      @closed = false
      @thread : Thread

      def initialize(@on_event : Proc(String, Nil))
        @kqueue = LibC.kqueue
        raise IO::Error.from_errno("kqueue") if @kqueue < 0

        register_user_event
        @thread = Thread.new(name: "croupier-kqueue") { event_loop }
      end

      # Watch a task input recursively. If it does not exist yet, watch the
      # nearest existing parent so its eventual creation is still detected.
      def watch(path : String) : Nil
        normalized = Path[path].normalize.to_s
        @lock.synchronize do
          return if @closed || @roots.includes?(normalized)
          @roots << normalized
          register_root(normalized)
        end
      end

      def close : Nil
        should_close = @lock.synchronize do
          next false if @closed
          @closed = true
          true
        end
        return unless should_close

        trigger_wakeup
        @thread.join
        @lock.synchronize do
          @fd_roots.each_key { |fd| LibC.close(fd) }
          @fd_roots.clear
          LibC.close(@kqueue)
        end
      end

      private def register_user_event : Nil
        change = kevent(
          ident: WAKE_IDENT,
          filter: LibC::EVFILT_USER,
          flags: LibC::EV_ADD | LibC::EV_CLEAR,
          fflags: 0_u32
        )
        submit(change, "register kqueue wake event")
      end

      private def trigger_wakeup : Nil
        change = kevent(
          ident: WAKE_IDENT,
          filter: LibC::EVFILT_USER,
          flags: 0_u16,
          fflags: LibC::NOTE_TRIGGER
        )
        # If the descriptor has already failed, the event thread will exit on
        # its own. Closing remains idempotent and should not mask cleanup.
        LibC.kevent(@kqueue, pointerof(change), 1, nil, 0, nil)
      end

      private def event_loop : Nil
        events = StaticArray(LibC::Kevent, 64).new { LibC::Kevent.new }
        loop do
          count = LibC.kevent(@kqueue, nil, 0, events.to_unsafe, events.size, nil)
          if count < 0
            next if Errno.value == Errno::EINTR
            break
          end

          roots = Set(String).new
          refresh_roots = Set(String).new
          wake = false
          @lock.synchronize do
            count.times do |index|
              event = events[index]
              if event.filter == LibC::EVFILT_USER && event.ident == WAKE_IDENT
                wake = true
              elsif event.filter == EVFILT_VNODE
                if entry = @fd_roots[event.ident.to_i]?
                  root, direct, directory = entry
                  existence_changed = @root_exists[root]? != File.exists?(root)
                  if direct || existence_changed
                    roots << root
                  end
                  if existence_changed || structural_event?(event.fflags, directory)
                    refresh_roots << root
                  end
                end
              end
            end
          end
          break if wake

          roots.each do |root|
            @on_event.call(root)
          end
          refresh_roots.each do |root|
            @lock.synchronize do
              register_root(root) unless @closed
            end
          end
        end
      rescue ex
        Log.error(exception: ex) { "macOS filesystem watcher stopped" }
      end

      # Close every descriptor for this input and reconstruct its recursive
      # watch set. Re-registering is needed after rename/delete and discovers
      # children created since the previous event.
      private def register_root(root : String) : Nil
        @fd_roots.select { |_, entry| entry[0] == root }.each_key do |fd|
          LibC.close(fd)
          @fd_roots.delete(fd)
        end

        root_exists = File.exists?(root)
        watch_paths(root, root_exists).each do |path|
          fd = LibC.open(path, O_EVTONLY | LibC::O_CLOEXEC)
          next if fd < 0 # The path may disappear while the tree is scanned.

          change = kevent(
            ident: fd.to_u64,
            filter: EVFILT_VNODE,
            flags: LibC::EV_ADD | LibC::EV_CLEAR,
            fflags: VNODE_EVENTS
          )
          if LibC.kevent(@kqueue, pointerof(change), 1, nil, 0, nil) < 0
            LibC.close(fd)
          else
            @fd_roots[fd] = {root, root_exists, File.directory?(path)}
          end
        end
        @root_exists[root] = root_exists
      end

      # Root plus, if it is a directory, every entry under it
      # (dotfiles included). The tree is walked explicitly with
      # Dir.each_child instead of interpolating root into a glob
      # pattern: metacharacters in a path's own name (a directory
      # literally named "assets[2]") must be taken literally, not
      # interpreted as a pattern — the same fix hash_state.cr's
      # collect_directory_entries made for directory hashing. A
      # symlinked root IS followed (matching the previous glob);
      # symlinked directories inside the tree are not descended
      # into (the glob's follow_symlinks: false behavior).
      private def watch_paths(root : String, root_exists : Bool) : Array(String)
        return [existing_parent(root)] unless root_exists

        paths = [root]
        collect_watch_paths(root, paths) if File.directory?(root)
        paths
      end

      private def collect_watch_paths(dir : String, paths : Array(String)) : Nil
        Dir.each_child(dir) do |child|
          entry = File.join(dir, child)
          paths << entry
          collect_watch_paths(entry, paths) if File.directory?(entry) && !File.symlink?(entry)
        end
      end

      private def existing_parent(path : String) : String
        parent = Path[path].parent
        until File.exists?(parent)
          next_parent = parent.parent
          break if next_parent == parent
          parent = next_parent
        end
        parent.to_s
      end

      # Normal writes to an already-watched file need no descriptor churn.
      # Rebuild only when a vnode was replaced/removed, or when a directory's
      # children changed and the recursive descriptor set may be stale.
      private def structural_event?(flags : UInt32, directory : Bool) : Bool
        replacement = NOTE_DELETE | NOTE_RENAME | NOTE_REVOKE
        (flags & replacement) != 0 || (directory && (flags & NOTE_WRITE) != 0)
      end

      private def kevent(
        ident : UInt64,
        filter : Int16,
        flags : UInt16,
        fflags : UInt32,
      ) : LibC::Kevent
        event = LibC::Kevent.new
        event.ident = ident
        event.filter = filter
        event.flags = flags
        event.fflags = fflags
        event.data = 0
        event.udata = Pointer(Void).null
        event
      end

      private def submit(event : LibC::Kevent, operation : String) : Nil
        if LibC.kevent(@kqueue, pointerof(event), 1, nil, 0, nil) < 0
          error = IO::Error.from_errno(operation)
          LibC.close(@kqueue)
          raise error
        end
      end
    end
  end
{% end %}
