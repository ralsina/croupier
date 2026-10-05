module Croupier
  # TaskManagerType methods for content hashing, input scanning and
  # the run-state file.
  class TaskManagerType
    # Record the hash of a task output for the next run's state file,
    # and return the hash the last run recorded for it, in one locked
    # step. Thread-safe for parallel task workers.
    def swap_output_hash(output : String, new_hash : String) : String?
      @hashes_lock.synchronize do
        previous = last_run[output]?
        next_run[output] = new_hash
        previous
      end
    end

    # Whether `key` (a file or kv:// key) was modified since the last run.
    def modified?(key : String) : Bool
      @modified_lock.synchronize { modified.includes?(key) }
    end

    # Files known to exist during the current run, so readiness sweeps
    # don't re-stat every input on every wave. Only positive answers
    # are cached: a missing file may still appear (a side effect of
    # another task), and caching the miss would leave tasks waiting
    # forever. Cleared when a run starts and on cleanup.
    @existing_files = Set(String).new

    # File-existence check with the per-run positive cache. The stat
    # runs outside @files_lock so workers don't serialize on the
    # filesystem; racing inserts of the same path are harmless.
    def file_exists?(path : String) : Bool
      return true if @files_lock.synchronize { @existing_files.includes?(path) }
      if File.exists?(path)
        @files_lock.synchronize { @existing_files << path }
        true
      else
        false
      end
    end

    # Scan the given inputs (all of them by default) and return a hash
    # with their sha1. Files, including those inside directory inputs,
    # are hashed in parallel by a worker pool bounded by CPU count.
    def scan_inputs(scope : Set(String) | Nil = nil)
      hash = {} of String => String
      inputs = scope || all_inputs

      # One stat per path classifies it. Every regular file is then
      # hashed: content mode never trusts metadata (that is what fast
      # mode is for).
      file_inputs = [] of String
      inputs.each do |path|
        if key = path.lchop?("kv://")
          # A kv input hashes its current value, so kv changes are
          # detected like file changes. A missing key hashes as "",
          # which matches an absent state-file entry.
          value = get(key)
          hash[path] = value.nil? ? "" : Digest::SHA1.hexdigest(value)
        elsif info = File.info?(path)
          if info.file?
            file_inputs << path
          elsif info.directory?
            hash[path] = hash_directory(path)
          else
            # A fifo, socket or device: reading it could block forever
            # (a fifo has no EOF until a writer appears), so hash its
            # metadata instead. Paths that don't stat at all (deleted
            # files, dangling symlinks) are left out.
            hash[path] = Digest::SHA1.hexdigest("#{info.type}:#{info.modification_time.to_unix_f}:#{info.size}")
          end
        end
      end

      hash_files_parallel(file_inputs).each do |path, sha1|
        hash[path] = sha1
      end
      hash
    end

    # Hash a single directory input.
    #
    # The digest is a hash of hashes, like a git tree: every file in
    # the tree is hashed (in parallel), and the per-file hashes are
    # folded into one SHA1 together with the sorted entry list. The
    # entry list and the file hashes are separate, delimited fields,
    # so two different trees can't produce the same input bytes.
    #
    # Public because Task#run hashes no_save directory outputs with it.
    # The digest must match what scan_inputs computes for the same
    # directory, or a task consuming it would re-run every time. Safe
    # to call from parallel task workers (it only uses local channels).
    def hash_directory(path : String) : String
      # Hidden entries count: adding, removing or changing a dotfile
      # changes the digest
      entries = [] of String
      Croupier.collect_tree(path, entries)
      entries.sort!

      return Digest::SHA1.hexdigest(entries.join("\n")) if @fast_dirs

      files = entries.select(&->File.file?(String))
      file_hashes = hash_files_parallel(files)

      Digest::SHA1.hexdigest do |ctx|
        # Field 1: the sorted entry list (tree structure)
        ctx.update(entries.join("\n"))
        ctx.update("\n\n")
        # Field 2: each file's path and content hash
        files.each do |f|
          ctx.update(f)
          ctx.update("\0")
          ctx.update(file_hashes[f])
          ctx.update("\n")
        end
      end
    end

    # Hash a list of files concurrently, returning a {path => sha1} map.
    # A pool of worker fibers (bounded by CPU count) takes work in
    # chunks of SCAN_CHUNK_SIZE files, which keeps channel overhead
    # low. Lists of up to one chunk are hashed inline.
    private def hash_files_parallel(file_inputs : Array(String)) : Hash(String, String)
      hash = {} of String => String
      return hash if file_inputs.empty?

      if file_inputs.size <= SCAN_CHUNK_SIZE
        file_inputs.each { |path| hash[path] = Croupier.hash_file(path) }
        return hash
      end

      chunks = file_inputs.each_slice(SCAN_CHUNK_SIZE).to_a
      num_workers = Math.min(System.cpu_count, chunks.size)
      enable_parallelism(num_workers)
      task_queue = Channel(Array(String)).new(chunks.size)
      result_queue = Channel({Hash(String, String), Exception?}).new(chunks.size)

      chunks.each { |chunk| task_queue.send(chunk) }
      # Closing lets workers exit (receive? returns nil) once the
      # queue is drained
      task_queue.close

      num_workers.times do |worker_index|
        spawn(name: "croupier-scan-worker-#{worker_index}") do
          loop do
            chunk = task_queue.receive?
            break unless chunk
            results = {} of String => String
            error = nil
            # Report failures (an unreadable or just-deleted file)
            # through the queue: a worker dying here would leave the
            # collector below waiting forever
            begin
              chunk.each { |path| results[path] = Croupier.hash_file(path) }
            rescue ex
              error = ex
            end
            result_queue.send({results, error})
          end
        end
      end

      first_error = nil
      chunks.size.times do
        results, error = result_queue.receive
        results.each { |path, sha1| hash[path] = sha1 }
        first_error ||= error
      end
      raise first_error if first_error
      hash
    end

    # Resize the default fiber execution context so worker fibers spread
    # across OS threads. Cheap and idempotent.
    #
    # The API exists only on Crystal >= 1.21 without -Dpreview_mt (that
    # flag selects the old runtime, which has no execution contexts).
    # Elsewhere this is a no-op and workers run concurrently on one
    # thread.
    private def enable_parallelism(workers : Int) : Nil
      workers = 1 if workers < 1
      {% if !flag?(:preview_mt) && compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
        Fiber::ExecutionContext.default.resize(workers.to_i32)
      {% end %}
    end

    # How many files each scan worker hashes at a time: small enough
    # that one huge file doesn't hold up much other work, big enough
    # that channel overhead stays negligible
    SCAN_CHUNK_SIZE = 64

    # Version of the state-file schema, stored as __version. A
    # mismatch (or a file with no version) discards all recorded
    # hashes: one full rebuild instead of comparing hashes computed
    # by a different scheme.
    STATE_VERSION = "1"

    # Save the current state. It is written to a temporary file and
    # renamed into place, so a crash can't leave a truncated state
    # file. The temp name includes the PID so two processes sharing a
    # directory never write the same temp file.
    def save_run
      state = {"__version"   => STATE_VERSION,
               "__scan_time" => @scan_started.to_s}.merge(this_run.merge(next_run))
      temp_file = "#{@state_file}.tmp.#{Process.pid}"
      File.open(temp_file, "w") do |file|
        file << YAML.dump(state)
        # fsync before the rename, or a crash could leave the renamed
        # file empty (which would cost a full rebuild)
        file.fsync
      end
      File.rename(temp_file, @state_file)
    end

    # Serialize runs on the same state file across processes. Without
    # this, two processes in one directory would race their
    # read-scan-run-save cycles and the last writer would erase the
    # other's results.
    #
    # flock is released by the kernel when the holder dies, so there
    # is no stale lock to clean up. The lock file is never deleted:
    # that would let a third process lock a new inode while the old
    # one is still held. The lock is polled without blocking so other
    # fibers keep running while waiting. Not reentrant: nothing inside
    # a run may call run_tasks again.
    def with_state_lock(dry_run : Bool, &)
      # A dry run never writes state, and atomic renames make its
      # reads safe against concurrent writers
      return yield if dry_run
      lock_path = "#{@state_file}.lock"
      File.open(lock_path, "a") do |lock_file|
        until state_lock_acquired?(lock_file)
          Log.debug { "Waiting for another croupier process holding #{lock_path}" }
          sleep 10.milliseconds
        end
        yield
      end
    end

    private def state_lock_acquired?(lock_file : File) : Bool
      lock_file.flock_exclusive(blocking: false)
      true
    rescue IO::Error
      # flock_exclusive(blocking: false) raises when the lock is held
      false
    end

    # When the loaded run started its scan (unix time). Fast mode
    # compares input mtimes against it; nil when the state file has
    # no __scan_time entry.
    @last_scan_time : Float64? = nil
    # Scan start of the run in progress; written to the state file
    @scan_started : Float64 = 0.0

    # Read the state file. Anything unexpected (corrupt YAML, wrong
    # version, wrong shape) returns an empty hash, which makes every
    # input look modified: a safe full rebuild, fixed by the next save.
    private def load_state_file : Hash(String, String)
      # Reset first so an early return can't keep the scan time of a
      # previously loaded file
      @last_scan_time = nil
      # Check the shape explicitly instead of rescuing broadly, so a
      # bug in the code below raises instead of looking like an
      # unusable state file
      parsed = YAML.parse(File.read(@state_file)).as_h?
      return {} of String => String if parsed.nil?
      return {} of String => String if parsed["__version"]?.try(&.to_s) != STATE_VERSION
      @last_scan_time = parsed["__scan_time"]?.try &.to_s.to_f?
      entries = {} of String => String
      parsed.each do |key, value|
        next if {"__version", "__scan_time"}.includes?(key.to_s)
        # A non-string value means the file isn't our schema: treat
        # the whole state as unusable, like a version mismatch
        unless hash = value.as_s?
          Log.warn { "State file #{@state_file} has a non-string entry for #{key}, rebuilding everything" }
          return {} of String => String
        end
        entries[key.to_s] = hash
      end
      entries
    rescue ex : YAML::ParseException | File::Error
      Log.warn { "State file #{@state_file} is unusable (#{ex.message}), rebuilding everything" }
      {} of String => String
    end
  end
end
