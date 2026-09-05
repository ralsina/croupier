module Croupier
  # TaskManagerType methods for content hashing, input scanning and
  # the run-state file.
  class TaskManagerType
    # Record the hash of a task output for the next run's state file.
    # Thread-safe for parallel task workers.
    def record_output_hash(output : String, new_hash : String) : Nil
      @hashes_lock.synchronize { next_run[output] = new_hash }
    end

    # Record `new_hash` for `output` and return the hash the last run
    # recorded for it, in a single locked step: task workers call this
    # once per output instead of a record-then-previous round-trip.
    def swap_output_hash(output : String, new_hash : String) : String | Nil
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
    # don't re-stat every plain-file input of every task on every wave.
    # Only positive answers are cached: a file that appeared since the
    # last check must still be discovered (negative results would
    # deadlock tasks waiting on side-effect files), while a file that
    # already exists stays satisfied. Cleared when a run starts and on
    # cleanup.
    @existing_files = Set(String).new

    # File-existence check with a per-run positive cache. The cache
    # check and the insert take @data_mutex, but the stat itself does
    # NOT: holding the mutex across a syscall serialized every worker's
    # readiness checks on the filesystem. Positive-only caching is
    # preserved (misses are re-checked, so files appearing mid-run are
    # still found), and racing inserters of the same path are
    # idempotent.
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
    # with their sha1.
    #
    # Plain files and the contents of directory inputs are hashed in
    # parallel (a pool of worker fibers bounded by CPU count), since both
    # the disk read and the hashing are independent per file.
    def scan_inputs(scope : Set(String) | Nil = nil)
      hash = {} of String => String
      inputs = scope || all_inputs

      # Partition inputs into kv keys, files (hashable in parallel)
      # and directories.
      file_inputs = [] of String
      inputs.each do |path|
        if key = path.lchop?("kv://")
          # A kv input's "hash" is the digest of its current value, so
          # kv modifications are detected exactly like file
          # modifications (a missing key hashes as "" and matches an
          # absent state-file entry)
          value = get(key)
          hash[path] = value.nil? ? "" : Digest::SHA1.hexdigest(value)
        elsif File.file? path
          file_inputs << path
        elsif File.directory? path
          hash[path] = hash_directory(path)
        end
      end

      hash_files_parallel(file_inputs).each do |path, sha1|
        hash[path] = sha1
      end
      hash
    end

    # Hash a single directory input.
    #
    # The directory digest is a hash-of-hashes: every file in the tree is
    # hashed independently (in parallel), and those per-file hashes are
    # folded into a final SHA1 along with the sorted entry list. This is
    # the Merkle-tree pattern (as used by git tree objects): collision
    # resistance is preserved, and per-file hashing parallelizes the
    # expensive part while leaving the door open to a future per-file
    # mtime+size cache.
    #
    # The path list and the file-hash list are framed as separate,
    # newline-joined fields with a distinct separator so two different
    # trees can't collide by construction (the previous scheme folded raw
    # file bytes directly into the same context with no boundary).
    #
    # Public because Task#run hashes no_save directory outputs with it:
    # the digest MUST match what scan_inputs computes for the same
    # directory when a later task consumes it as an input, or that
    # dependent would re-run on every invocation. Uses only local
    # channels (via hash_files_parallel), so it is safe to call from
    # parallel task workers.
    def hash_directory(path : String) : String
      # Walk the tree once and reuse the list. Hidden entries count:
      # an added/removed/changed .env must change the digest. The tree
      # is traversed explicitly instead of interpolating `path` into a
      # glob pattern: metacharacters in a path's own name (a directory
      # literally named "assets[2]") must be taken literally, not
      # interpreted as a pattern.
      entries = [] of String
      collect_directory_entries(path, entries)
      entries.sort!

      return Digest::SHA1.hexdigest(entries.join("\n")) if @fast_dirs

      # Hash every file in the tree in parallel.
      files = entries.select(&->File.file?(String))
      file_hashes = hash_files_parallel(files)

      Digest::SHA1.hexdigest do |ctx|
        # Field 1: the sorted entry list (captures tree structure).
        ctx.update(entries.join("\n"))
        ctx.update("\n\n")
        # Field 2: each file's path + content hash (captures contents).
        files.each do |f|
          ctx.update(f)
          ctx.update("\0")
          ctx.update(file_hashes[f])
          ctx.update("\n")
        end
      end
    end

    # Every path under `dir` (files and subdirectories, dotfiles
    # included, `dir` itself excluded) appended to `entries`. A
    # symlinked `dir` IS followed, so its real contents are hashed;
    # symlinked directories inside the tree are not descended into
    # (the old glob's follow_symlinks: false behavior). The entry
    # list is the basis of the directory digest, so its shape is
    # pinned by specs: changing it would silently re-stale every
    # directory input.
    private def collect_directory_entries(dir : String, entries : Array(String)) : Nil
      Dir.each_child(dir) do |child|
        entry = File.join(dir, child)
        entries << entry
        collect_directory_entries(entry, entries) if File.directory?(entry) && !File.symlink?(entry)
      end
    end

    # Hash a list of files concurrently, returning a {path => sha1} map.
    # Uses a shared channel of work and a small pool of worker fibers
    # bounded by CPU count. Work and results travel in chunks: every
    # channel operation costs a lock, so one message per file meant two
    # lock round-trips per input; chunks amortize that to ~2 per 64
    # files. Small batches are hashed inline, skipping the pool
    # machinery entirely.
    private def hash_files_parallel(file_inputs : Array(String)) : Hash(String, String)
      hash = {} of String => String
      return hash if file_inputs.empty?

      chunk_size = 64
      if file_inputs.size <= chunk_size
        file_inputs.each { |path| hash[path] = Croupier.hash_file(path) }
        return hash
      end

      chunks = file_inputs.each_slice(chunk_size).to_a
      num_workers = Math.min(System.cpu_count, chunks.size)
      enable_parallelism(num_workers)
      task_queue = Channel(Array(String)).new(chunks.size)
      result_queue = Channel(Hash(String, String)).new(chunks.size)

      chunks.each { |chunk| task_queue.send(chunk) }
      # Close the queue so workers exit (receive? returns nil) instead of
      # parking forever on the drained channel
      task_queue.close

      num_workers.times do
        spawn do
          loop do
            chunk = task_queue.receive?
            break unless chunk
            results = {} of String => String
            chunk.each { |path| results[path] = Croupier.hash_file(path) }
            result_queue.send(results)
          end
        end
      end

      chunks.size.times do
        result_queue.receive.each { |path, sha1| hash[path] = sha1 }
      end
      hash
    end

    # Resize the default fiber execution context so worker fibers spread
    # across OS threads (real parallelism, not just concurrency). Cheap and
    # idempotent, so it's safe to call once per batch / per call.
    #
    # The API only exists on Crystal >= 1.21 without -Dpreview_mt (the
    # deprecated flag selects the old runtime, which lacks execution
    # contexts). Guard on both so the call compiles everywhere; elsewhere
    # this degrades to a no-op, same as before the resize existed.
    private def enable_parallelism(workers : Int) : Nil
      workers = 1 if workers < 1
      {% if !flag?(:preview_mt) && compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
        Fiber::ExecutionContext.default.resize(workers.to_i32)
      {% end %}
    end

    # Version of the state-file schema, stored as __version. A
    # mismatch (including files written before versioning existed)
    # discards all recorded hashes: one full rebuild instead of
    # silently comparing hashes computed by a different scheme (the
    # directory digest already changed shape once).
    STATE_VERSION = "1"

    # We ran all tasks, store the current state. Written to a
    # temporary file and renamed into place, so a crash mid-write
    # can't leave a truncated state file behind.
    def save_run
      state = {"__version"   => STATE_VERSION,
               "__scan_time" => @scan_started.to_s}.merge(this_run.merge(next_run))
      File.open("#{@state_file}.tmp", "w") do |file|
        file << YAML.dump(state)
      end
      File.rename("#{@state_file}.tmp", @state_file)
    end

    # Read the state file, guarding against corruption and schema
    # drift: anything unexpected means we know nothing about the
    # previous run, which makes every input look modified (a full
    # rebuild) — safe, and self-healing on the next save.
    # When the run being loaded started its scan (unix_f), recorded so
    # fast mode compares mtimes against the previous run's scan start;
    # nil for state files written before it was recorded
    @last_scan_time : Float64 | Nil = nil
    # Scan start of the run in progress; written to the state file
    @scan_started : Float64 = 0.0

    private def load_state_file : Hash(String, String)
      # Full reset on every load: an early return must not leave a
      # stale scan time from a previous state file leaking in
      @last_scan_time = nil
      # An explicit as_h? check instead of a broad rescue, so a bug in
      # the mapping handling below raises instead of masquerading as
      # "unusable state, rebuild everything"
      parsed = YAML.parse(File.read(@state_file)).as_h?
      return {} of String => String if parsed.nil?
      return {} of String => String if parsed["__version"]?.try(&.to_s) != STATE_VERSION
      @last_scan_time = parsed["__scan_time"]?.try &.to_s.to_f?
      parsed.reject! { |key, _| {"__version", "__scan_time"}.includes?(key.to_s) }
        .map { |key, value| {key.to_s, value.to_s} }.to_h
    rescue ex : YAML::ParseException | File::Error
      # Invalid YAML or an unreadable file means we know nothing about
      # the previous run: a full rebuild, self-healed on the next save
      Log.warn { "State file #{@state_file} is unusable (#{ex.message}), rebuilding everything" }
      {} of String => String
    end
  end
end
