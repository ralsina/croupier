# Croupier describes a task graph and lets you operate on them
require "./croupier/errors"
require "./task"
require "./croupier/kv_store"
require "./croupier/hash_state"
require "./croupier/graph"
require "./croupier/runner"
require "./croupier/watcher"
require "digest/sha1"
{% if flag?(:linux) %}
  require "inotify"
  require "./croupier/linux_watcher"
{% elsif flag?(:darwin) %}
  require "./croupier/kqueue_watcher"
{% end %}
require "kiwi/file_store"
require "kiwi/memory_store"
require "log"

module Croupier
  VERSION = {{ `shards version #{__DIR__}`.chomp.stringify }}

  # Log with "croupier" as the source
  Log = ::Log.for("croupier")

  alias CallbackProc = Proc(String, Nil)

  # SHA1 of a file's contents, read in chunks so large files are never
  # held in memory whole. Raises if the file can't be opened.
  def self.hash_file(path : String) : String
    File.open(path) do |file|
      digest = Digest::SHA1.new
      buffer = Bytes.new(64 * 1024)
      while read = file.read(buffer)
        break if read == 0
        digest.update(buffer[0, read])
      end
      digest.hexfinal
    end
  end

  # Append every path under `dir` (files and subdirectories, dotfiles
  # included, `dir` itself excluded) to `entries`. The tree is walked
  # rather than globbed, so metacharacters in a name (a directory
  # called "assets[2]") are taken literally. A symlinked `dir` is
  # followed; symlinked directories inside the tree are not descended
  # into. Directory digests are built from this list, so changing its
  # shape re-stales every directory input (specs pin it).
  def self.collect_tree(dir : String, entries : Array(String)) : Nil
    Dir.each_child(dir) do |child|
      entry = File.join(dir, child)
      entries << entry
      collect_tree(entry, entries) if File.directory?(entry) && !File.symlink?(entry)
    end
  end

  # TaskManager is a singleton that keeps track of all tasks.
  # Its methods live in focused files under src/croupier/: kv_store.cr,
  # hash_state.cr, graph.cr, runner.cr and watcher.cr.
  class TaskManagerType
    # Registry of all tasks, keyed by each output (or by id for tasks
    # without outputs).
    #
    # Read without locks while a run executes, so the task set must not
    # change mid-run: `Task.new` and `remove_task` raise `UsageError`
    # during a run. Change it through those two methods (and grow
    # inputs through `add_input`), never by writing to the hash.
    getter tasks : Hash(String, Croupier::Task) = {} of String => Croupier::Task
    # Inputs (files and kv:// keys) modified since the last run; they
    # make the tasks that consume them stale.
    #
    # Task procs touch this set from parallel workers (`set` marks
    # kv:// keys, `modified?` reads it), so every internal access goes
    # through @modified_lock. Mutating it directly from a running proc
    # races.
    property modified = Set(String).new
    # Hashes recorded by the previous run (from the state file, or
    # folded in by the previous auto mode cycle).
    #
    # Concurrency contract shared by last_run / this_run / next_run:
    # task workers only touch them through the @hashes_lock accessors
    # in hash_state.cr (swap_output_hash). Every
    # other access happens on the coordinating fiber (the run_tasks
    # caller, or the autorun fiber in auto mode), so those sites take
    # no lock.
    property last_run = {} of String => String
    # Input hashes scanned at the start of this run.
    #
    # See last_run for the concurrency contract.
    property this_run = {} of String => String
    # Output hashes recorded by tasks during this run.
    #
    # See last_run for the concurrency contract.
    property next_run = {} of String => String
    # If true, only compare file dates
    property? fast_mode : Bool = false
    # If true, it's running in auto mode
    property? auto_mode : Bool = false
    # If true, directories depend on a list of files, not its contents
    property? fast_dirs : Bool = false
    # If true, enable early cutoff optimization (skip tasks when upstream outputs unchanged)
    property? early_cutoff : Bool = true
    # Path to the state file that stores hashes between runs
    property state_file : String = ".croupier"
    # If set, it's called after every task finishes
    property progress_callback : Proc(String, Nil) = ->(_id : String) { }
    # If set, it's called in auto mode after changes are detected but before tasks run
    # Receives the set of modified paths (files and kv:// keys)
    property before_run_hook : Proc(Set(String), Nil) = ->(_changes : Set(String)) { }
    # A hash of mutexes required by tasks
    property mutexes = {} of String => Sync::Mutex
    # Task id -> task, so the duplicate-id check on task creation is
    # O(1) instead of a scan over every registered task.
    getter tasks_by_id : Hash(String, Task) = {} of String => Task
    @graph_invalidated : Bool = false

    # Register the mutex `name`, keeping the existing lock if there is
    # one: replacing it could swap out a lock a running task holds,
    # and tasks sharing the name would stop excluding each other.
    def add_mutex(name : String)
      @data_mutex.synchronize { mutexes[name] ||= Sync::Mutex.new }
    end

    def lock_mutex(name : String)
      # The registry read takes no lock: mutexes are registered when a
      # task declares them, before any run starts, and taking
      # @data_mutex twice per proc call made workers contend on it.
      # The locked fallback only covers direct calls with a name that
      # was never declared.
      if mutex = mutexes[name]?
        mutex.lock
      else
        mutex = @data_mutex.synchronize { mutexes[name] ||= Sync::Mutex.new }
        mutex.lock
      end
    end

    def unlock_mutex(name : String)
      # No lock and no KeyError: this runs in Task#run's ensure, where
      # raising would mask the proc's own exception
      mutexes[name]?.try &.unlock
    end

    # Locks for state shared with parallel task workers, split by
    # concern so hot paths don't contend on one lock: the k/v store
    # (@store_lock), the run hashes (@hashes_lock), the modified set
    # (@modified_lock) and the file-existence cache (@files_lock).
    # @data_mutex covers the rest: registry writes, the run counter,
    # the wave flag, the pending add_input queue and the mutex
    # registry fallback.
    @data_mutex = Sync::Mutex.new
    @store_lock = Sync::Mutex.new
    @hashes_lock = Sync::Mutex.new
    @modified_lock = Sync::Mutex.new
    @files_lock = Sync::Mutex.new

    # Remove all tasks and everything else (good for tests)
    def cleanup
      # Stop the autorun fiber first, so it doesn't fire runs against
      # the cleared manager halfway through cleanup
      auto_stop
      @modified_lock.synchronize { modified.clear }
      # Locked: cleanup can race a live autorun cycle's registry reads,
      # and a run ending in another fiber (which applies the add_input
      # queue and decrements the run counter)
      @data_mutex.synchronize do
        tasks.clear
        tasks_by_id.clear
        @pending_inputs.clear
        @run_active = 0
      end
      last_run.clear
      this_run.clear
      next_run.clear
      @all_inputs = nil
      @sorted_keys = nil
      @reverse_deps.clear
      # Locked: the filesystem watcher may still deliver events while
      # cleanup runs
      clear_queued_changes
      @existing_files.clear
      @_store_path = nil
      @_store = Kiwi::MemoryStore.new
      @store_cache.clear
      @store_misses.clear
      @store_keys.clear
      @fast_mode = false
      @fast_dirs = false
      @auto_mode = false
      @graph_invalidated = false
      # Session-level state a run may have configured
      @state_file = ".croupier"
      @early_cutoff = true
      mutexes.clear
      @progress_callback = ->(_id : String) { }
      @before_run_hook = ->(_changes : Set(String)) { }
      close_watcher
    end
  end

  # The global task manager (singleton)
  TaskManager = TaskManagerType.new
end
