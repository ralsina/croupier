# Croupier describes a task graph and lets you operate on them
require "./task"
require "./topo_sort"
require "./croupier/kv_store"
require "./croupier/hash_state"
require "./croupier/graph"
require "./croupier/runner"
require "./croupier/watcher"
require "digest/sha1"
{% if flag?(:linux) %}
  require "inotify"
{% end %}
require "kiwi/file_store"
require "kiwi/memory_store"
require "log"

module Croupier
  VERSION = {{ `shards version #{__DIR__}`.chomp.stringify }}

  # Log with "croupier" as the source
  Log = ::Log.for("croupier")

  alias CallbackProc = Proc(String, Nil)

  # SHA1 of a file's contents, streamed so large files are never
  # buffered whole. Shared by Task#run (no_save output verification)
  # and the manager's input scanner; an unreadable file still raises
  # from File.open, same as File.read did.
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

  # TaskManager is a singleton that keeps track of all tasks.
  # Its methods live in focused files under src/croupier/: kv_store.cr,
  # hash_state.cr, graph.cr, runner.cr and watcher.cr.
  class TaskManagerType
    # Registry of all tasks.
    #
    # Treat as read-only while tasks are running: workers and the
    # coordinating fiber traverse it (and the Task objects in it)
    # concurrently during run_tasks. To grow a task's dependencies
    # between runs, use `add_input` instead of mutating `tasks` or
    # `Task#inputs` directly.
    property tasks = {} of String => Croupier::Task
    # Registry of modified files, which will make tasks stale
    property modified = Set(String).new
    # SHA1 of files from last run
    property last_run = {} of String => String
    # SHA1 of files as of starting this run
    property this_run = {} of String => String
    # SHA1 of input files as of ending this run
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
    # Receives the list of changed files as an argument
    property before_run_hook : Proc(Set(String), Nil) = ->(_changes : Set(String)) { }
    # A hash of mutexes required by tasks
    property mutexes = {} of String => Sync::Mutex
    # Task id -> task index so the per-creation duplicate-id check is
    # O(1) instead of a linear scan over every registered task (which
    # made creating N tasks O(N^2); a 4000-task site spent ~200ms in
    # the scan alone)
    property tasks_by_id = {} of String => Task
    @graph_invalidated : Bool = false

    def add_mutex(name : String)
      @data_mutex.synchronize { mutexes[name] = Sync::Mutex.new }
    end

    def lock_mutex(name : String)
      # Registry reads are lock-free: every naming path registers the
      # mutex at declaration time (block initializer, mutex= setter),
      # before waves start — the same read-only-during-runs contract
      # the tasks registry relies on. Taking @data_mutex here twice
      # per task proc was a lock convoy (see the 0.14 performance
      # report). The locked fallback only covers direct calls with a
      # never-declared name.
      if mutex = mutexes[name]?
        mutex.lock
      else
        mutex = @data_mutex.synchronize { mutexes[name] ||= Sync::Mutex.new }
        mutex.lock
      end
    end

    def unlock_mutex(name : String)
      # Lock-free read, no KeyError: this runs in Task#run's ensure,
      # where raising would mask the proc's own exception
      mutexes[name]?.try &.unlock
    end

    # Guards the shared data containers, which parallel task workers
    # mutate and read from multiple OS threads. Split by concern so
    # hot paths don't contend on one lock (see the 0.14 performance
    # report): store trio, run hashes, modified set, existing-files
    # cache each get their own; @data_mutex stays for the rare paths
    # (mutex registry fallback, pending-input queue, wave flag).
    @data_mutex = Sync::Mutex.new
    @store_lock = Sync::Mutex.new
    @hashes_lock = Sync::Mutex.new
    @modified_lock = Sync::Mutex.new
    @files_lock = Sync::Mutex.new

    # Remove all tasks and everything else (good for tests)
    def cleanup
      modified.clear
      tasks.clear
      tasks_by_id.clear
      # Stop the autorun fiber first, so it doesn't fire runs against
      # the cleared manager halfway through cleanup
      auto_stop
      last_run.clear
      this_run.clear
      next_run.clear
      @all_inputs.clear
      @graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
      @graph_sorted = [] of String
      @reverse_deps.clear
      @queued_changes.clear
      @existing_files.clear
      @_store_path = nil
      @_store = Kiwi::MemoryStore.new
      @store_cache.clear
      @store_misses.clear
      @fast_mode = false
      @auto_mode = false
      @graph_invalidated = false
      # Session-level state a run may have configured
      @state_file = ".croupier"
      @early_cutoff = true
      mutexes.clear
      @progress_callback = ->(_id : String) { }
      @before_run_hook = ->(_changes : Set(String)) { }
      {% if flag?(:linux) %}
        return unless watcher = @@watcher
        begin
          watcher.close
        rescue ex : Inotify::Error
          # Ignore "Bad file descriptor" errors during cleanup
          # This can happen when the watcher is already closed or invalid
        end
        @@watcher = nil
      {% end %}
    end
  end

  # The global task manager (singleton)
  TaskManager = TaskManagerType.new
end
