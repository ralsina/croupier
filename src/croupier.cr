# Croupier describes a task graph and lets you operate on them
require "./task"
require "./topo_sort"
require "crystalline"
require "digest/sha1"
require "inotify"
require "kiwi/file_store"
require "kiwi/memory_store"
require "log"
require "wait_group"

module Croupier
  VERSION = {{ `shards version #{__DIR__}`.chomp.stringify }}

  # Log with "croupier" as the source
  Log = ::Log.for("croupier")

  alias CallbackProc = Proc(String, Nil)

  # TaskManager is a singleton that keeps track of all tasks
  class TaskManagerType
    # Registry of all tasks
    property tasks = {} of String => Croupier::Task
    # Registry of modified files, which will make tasks stale
    property modified = Set(String).new
    # SHA1 of files from last run
    property last_run = {} of String => String
    # SHA1 of files as of starting this run
    property this_run = {} of String => String
    # SAH1 of input files as of ending this run
    property next_run = {} of String => String
    # If true, only compare file dates
    property? fast_mode : Bool = false
    # If true, it's running in auto mode
    property? auto_mode : Bool = false
    # If true, directories depend on a list of files, not its contents
    property? fast_dirs : Bool = false
    # If set, it's called after every task finishes
property progress_callback : Proc(String, Nil) = ->(_id : String) { }
    # A hash of mutexes required by tasks
    property mutexes = {} of String => Mutex

    def add_mutex(name : String)
      mutexes[name] = Mutex.new
    end

    def lock_mutex(name : String)
      mutexes[name].lock
    end

    def unlock_mutex(name : String)
      mutexes[name].unlock
    end

    # Files with changes detected in auto_run
    @queued_changes : Set(String) = Set(String).new

    # Key/Value store
    @_store : Kiwi::Store = Kiwi::MemoryStore.new
    @_store_path : String | Nil = nil

    def set(key, value)
      Log.debug { "Setting k/v data for #{key}" }
      @_store.set(key, value)
      @modified << "kv://#{key}"
    end

    def get(key)
      @_store.get(key)
    end

    # Use a persistent k/v store in this path instead of
    # the default memory store
    def use_persistent_store(path : String)
      return if path == @_store_path
      raise "Can't change persistent k/v store path" unless @_store_path.nil?
      new_store = Kiwi::FileStore.new(path)
      if @_store_path.nil?
        # Convert from MemoryStore to FileStore
        old_store = @_store.as(Kiwi::MemoryStore)
        old_store.@mem.each { |k, v| new_store[k] = v }
        @_store = new_store
      end
      Log.debug { "Storing k/v data in #{path}" }
    end

    # Remove all tasks and everything else (good for tests)
    def cleanup
      modified.clear
      tasks.clear
      last_run.clear
      this_run.clear
      next_run.clear
      @all_inputs.clear
      @graph = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new
      @graph_sorted = [] of String
      @queued_changes.clear
      @_store_path = nil
      @_store = Kiwi::MemoryStore.new
      @fast_mode = false
      @auto_mode = false
      if watcher = @@watcher
        begin
          watcher.close
        rescue ex : Inotify::Error
          # Ignore "Bad file descriptor" errors during cleanup
          # This can happen when the watcher is already closed or invalid
        end
        @@watcher = nil
      end
    end

    # Tasks as a dependency graph sorted topologically
    @graph = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new
    @graph_sorted = [] of String

    def sorted_task_graph
      return @graph, @graph_sorted unless @graph.@vertice_dict.empty?
      # First, we create the graph
      @graph = Crystalline::Graph::DirectedAdjacencyGraph(String, Set(String)).new

      # Add all tasks and inputs as vertices
      # Add all dependencies as edges

      # The start node is just a convenience
      @graph.add_vertex "start"

      # All inputs are vertices
      all_inputs.each do |input|
        if !tasks.has_key? input
          @graph.add_vertex input
          @graph.add_edge "start", input
        end
      end

      # Tasks without outputs are added as vertices by ID
      tasks.values.each do |task|
        if task.@outputs.empty?
          @graph.add_vertex task.@id
        end
      end

      # Add vertices and edges for inputs
      tasks.each do |output, task|
        @graph.add_vertex output
        if task.@inputs.empty?
          @graph.add_edge "start", output
        end
        task.@inputs.each do |input|
          @graph.add_edge input, output
        end
      end

      # Only return tasks, not inputs in the sorted graph
      @graph_sorted = topological_sort(@graph.@vertice_dict).select { |v| tasks.has_key? v }
      return @graph, @graph_sorted
    end

    # All inputs from all tasks
    @all_inputs = Set(String).new

    def all_inputs
      return @all_inputs unless @all_inputs.empty?
      tasks.values.each do |task|
        @all_inputs.concat task.@inputs
      end
      @all_inputs
    end

    # The set of all inputs for the given tasks
    def inputs(targets : Array(String))
      result = Set(String).new
      targets.each do |target|
        raise "Unknown target #{target}" unless tasks.has_key? target
      end

      dependencies(targets).each do |task|
        result.concat tasks[task].@inputs
      end

      result
    end

    # Get a task list of what tasks need to be done to produce `outputs`
    # The list is sorted so it can be executed in order
    def dependencies(outputs : Array(String))
      outputs.each do |output|
        if !tasks.has_key?(output)
          raise "Unknown output #{output}"
        end
      end
      result = self._dependencies outputs
      sorted_task_graph[1].select(->(v : String) { result.includes? v })
    end

    # Get a task list of what tasks need to be done to produce `output`
    # The list is sorted so it can be executed in order
    # Overloaded to accept a single string for convenience
    def dependencies(output : String)
      dependencies([output])
    end

    # Helper function for dependencies
    def _dependencies(outputs : Array(String))
      result = Set(String).new
      outputs.each do |output|
        if tasks.has_key? output
          result << output
          result.concat _dependencies(tasks[output].@inputs.to_a)
        end
      end
      result
    end

    def depends_on(input : String)
      depends_on [input]
    end

    def depends_on(inputs : Array(String))
      result = Set(String).new
      TaskManager.tasks.values.each do |t|
        if (t.@inputs & Set.new(inputs)).size > 0
          result.concat t.outputs
          result.concat depends_on(t.outputs)
        end
      end
      result
    end

    # Read state of last run, then scan inputs and compare
    def mark_stale_inputs
      if File.exists? ".croupier"
        last_run_date = File.info(".croupier").modification_time
        last_run = File.open(".croupier") do |file|
          YAML.parse(file).as_h.map { |k, v| [k.to_s, v.to_s] }.to_h
        end
      else
        last_run_date = Time.utc # Now
        last_run = {} of String => String
      end
      if @fast_mode
        all_inputs.each do |file|
          if info = File.info?(file)
            @modified << file if last_run_date < info.modification_time
          end
        end
      else
        (@this_run = scan_inputs).each do |file, sha1|
          @modified << file if last_run.fetch(file, "") != sha1
        end
      end
    end

    # Scan all inputs and return a hash with their sha1
    def scan_inputs
      all_inputs.reduce({} of String => String) do |hash, path|
        if File.file? path
          hash[path] = Digest::SHA1.hexdigest(File.read(path))
        else
          if File.directory? path
            digest = Digest::SHA1.digest do |ctx|
              # Hash the directory tree
              ctx.update(Dir.glob("#{path}/**/*").sort.join("\n"))
              if !@fast_dirs
                # Hash *everything* in the directory (this will be slow)
                Dir.glob("#{path}/**/*").each do |f|
                  ctx.update File.read(f) if File.file? f
                end
              end
            end
            hash[path] = digest.hexstring
          end
        end
        hash
      end
    end

    # We ran all tasks, store the current state
    def save_run
      File.open(".croupier", "w") do |file|
        file << YAML.dump(this_run.merge next_run)
      end
    end

    # Check if all inputs are correct:
    # They should all be either task outputs or existing files
    def check_dependencies
      bad_inputs = all_inputs.select { |input|
        !input.lchop?("kv://") &&
          !tasks.has_key?(input) &&
          !File.exists?(input)
      }
      raise "Can't run: Unknown inputs #{bad_inputs.join(", ")}" \
         unless bad_inputs.empty?
    end

    # Run all stale tasks in dependency order
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    def run_tasks(
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false,
    )
      _, tasks = sorted_task_graph
      check_dependencies
      run_tasks(tasks, run_all, dry_run, parallel, keep_going)
    end

    # Run the tasks needed to create or update the requested targets
    #
    # If `run_all` is true, run non-stale tasks too
    # If `dry_run` is true, only log what would be done, but don't do it
    # If `parallel` is true, run tasks in parallel
    # If `keep_going` is true, keep going even if a task fails
    def run_tasks(
      targets : Array(String),
      run_all : Bool = false,
      dry_run : Bool = false,
      parallel : Bool = false,
      keep_going : Bool = false,
    )
      tasks = dependencies(targets)
      if parallel
        _run_tasks_parallel(tasks, run_all, dry_run, keep_going)
      else
        _run_tasks(tasks, run_all, dry_run, keep_going)
      end
    end

    # Internal helper to run tasks serially
    def _run_tasks(
      outputs,
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false,
    )
      mark_stale_inputs
      finished = Set(Task).new
      outputs.compact_map { |o|
        tasks.fetch(o, nil)
      }.reject { |t|
        !(t.stale? || run_all || t.@always_run)
      }.each do |t|
        next if t.nil? || finished.includes?(t)
        Log.debug { "Running task for #{t.outputs}" }
        raise "Can't run task for #{t.outputs}: Waiting for #{t.waiting_for}" unless t.waiting_for.empty? || dry_run
        begin
          t.run unless dry_run
        rescue ex
          Log.error { "Error running task for #{t.outputs}: #{ex}" }
          raise ex unless keep_going
        end
        finished << t
      end
      save_run
    end

    # Internal helper to run tasks concurrently
    #
    # Whenever a task is ready, launch it in a separate fiber
    # This is only concurrency, not parallelism, but on tests
    # it seems to be faster than running tasks sequentially.
    def _run_tasks_parallel(
      targets : Array(String) = [] of String,
      run_all : Bool = false,
      dry_run : Bool = false,
      keep_going : Bool = false,
    )
      mark_stale_inputs

      targets = tasks.keys if targets.empty?
      _tasks = dependencies(targets).map { |t| tasks[t] }
      finished_tasks = Set(Task).new
      failed_tasks = Set(Task).new
      errors = [] of String

      loop do
        if run_all
          stale_tasks = _tasks.reject { |t|
            finished_tasks.includes?(t) || failed_tasks.includes?(t)
          }
        else
          stale_tasks = _tasks.select(&.stale?).reject { |t|
            finished_tasks.includes?(t) || failed_tasks.includes?(t)
          }
        end

        break if stale_tasks.empty?

        # The uniq is because a task may be repeated in the
        # task graph because of multiple outputs. We don't
        # want to run it twice.
        batch = stale_tasks.select(&.ready?(run_all)).uniq!.shuffle

        if batch.size == 0
          # No tasks are ready
          raise "Can't run tasks: Waiting for #{stale_tasks.map(&.waiting_for).uniq!.join(", ")}"
        end

        chunk_size = batch.size // 4
        chunks = [
          batch[0...chunk_size],
          batch[chunk_size...chunk_size * 2],
          batch[chunk_size * 2...chunk_size * 3],
          batch[chunk_size * 3..],
        ]

        wg = WaitGroup.new(batch.size)
        Log.debug { "Starting batch of #{batch.size} tasks in 4 chunks of #{chunk_size} tasks" }
        # This uses each `chunk` as a queue and runs all 4 queues in parallel.
        # It's not the *best* way to do it because if a chunk has larger tasks
        # than the others, it will take longer to finish. But it's good enough
        # for now.
        chunks.each_with_index do |chunk, i|
          spawn do
            chunk.each do |t|
              begin
                Fiber.yield
                t.run unless dry_run
              rescue ex
                failed_tasks << t
                errors << ex.message.to_s
              ensure
                # Task is done, do not run again
                t.stale = false
                finished_tasks << t
                wg.done
                Fiber.yield
              end
            end
          end
        end
        # Wait for the whole batch to finish
        wg.wait
      end
      raise errors.join("\n") unless errors.empty? unless keep_going
      save_run
    end

    @autorun_control = Channel(Bool).new

    def auto_stop
      @autorun_control.send true
      @autorun_control.receive?
      @autorun_control = Channel(Bool).new
    end

    def auto_run(targets : Array(String) = [] of String)
      @auto_mode = true
      targets = tasks.keys if targets.empty?
      # Only want dependencies that are not tasks
      inputs = inputs(targets)
      raise "No inputs to watch, can't auto_run" if inputs.empty?
      watch(targets)
      spawn do
        loop do
          select
          when @autorun_control.receive
            Log.info { "Stopping automatic run" }
            @autorun_control.close
            if watcher = @@watcher
              watcher.close # Stop watchers
            end
            break
          else
            begin
              # Sleep early is better for race conditions in tests
              # If we sleep late, it's likely that we'll get the
              # stop order and break the loop without running, so
              # we can't see the side effects without sleeping in
              # the tests.
              sleep 0.01.seconds
              next if @queued_changes.empty? && @modified.empty?
              Log.info { "Detected changes in #{@queued_changes}" }
              # Mark all targets as stale
              targets.each { |t| tasks[t].stale = true }
              @modified += @queued_changes
              Log.debug { "Modified: #{@modified}" }
              run_tasks(targets: targets)
              # Only clean queued changes after a successful run
              @modified.clear
              @queued_changes.clear
            rescue ex
              # Sometimes we can't run because not all dependencies
              # are there yet or whatever. We'll try again later
              unless ex.message.to_s.starts_with?("Can't run: Unknown inputs")
                Log.warn { "Automatic run failed (will retry): #{ex.message}" }
              end
            end
          end
        end
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
      @@watcher = Inotify::Watcher.new
      targets = tasks.keys if targets.empty?
      target_inputs = inputs(targets)

      if watcher = @@watcher
        watcher.on_event do |event|
          # It's a file we care about, add it to the queue
          path = Path["#{event.path}/#{event.name}"].normalize.to_s
          Log.debug { "Detected change in #{path}" }
          Log.trace { "Event: #{event}" }
          # If path matches a watched path, add it to the queue
          if target_inputs.includes? path
            @queued_changes << path
            Log.debug { "Queued change in #{path}" }
          elsif target_inputs.any? { |input|
                  if path.starts_with? "#{input}/"
                    # If we are watching a folder in path, add the folder to the queue
                    @queued_changes << input
                    Log.debug { "Queued change in #{input}" }
                  end
                }
          else
            Log.trace { "Ignoring event" }
          end
        end

        watch_flags = LibInotify::IN_DELETE |
                      LibInotify::IN_CREATE |
                      LibInotify::IN_MODIFY |
                      LibInotify::IN_MOVED_TO |
                      LibInotify::IN_CLOSE_WRITE
        # NOT watching IN_DELETE_SELF, IN_MOVE_SELF because
        # when those are triggered we have no input file to
        # process.

        target_inputs.each do |input|
          # Don't watch for changes in k/v store
          next if input.lchop?("kv://")
          if File.exists? input
            watcher.watch input, watch_flags
          else
            # It's a file that doesn't exist. To detect it
            # being created, we watch the parent directory
            # if we are not already watching it.
            path = (Path[input].parent).to_s
            if !watcher.watching.includes?(path)
              watcher.watch path, watch_flags
            end
          end
        end
      end
    end
  end

  # The global task manager (singleton)
  TaskManager = TaskManagerType.new
end
