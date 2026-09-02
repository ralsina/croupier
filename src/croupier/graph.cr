module Croupier
  # Raised when a task can't run yet because an input is not
  # satisfiable: it is neither a fresh task, an existing file, nor a
  # kv:// key. In auto mode this is an expected transient state (inputs
  # appear incrementally), so the autorun loop rescues this class and
  # retries with backoff instead of logging a warning on every cycle.
  class UnknownInputsError < Exception
  end

  # TaskManagerType methods for the dependency graph, staleness
  # computation and dependency queries.
  class TaskManagerType
    # Add `input` to the inputs of the task registered as `task_key`.
    #
    # Thread-safe (guarded by @data_mutex), so it is the supported way to
    # grow a task's dependencies from task procs running on parallel
    # workers. It can only influence the NEXT run: wave planning, staleness
    # propagation and the task graph are all computed before workers
    # start. It invalidates the graph and input caches so that next run
    # actually sees the new dependency.
    #
    # While a parallel wave is executing, the addition is queued and
    # applied by the coordinating fiber at the wave barrier: mutating a
    # task's input set while the coordinator iterates it (readiness
    # sweeps, early-cutoff staleness recomputes) would be a data race.
    # Outside waves it is applied immediately.
    #
    # Returns true if the input was added, false if the task already had
    # it. Raises if `task_key` is not a registered task, or if the input
    # is one of the task's own keys (that would be a cycle).
    @pending_inputs = [] of {String, String}
    @parallel_wave_active = false

    def add_input(task_key : String, input : String) : Bool
      @data_mutex.synchronize do
        task = tasks[task_key]?
        raise "Unknown task #{task_key}" unless task
        raise "Cycle detected" if task.keys.includes?(input)
        return false if task.inputs.includes?(input)

        if @parallel_wave_active
          @pending_inputs << {task_key, input}
        else
          task.inputs << input
          invalidate_graph_cache
        end
        true
      end
    end

    # Apply queued add_input calls. Runs on the coordinating fiber at
    # wave boundaries, when no worker can mutate task inputs
    # concurrently with the iteration below.
    private def apply_pending_inputs
      pending = @data_mutex.synchronize do
        swapped = @pending_inputs
        @pending_inputs = [] of {String, String}
        swapped
      end
      return if pending.empty?
      pending.each do |task_key, input|
        if task = tasks[task_key]?
          # Set#<< is idempotent: duplicates queued during the wave
          # collapse on their own
          task.inputs << input
        end
      end
      invalidate_graph_cache
    end

    # Register a subtask with the task manager
    def register_subtask(master_id : String, subtask : Task)
      # Track this subtask in master's subtask list
      if master = tasks[master_id]?
        master.subtask_ids << subtask.id
      end

      invalidate_graph_cache
    end

    # Remove all subtasks belonging to a master task
    def remove_subtasks(master_id : String)
      if master = tasks[master_id]?
        keys_to_delete = [] of String
        tasks.each do |key, task|
          keys_to_delete << key if master.subtask_ids.includes?(task.id)
        end
        keys_to_delete.each do |key|
          if task = tasks.delete(key)
            tasks_by_id.delete(task.id)
          end
        end
        master.subtask_ids.clear
      end

      invalidate_graph_cache
    end

    # Invalidate the cached task graph. Only touches in-memory state:
    # @graph_invalidated is what auto_run consults, so there is no
    # reason to round-trip a flag through the k/v store (which, with a
    # persistent store, meant a disk write per invalidation and a disk
    # read per auto cycle).
    def invalidate_graph_cache
      @graph_invalidated = true
      @all_inputs.clear
    end

    # Tasks as a dependency graph sorted topologically.
    # A plain adjacency hash (vertex => the vertices it points at); the
    # default block registers vertices on first touch, so an edge add
    # never needs a separate "add vertex" step.
    @graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
    @graph_sorted = [] of String
    # Reverse dependency map (output name -> task keys that depend on it).
    # Built in propagate_staleness and reused by the early-cutoff scans so
    # they don't have to walk every task per output.
    @reverse_deps = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }

    def sorted_task_graph
      # Rebuild graph if invalidated
      if @graph_invalidated || @graph.empty?
        @graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
        @graph_sorted = [] of String
        @graph_invalidated = false
        # Clear all_inputs cache so it gets rebuilt with new subtask inputs
        @all_inputs.clear

        # All inputs are vertices
        all_inputs.each do |input|
          # The start node is just a convenience root for non-task inputs
          @graph["start"] << input unless tasks.has_key? input
        end

        # Add vertices and edges for tasks: tasks with no inputs hang
        # off the start node, each input gets an edge into the task.
        # Every vertex (including tasks without outputs, keyed by id)
        # is registered on first touch by the hash's default block.
        tasks.each do |output, task|
          if task.@inputs.empty?
            @graph["start"] << output
          end
          task.@inputs.each do |input|
            @graph[input] << output
          end
        end

        # Register every mentioned vertex as a key so leaves show up
        # with empty sets, matching the shape callers expect (the sort
        # itself no longer registers vertices as a side effect of
        # reading them)
        @graph.values.flat_map(&.to_a).each do |vertex|
          @graph[vertex]
        end

        # Only return tasks, not inputs in the sorted graph
        @graph_sorted = Croupier.topological_sort(@graph).select { |v| tasks.has_key? v }
      end
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
    # Uses memoization to avoid exponential blowup when many tasks share dependencies
    def _dependencies(outputs : Array(String))
      _dependencies_impl(outputs, {} of String => Set(String))
    end

    # Memoized implementation of _dependencies
    # Each node's own transitive closure is cached, so a memoized entry
    # never includes contributions from sibling outputs processed earlier
    # in the same call.
    private def _dependencies_impl(outputs : Array(String), memo : Hash(String, Set(String)))
      result = Set(String).new
      outputs.each do |output|
        # Return cached result if available
        if memo.has_key?(output)
          result.concat memo[output]
          next
        end

        if tasks.has_key?(output)
          # Compute this node's *own* closure (itself + closure of inputs)
          # into a local set, cache THAT, then merge into the running result.
          node_result = Set(String).new
          node_result << output
          node_result.concat(_dependencies_impl(tasks[output].@inputs.to_a, memo))
          memo[output] = node_result
          result.concat(node_result)
        end
      end
      result
    end

    def depends_on(input : String)
      depends_on [input]
    end

    def depends_on(inputs : Array(String))
      depends_on_impl(inputs, {} of String => Set(String), consumers_index)
    end

    # input -> tasks consuming it, built in one pass so depends_on
    # doesn't rescan every registered task for every queried input.
    private def consumers_index
      consumers = Hash(String, Array(Task)).new
      tasks.each_value do |task|
        task.@inputs.each do |input|
          if bucket = consumers[input]?
            bucket << task
          else
            consumers[input] = [task]
          end
        end
      end
      consumers
    end

    # Memoized implementation of depends_on
    # Each input's own transitive closure (the outputs of all tasks that
    # consume it, plus their closures) is cached independently, so a
    # memoized entry never includes contributions from other inputs
    # processed earlier in the same call.
    private def depends_on_impl(
      inputs : Array(String),
      memo : Hash(String, Set(String)),
      consumers : Hash(String, Array(Task)),
    )
      result = Set(String).new
      inputs.each do |input|
        # Return cached result if available
        if memo.has_key?(input)
          result.concat memo[input]
          next
        end

        # Compute this input's *own* closure into a local set: for every
        # task that consumes `input`, add its outputs and their closures.
        node_result = Set(String).new
        consumers.fetch(input, nil).try &.each do |task|
          node_result.concat task.outputs
          node_result.concat(depends_on_impl(task.outputs, memo, consumers))
        end
        memo[input] = node_result
        result.concat(node_result)
      end
      result
    end

    # Read state of last run, then scan inputs and compare.
    #
    # With run_all the scan only feeds staleness *decisions*, which are
    # then overridden anyway (every task re-runs), so in fast mode the
    # mtime scan is skipped as pure overhead. Content mode still scans
    # because @this_run feeds save_run and skipping it would make the
    # next incremental run rebuild everything.
    def mark_stale_inputs(run_all : Bool = false, targets : Array(String)? = nil)
      # New run: the positive file-existence cache may be stale
      @existing_files.clear
      if auto_mode?
        # In auto mode, the watcher tells us WHAT to look at, but events
        # fire on rewrites even when the content is identical (a task
        # that regenerates a watched input unchanged retriggers itself
        # forever). Like content mode, decide by comparing hashes
        # against the last completed cycle; unscanned entries (deleted
        # files, kv keys set outside this flow) are kept as modified.
        @this_run = scan_inputs
        @modified = @modified.select { |path|
          if hash = @this_run[path]?
            last_run.fetch(path, "") != hash
          else
            true
          end
        }.to_set
        return
      end

      # Preserve k/v store modifications before clearing
      # K/v modifications are added via set() and need to survive the clear
      # so that propagate_staleness() can detect tasks that depend on them
      kv_modifications = @modified.select(&.starts_with?("kv://"))
      @modified.clear

      # When this run's scan starts: recorded in the state file so the
      # NEXT fast-mode run compares input mtimes against this moment
      # rather than the state file's own mtime (written at the END of
      # the run, which hides inputs modified mid-run)
      @scan_started = Time.utc.to_unix_f

      if File.exists? @state_file
        last_run_date = File.info(@state_file).modification_time
        @last_run = load_state_file
      else
        last_run_date = Time.utc # Now
        @last_run = {} of String => String
        @last_scan_time = nil
      end

      # A targeted run only scans the inputs of the tasks it may
      # execute, instead of re-hashing every input of every registered
      # task: unrelated inputs are left as recorded by the previous
      # run, so a change to them is still detected by the next run
      # that includes their tasks.
      scan_scope = if targets
                     scope = Set(String).new
                     targets.each do |name|
                       if task = tasks[name]?
                         scope.concat task.@inputs
                       end
                     end
                     scope
                   else
                     all_inputs
                   end

      if @fast_mode
        mark_stale_inputs_fast_mode(run_all, scan_scope, kv_modifications, last_run_date)
      else
        mark_stale_inputs_content_mode(scan_scope, kv_modifications)
      end
    end

    private def mark_stale_inputs_fast_mode(
      run_all : Bool,
      scan_scope : Set(String),
      kv_modifications : Array(String),
      last_run_date : Time,
    )
      # Base @this_run on @last_run so input hashes recorded by the
      # last hash-mode run survive the save: fast mode can't hash, so
      # wiping them would make the next hash-mode run treat every
      # input as modified (a surprise full rebuild)
      @this_run = @last_run.dup
      return if run_all
      # Compare mtimes against the last run's scan START (recorded
      # in the state file): the file is saved at the END of the
      # run, so its own mtime would hide inputs modified mid-run.
      # A one-second grace window (the classic make solution)
      # absorbs filesystem timestamp granularity and the small
      # clock skew between recorded wall time and mtimes, at the
      # cost of occasionally re-detecting an input modified just
      # before the previous scan. The fallback for state files
      # written before __scan_time existed compares mtime to
      # mtime, which needs no grace.
      scan_started = if baseline = @last_scan_time
                       baseline - 1.0
                     else
                       last_run_date.to_unix_f
                     end
      scan_scope.each do |file|
        if info = File.info?(file)
          @modified << file if info.modification_time.to_unix_f > scan_started
        end
      end
      # Fast mode can't hash values, so k/v modifications are still
      # detected through set()'s flags and must survive the clear
      @modified |= kv_modifications.to_set
    end

    private def mark_stale_inputs_content_mode(
      scan_scope : Set(String),
      kv_modifications : Array(String),
    )
      scanned = scan_inputs(scan_scope)
      # Base @this_run on @last_run so hashes of inputs outside the
      # scan scope survive into the state file save (dropping them
      # would make the next full run treat those inputs as modified
      # and rebuild everything).
      @this_run = @last_run.merge(scanned)
      scanned.each do |file, sha1|
        @modified << file if last_run.fetch(file, "") != sha1
      end
      # k/v modifications are hash-detected like files here; set()'s
      # flags from before the run (or from the previous run) are
      # stale by comparison and must NOT survive the clear, or a
      # one-time change re-stales its dependents on every later run
    end

    # Propagate staleness through the task graph in a single forward pass.
    # This replaces the expensive recursive staleness checking with an
    # O(V+E) algorithm that's critical for tasks with many inputs.
    #
    # With run_all every task re-runs regardless of freshness, and
    # staleness only gates *ordering* (dependents wait for stale
    # dependencies). Leaving every task stale preserves correct ordering
    # while skipping the per-task root scan (File.exists? and kv lookups
    # per output), which is pure overhead under run_all.
    def propagate_staleness(run_all : Bool = false)
      # Reset all task staleness to true (stale) first as default
      tasks.values.each(&.stale=(true))

      # Build reverse dependency graph (who depends on me) into the cached
      # @reverse_deps field, reused later by the early-cutoff scans.
      @reverse_deps.clear
      tasks.each do |output, task|
        task.inputs.each do |input|
          if tasks.has_key?(input)
            @reverse_deps[input] << output
          end
        end
      end

      if run_all
        Log.debug { "run_all: skipping staleness root scan, all tasks stale" }
        return
      end

      reverse_deps = @reverse_deps

      stale_tasks = find_stale_roots

      # Propagate staleness through the graph
      # If a task is stale, all tasks that depend on it are also stale
      # We use a worklist algorithm for efficiency
      worklist = stale_tasks.to_a

      while !worklist.empty?
        stale_output = worklist.shift

        reverse_deps[stale_output].each do |dependent|
          unless stale_tasks.includes?(dependent)
            stale_tasks << dependent
            worklist << dependent
          end
        end
      end

      # Mark non-stale tasks as fresh
      tasks.each do |output, task|
        task.stale = stale_tasks.includes?(output)
      end

      Log.debug { "Propagated staleness: #{stale_tasks.size} stale, #{tasks.size - stale_tasks.size} fresh" }
    end

    # Find definitively stale tasks (roots of staleness)
    # These are tasks that are stale for their own reasons:
    # 1. always_run tasks
    # 2. Tasks with no inputs
    # 3. Tasks with missing outputs
    # 4. Tasks with modified inputs
    private def find_stale_roots : Set(String)
      stale_tasks = Set(String).new

      tasks.each do |output, task|
        if task.always_run? || task.inputs.empty?
          stale_tasks << output
          next
        end

        file_outputs = task.outputs.reject(&.lchop?("kv://"))
        kv_outputs = task.outputs.select(&.lchop?("kv://")).map(&.lchop("kv://"))

        # Check if outputs are missing
        missing_outputs = file_outputs.any? { |o| !File.exists?(o) } ||
                          kv_outputs.any? { |o| !TaskManager.get(o) }
        if missing_outputs
          stale_tasks << output
          next
        end

        # Check if inputs are modified
        modified_inputs = task.inputs.any? { |i| modified.includes?(i) }
        if modified_inputs
          stale_tasks << output
          next
        end
      end

      stale_tasks
    end

    # Check if all inputs are correct:
    # They should all be either task outputs or existing files.
    # With `targets`, only the inputs of the requested closure are
    # checked (a targeted run doesn't care about unrelated tasks'
    # inputs).
    def check_dependencies(targets : Array(String)? = nil)
      scope = targets ? inputs(targets) : all_inputs
      bad_inputs = scope.select { |input|
        !input.lchop?("kv://") &&
          !tasks.has_key?(input) &&
          !File.exists?(input)
      }
      raise UnknownInputsError.new("Can't run: Unknown inputs #{bad_inputs.join(", ")}") \
        unless bad_inputs.empty?
    end
  end
end
