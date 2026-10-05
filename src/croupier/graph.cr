module Croupier
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
    @parallel_wave_active = false
    # How many runs are executing right now (two can overlap in one
    # process: an auto cycle and a manual run_tasks waiting on the
    # state lock). Task creation is a setup-time operation, and
    # creating tasks mid-run is rejected instead of racing the run's
    # unlocked registry reads.
    @run_active = 0

    def add_input(task_key : String, input : String) : Bool
      @data_mutex.synchronize do
        task = tasks[task_key]?
        raise UnknownTaskError.new("Unknown task #{task_key}") unless task
        raise CycleError.new("Cycle detected: #{input} is a key of task #{task_key} itself") if task.keys.includes?(input)
        return false if task.inputs.includes?(input)

        if @parallel_wave_active
          # Deferred to the wave barrier: mutating an input set while
          # the coordinator iterates it is a data race
          @pending_inputs << {task_key, input}
        else
          task.inputs << input
          invalidate_graph_cache
        end
        true
      end
    end

    # add_input calls queued while a parallel wave is executing,
    # applied by the coordinating fiber at the wave barrier: mutating
    # an input set from a worker fiber while the coordinator iterates
    # it (readiness sweeps, early-cutoff staleness recomputes) is a
    # data race. Applied in call order inside the barrier's single
    # critical section.
    @pending_inputs = [] of {String, String}

    # Register a freshly constructed task. Task creation is a
    # setup-time operation: the task set is fixed before the first
    # run, and a mid-run creation would write the registries while
    # the run's reads take no lock. To change the task set, stop (in
    # auto mode), rebuild the graph, and start again.
    # @parallel_wave_active implies @run_active > 0 (a wave only runs
    # inside a run), so the run counter alone decides. Registration
    # and removal both check-and-apply inside one lock acquisition:
    # the run's registry reads take no lock, so ANY registry mutation
    # mid-run is a race, and a run starting between check and apply
    # would slip past (the immediate add_input path does the same).
    def register_task(task : Task, explicit_id : String?) : Nil
      @data_mutex.synchronize do
        reject_registry_mutation_during_run
        task.register_with_manager(explicit_id)
      end
    end

    # Remove a task by key: drops every registry key that resolves to
    # it (a multi-output task is registered under each output) and its
    # id-index entry, so re-creating a task under the same id works.
    # This is the supported removal — raw `TaskManager.tasks.delete`
    # leaves tasks_by_id stale, and the duplicate-id check then
    # rejects the replacement. Raises UnknownTaskError when the key
    # is not registered, and UsageError while a run is in progress
    # (change the task set between runs: stop auto mode, rebuild,
    # start again).
    def remove_task(task_key : String) : Nil
      @data_mutex.synchronize do
        reject_registry_mutation_during_run
        task = tasks[task_key]?
        raise UnknownTaskError.new("Unknown task #{task_key}") unless task
        task.keys.each do |key|
          tasks.delete(key) if tasks[key]?.same?(task)
        end
        tasks_by_id.delete(task.id) if tasks_by_id[task.id]?.same?(task)
        invalidate_graph_cache
      end
    end

    private def reject_registry_mutation_during_run : Nil
      return unless @run_active > 0
      raise UsageError.new(
        "Cannot change the task set while a run is in progress; build the task graph " \
        "before running (stop auto mode, rebuild, start again)"
      )
    end

    # Invalidate the cached task graph. Only touches in-memory state:
    # @graph_invalidated is what auto_run consults, so there is no
    # reason to round-trip a flag through the k/v store (which, with a
    # persistent store, meant a disk write per invalidation and a disk
    # read per auto cycle).
    def invalidate_graph_cache
      @graph_invalidated = true
      @all_inputs = nil
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
        # Invalidate the all_inputs cache so it is rebuilt with the
        # new dynamically created tasks
        @all_inputs = nil

        # All inputs are vertices
        all_inputs.each do |input|
          # The virtual root (ROOT_VERTEX) is a convenience node for
          # non-task inputs
          @graph[Croupier::ROOT_VERTEX] << input unless tasks.has_key? input
        end

        # Add vertices and edges for tasks: tasks with no inputs hang
        # off the virtual root, each input gets an edge into the task.
        # Every vertex (including tasks without outputs, keyed by id)
        # is registered on first touch by the hash's default block.
        tasks.each do |output, task|
          if task.@inputs.empty?
            @graph[Croupier::ROOT_VERTEX] << output
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

    # All inputs from all tasks, cached. nil means invalid: emptiness
    # used to be the invalidity signal, which made a legitimately
    # input-less task set rescan (and rebuild the set) on every call.
    @all_inputs : Set(String)? = nil

    def all_inputs : Set(String)
      if cached = @all_inputs
        return cached
      end
      result = Set(String).new
      tasks.values.each do |task|
        result.concat task.@inputs
      end
      @all_inputs = result
      result
    end

    # The set of all inputs for the given tasks
    def inputs(targets : Array(String))
      result = Set(String).new
      targets.each do |target|
        raise UnknownTaskError.new("Unknown target #{target}") unless tasks.has_key? target
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
          raise UnknownTaskError.new("Unknown output #{output}")
        end
      end
      result = _dependencies outputs
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

    # Read state of last run, then scan inputs and compare, leaving
    # the changed paths in @modified for propagate_staleness.
    #
    # Three modes, one method each with its own scan+compare:
    #
    #   auto     the watcher already said WHAT changed; hashing only
    #            re-confirms it (unchanged rewrites must not retrigger)
    #   fast     mtime comparison against the last run's scan start,
    #            no content hashing
    #   content  full hash comparison against the last run's hashes
    #
    # Two modifiers complete the old 5-flag matrix:
    #
    #   run_all  only matters to fast mode: staleness decisions are
    #            overridden anyway (every task re-runs), so its mtime
    #            scan is pure overhead. Content mode still scans
    #            because @this_run feeds save_run, and skipping it
    #            would make the next incremental run rebuild
    #            everything.
    #   targets  narrows the scan scope to the inputs of the tasks
    #            the run may execute; unrelated inputs keep their
    #            recorded hashes, so a change to them is detected by
    #            the next run that includes their tasks.
    def mark_stale_inputs(run_all : Bool = false, targets : Array(String)? = nil)
      # New run: the positive file-existence cache may be stale
      @existing_files.clear
      # When this run's scan starts: recorded in the state file so the
      # NEXT fast-mode run compares input mtimes against this moment
      # rather than the state file's own mtime (written at the END of
      # the run, which hides inputs modified mid-run). A zero here
      # would make every input look modified (one spurious full
      # rebuild when auto and fast mode mix).
      @scan_started = Time.utc.to_unix_f
      if auto_mode?
        scan_auto_mode
      elsif @fast_mode
        scan_fast_mode(run_all, targets)
      else
        scan_content_mode(targets)
      end
    end

    # Auto mode: the watcher queues changed paths, so the scan only has
    # to confirm them: events fire on rewrites even when the content is
    # identical (a task that regenerates a watched input unchanged
    # would retrigger itself forever). Like content mode, decide by
    # comparing hashes against the last completed cycle; unscanned
    # entries (deleted files, kv keys set outside this flow) are kept
    # as modified. last_run comes from the autorun fiber's in-memory
    # folds, not the state file, which is why nothing is loaded here.
    private def scan_auto_mode : Nil
      @this_run = scan_inputs
      # Keep only changes the last completed cycle doesn't already
      # know about. Mutated in place under the lock: reassigning the
      # set would race readers holding the old reference
      @modified_lock.synchronize do
        kept = @modified.select { |path|
          if hash = @this_run[path]?
            last_run.fetch(path, "") != hash
          else
            true
          end
        }
        @modified.clear
        kept.each { |path| @modified << path }
      end
    end

    # How far back (seconds) the fast-mode mtime comparison reaches,
    # absorbing filesystem timestamp granularity and clock skew at
    # the cost of occasionally re-detecting an input modified just
    # before the previous scan (the classic make solution).
    FAST_MODE_GRACE = 1.0

    # Fast mode: an input is modified when its mtime is newer than the
    # last run's scan start. No content hashing at all.
    private def scan_fast_mode(run_all : Bool, targets : Array(String)?) : Nil
      kv_modifications = take_kv_modifications
      state_file_date = load_last_run
      # Base @this_run on @last_run so input hashes recorded by the
      # last hash-mode run survive the save: fast mode can't hash, so
      # wiping them would make the next hash-mode run treat every
      # input as modified (a surprise full rebuild)
      @this_run = @last_run.dup
      # With run_all every task re-runs regardless of staleness, so
      # the mtime sweep is pure overhead; @this_run above is still
      # prepared so the save keeps the last recorded hashes
      return if run_all
      # Compare mtimes against the last run's scan START (recorded
      # in the state file): the file is saved at the END of the run,
      # so its own mtime would hide inputs modified mid-run. The
      # grace window absorbs filesystem timestamp granularity and the
      # small clock skew between recorded wall time and mtimes, at
      # the cost of occasionally re-detecting an input modified just
      # before the previous scan. The fallback for state files
      # written before __scan_time existed compares mtime to mtime,
      # which needs no grace.
      scan_started = if baseline = @last_scan_time
                       baseline - FAST_MODE_GRACE
                     else
                       state_file_date.to_unix_f
                     end
      # Stat outside the lock: the sweep can touch every input of
      # every task, and holding @modified_lock across those syscalls
      # blocks task workers calling set()/modified? — the same lock
      # convoy file_exists? avoids by stat'ing unlocked. Collect the
      # modified files first, then insert them under the lock in one
      # batch, together with the k/v modifications.
      modified_now = [] of String
      scan_scope(targets).each do |file|
        if info = File.info?(file)
          modified_now << file if info.modification_time.to_unix_f > scan_started
        end
      end
      @modified_lock.synchronize do
        modified_now.each { |file| @modified << file }
        # Fast mode can't hash values, so k/v modifications are still
        # detected through set()'s flags and must survive the clear
        kv_modifications.each { |key| @modified << key }
      end
    end

    # Content mode: hash every input in scope and compare against the
    # last run's recorded hashes.
    private def scan_content_mode(targets : Array(String)?) : Nil
      # The k/v entries are taken out (and @modified cleared) even
      # though the return value is unused: k/v modifications are
      # hash-detected like files below, and set()'s flags from before
      # the run (or from the previous run) are stale by comparison
      # and must NOT survive the clear, or a one-time change
      # re-stales its dependents on every later run
      take_kv_modifications
      load_last_run
      scanned = scan_inputs(scan_scope(targets))
      # Base @this_run on @last_run so hashes of inputs outside the
      # scan scope survive into the state file save (dropping them
      # would make the next full run treat those inputs as modified
      # and rebuild everything).
      @this_run = @last_run.merge(scanned)
      @modified_lock.synchronize do
        scanned.each do |file, sha1|
          @modified << file if last_run.fetch(file, "") != sha1
        end
      end
    end

    # Split the kv:// entries out of @modified and clear the rest:
    # k/v modifications are marked by set() (possibly from parallel
    # task workers of a PREVIOUS run) and must survive the clear,
    # because in fast mode those flags are the only kv change
    # detection there is.
    private def take_kv_modifications : Array(String)
      @modified_lock.synchronize do
        kv = @modified.select(&.starts_with?("kv://"))
        @modified.clear
        kv
      end
    end

    # Load the last run's recorded hashes from the state file, or
    # start from scratch when there is none. Also refreshes
    # @last_scan_time (fast mode's comparison baseline). Returns the
    # state file's own mtime: the pre-__scan_time fallback fast mode
    # compares against when no baseline was recorded.
    private def load_last_run : Time
      if File.exists? @state_file
        last_run_date = File.info(@state_file).modification_time
        @last_run = load_state_file
        last_run_date
      else
        @last_run = {} of String => String
        @last_scan_time = nil
        Time.utc # No state file: nothing can be older than now
      end
    end

    # The inputs a run needs to look at: every input of every task
    # when untargeted, or just the inputs of the given targets.
    private def scan_scope(targets : Array(String)?) : Set(String)
      return all_inputs unless targets
      scope = Set(String).new
      targets.each do |name|
        if task = tasks[name]?
          scope.concat task.@inputs
        end
      end
      scope
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
      # One pass per unique task: `tasks` is keyed per output, so a
      # multi-output task would re-traverse its inputs once per key.
      @reverse_deps.clear
      seen_tasks = Set(Task).new
      tasks.each_value do |task|
        next unless seen_tasks.add?(task)
        task.inputs.each do |input|
          if tasks.has_key?(input)
            # Every key of the dependent task is a staleness carrier:
            # when this input goes stale, the whole task goes stale
            @reverse_deps[input].concat(task.keys)
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
      # We use a worklist algorithm for efficiency. An index cursor
      # instead of shift(): popping from the array front is O(n) per
      # visit, which quietly turned the O(V+E) walk into O(V*E) on
      # wide graphs.
      worklist = stale_tasks.to_a
      cursor = 0
      while cursor < worklist.size
        stale_output = worklist[cursor]
        cursor += 1

        reverse_deps[stale_output].each do |dependent|
          unless stale_tasks.includes?(dependent)
            stale_tasks << dependent
            worklist << dependent
          end
        end
      end

      # Mark non-stale tasks as fresh (once per unique task; a
      # multi-output task's keys enter stale_tasks jointly)
      seen_tasks.clear
      tasks.each_value do |task|
        next unless seen_tasks.add?(task)
        task.stale = task.keys.any? { |key| stale_tasks.includes?(key) }
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

      # One staleness evaluation per unique task: `tasks` is keyed
      # per output, so iterating it directly would re-stat every
      # output of a multi-output task once per key
      seen_tasks = Set(Task).new
      tasks.each_value do |task|
        next unless seen_tasks.add?(task)

        if task.always_run? || task.inputs.empty?
          stale_tasks.concat(task.keys)
          next
        end

        file_outputs = task.outputs.reject(&.lchop?("kv://"))
        kv_outputs = task.outputs.select(&.lchop?("kv://")).map(&.lchop("kv://"))

        # Check if outputs are missing
        missing_outputs = file_outputs.any? { |o| !File.exists?(o) } ||
                          kv_outputs.any? { |o| !TaskManager.get(o) }
        if missing_outputs
          stale_tasks.concat(task.keys)
          next
        end

        # Check if inputs are modified
        modified_inputs = task.inputs.any? { |i| modified?(i) }
        if modified_inputs
          stale_tasks.concat(task.keys)
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
