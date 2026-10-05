module Croupier
  # TaskManagerType methods for the dependency graph, staleness
  # computation and dependency queries.
  class TaskManagerType
    # Set by run_wave while worker fibers are executing a batch.
    @parallel_wave_active = false
    # How many runs are executing. Two can overlap in one process (an
    # auto cycle and a manual run_tasks waiting on the state lock).
    # While it is above zero the task set can't change.
    @run_active = 0

    # Add `input` to the inputs of the task registered as `task_key`.
    #
    # The supported way to grow a task's dependencies, including from
    # task procs on parallel workers. The graph, staleness and the
    # wave plan are computed before tasks run, so the new dependency
    # takes effect on the next run. During a parallel wave the
    # addition is queued and applied at the wave barrier.
    #
    # Returns false if the task already had the input. Raises
    # UnknownTaskError for an unregistered `task_key`, and CycleError
    # if `input` is one of the task's own keys.
    def add_input(task_key : String, input : String) : Bool
      @data_mutex.synchronize do
        task = tasks[task_key]?
        raise UnknownTaskError.new("Unknown task #{task_key}") unless task
        raise CycleError.new("Cycle detected: #{input} is a key of task #{task_key} itself") if task.keys.includes?(input)
        return false if task.inputs.includes?(input)

        if @parallel_wave_active
          # The coordinator iterates input sets during the wave
          @pending_inputs << {task_key, input}
        else
          task.inputs << input
          invalidate_graph_cache
        end
        true
      end
    end

    # add_input calls made during a parallel wave, applied in call
    # order by the coordinating fiber at the wave barrier.
    @pending_inputs = [] of {String, String}

    # Register a newly constructed task (called by Task.new).
    #
    # The task set is fixed while a run executes: runs read the
    # registries without a lock. Raises UsageError during a run. The
    # check and the registration happen under one lock acquisition,
    # so a run can't start in between.
    def register_task(task : Task, explicit_id : String?) : Nil
      @data_mutex.synchronize do
        reject_registry_mutation_during_run
        task.register_with_manager(explicit_id)
      end
    end

    # Remove the task registered as `task_key`: every key it is
    # registered under (one per output) and its id-index entry.
    # Use this instead of `tasks.delete`, which leaves tasks_by_id
    # stale so re-creating a task with the same id fails.
    #
    # Raises UnknownTaskError for an unknown key, and UsageError
    # during a run.
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

    # Mark the cached graph and input set stale. The autorun loop
    # checks @graph_invalidated to decide whether to re-run.
    def invalidate_graph_cache
      @graph_invalidated = true
      @all_inputs = nil
    end

    # Dependency graph as an adjacency hash (vertex => the vertices it
    # points at). The default block adds vertices on first touch.
    @graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
    @graph_sorted = [] of String
    # Task key => keys of the tasks that depend on it. Built by
    # propagate_staleness, reused by early cutoff.
    @reverse_deps = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }

    # The dependency graph and the task keys in topological order,
    # rebuilt when invalidated.
    def sorted_task_graph
      if @graph_invalidated || @graph.empty?
        @graph = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }
        @graph_sorted = [] of String
        @graph_invalidated = false
        @all_inputs = nil

        # Inputs that are not tasks hang off the virtual root
        all_inputs.each do |input|
          @graph[Croupier::ROOT_VERTEX] << input unless tasks.has_key? input
        end

        # Each input gets an edge into the task; tasks without inputs
        # hang off the virtual root
        tasks.each do |output, task|
          if task.@inputs.empty?
            @graph[Croupier::ROOT_VERTEX] << output
          end
          task.@inputs.each do |input|
            @graph[input] << output
          end
        end

        # Make every vertex a key, so leaves appear with empty sets
        @graph.values.flat_map(&.to_a).each do |vertex|
          @graph[vertex]
        end

        # The sorted list holds tasks only, not plain inputs
        @graph_sorted = Croupier.topological_sort(@graph).select { |v| tasks.has_key? v }
      end
      return @graph, @graph_sorted
    end

    # All inputs of all tasks, cached; nil means the cache is invalid.
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

    # Every input of the given targets and their dependencies. Raises
    # UnknownTaskError for an unknown target.
    def inputs(targets : Array(String))
      result = Set(String).new
      dependencies(targets).each do |task|
        result.concat tasks[task].@inputs
      end

      result
    end

    # The tasks needed to produce `outputs` (including themselves),
    # in execution order.
    def dependencies(outputs : Array(String))
      outputs.each do |output|
        if !tasks.has_key?(output)
          raise UnknownTaskError.new("Unknown output #{output}")
        end
      end
      result = _dependencies outputs
      sorted_task_graph[1].select(->(v : String) { result.includes? v })
    end

    # Single-output convenience overload.
    def dependencies(output : String)
      dependencies([output])
    end

    # Unsorted transitive closure of `outputs`, memoized so shared
    # dependencies are visited once.
    def _dependencies(outputs : Array(String))
      _dependencies_impl(outputs, {} of String => Set(String))
    end

    # The memo holds each node's own closure, never its siblings'.
    private def _dependencies_impl(outputs : Array(String), memo : Hash(String, Set(String)))
      result = Set(String).new
      outputs.each do |output|
        if memo.has_key?(output)
          result.concat memo[output]
          next
        end

        if tasks.has_key?(output)
          node_result = Set(String).new
          node_result << output
          node_result.concat(_dependencies_impl(tasks[output].@inputs.to_a, memo))
          memo[output] = node_result
          result.concat(node_result)
        end
      end
      result
    end

    # Outputs of every task that depends, directly or not, on `input`.
    def depends_on(input : String)
      depends_on [input]
    end

    def depends_on(inputs : Array(String))
      depends_on_impl(inputs, {} of String => Set(String), consumers_index)
    end

    # Input => tasks that consume it.
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

    # The memo holds each input's own closure (the outputs of its
    # consumers, plus theirs), never other inputs'.
    private def depends_on_impl(
      inputs : Array(String),
      memo : Hash(String, Set(String)),
      consumers : Hash(String, Array(Task)),
    )
      result = Set(String).new
      inputs.each do |input|
        if memo.has_key?(input)
          result.concat memo[input]
          next
        end

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

    # Compare inputs against the last run and leave the changed ones
    # in @modified for propagate_staleness. Three modes:
    #
    #   auto     the watcher reported what changed; hashing confirms
    #            it, so unchanged rewrites don't retrigger
    #   fast     mtime against the last run's scan start, no hashing
    #   content  content hashes against the last run's hashes
    #
    # `run_all` skips fast mode's mtime scan (every task re-runs
    # anyway). Content mode still scans, because @this_run feeds the
    # state file. `targets` limits the scan to those tasks' inputs;
    # other inputs keep their recorded hashes.
    def mark_stale_inputs(run_all : Bool = false, targets : Array(String)? = nil)
      @existing_files.clear
      # Saved in the state file: the next fast-mode run compares
      # mtimes against it. The state file's own mtime is written at
      # the end of the run and would hide inputs modified mid-run.
      @scan_started = Time.utc.to_unix_f
      if auto_mode?
        scan_auto_mode
      elsif @fast_mode
        scan_fast_mode(run_all, targets)
      else
        scan_content_mode(targets)
      end
    end

    # Auto mode: keep only the watcher-reported paths whose hash
    # differs from the last completed cycle. Events also fire on
    # identical rewrites, and a task regenerating a watched input
    # unchanged would otherwise retrigger itself forever. Paths with
    # no hash (deleted files) stay modified. last_run is maintained
    # in memory by the autorun loop, so the state file isn't read.
    private def scan_auto_mode : Nil
      @this_run = scan_inputs
      # Mutated in place: callers may hold the `modified` set
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

    # Seconds subtracted from the fast-mode baseline, absorbing
    # timestamp granularity and clock skew. The cost is sometimes
    # re-detecting an input modified just before the previous scan.
    FAST_MODE_GRACE = 1.0

    # Fast mode: an input is modified when its mtime is newer than the
    # last run's scan start. No content hashing at all.
    private def scan_fast_mode(run_all : Bool, targets : Array(String)?) : Nil
      kv_modifications = take_kv_modifications
      state_file_date = load_last_run
      # Keep the hashes recorded by content mode: fast mode can't
      # hash, and dropping them would make the next content-mode run
      # rebuild everything
      @this_run = @last_run.dup
      # Every task re-runs, so the mtime sweep would be wasted
      return if run_all
      # Baseline: the last run's scan start minus the grace window.
      # State files without __scan_time fall back to the file's own
      # mtime (mtime against mtime needs no grace).
      scan_started = if baseline = @last_scan_time
                       baseline - FAST_MODE_GRACE
                     else
                       state_file_date.to_unix_f
                     end
      # Stat outside @modified_lock, then insert in one batch:
      # holding the lock across a stat of every input would block
      # workers calling set() and modified?
      modified_now = [] of String
      scan_scope(targets).each do |file|
        if info = File.info?(file)
          modified_now << file if info.modification_time.to_unix_f > scan_started
        end
      end
      @modified_lock.synchronize do
        modified_now.each { |file| @modified << file }
        # Fast mode can't hash values: set()'s flags are its only
        # k/v change detection
        kv_modifications.each { |key| @modified << key }
      end
    end

    # Content mode: hash every input in scope and compare against the
    # last run's recorded hashes.
    private def scan_content_mode(targets : Array(String)?) : Nil
      # Discard set()'s k/v flags: content mode hashes k/v values
      # like files, and a kept flag would re-stale dependents on
      # every later run
      take_kv_modifications
      load_last_run
      scanned = scan_inputs(scan_scope(targets))
      # Keep hashes of inputs outside the scan scope, so a later full
      # run doesn't see them as modified
      @this_run = @last_run.merge(scanned)
      @modified_lock.synchronize do
        scanned.each do |file, sha1|
          @modified << file if last_run.fetch(file, "") != sha1
        end
      end
    end

    # Clear @modified and return the kv:// entries it held (set()
    # marks them, possibly during a previous run). Fast mode keeps
    # them; content mode discards them.
    private def take_kv_modifications : Array(String)
      @modified_lock.synchronize do
        kv = @modified.select(&.starts_with?("kv://"))
        @modified.clear
        kv
      end
    end

    # Load last_run (and @last_scan_time) from the state file, or
    # start empty without one. Returns the state file's mtime, fast
    # mode's fallback baseline.
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

    # Inputs of the given targets, or of every task.
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

    # Mark every task stale or fresh in one O(V+E) pass: find the
    # tasks stale on their own, then everything downstream of them.
    #
    # With run_all every task stays stale. Staleness then only orders
    # the run (dependents wait for stale dependencies), so the root
    # scan is skipped.
    def propagate_staleness(run_all : Bool = false)
      @reverse_deps.clear
      each_unique_task do |task|
        task.inputs.each do |input|
          if tasks.has_key?(input)
            @reverse_deps[input].concat(task.keys)
          end
        end
      end

      if run_all
        Log.debug { "run_all: skipping staleness root scan, all tasks stale" }
        each_unique_task(&.stale=(true))
        return
      end

      # Start from the keys of tasks stale on their own account
      stale_tasks = Set(String).new
      each_unique_task do |task|
        stale_tasks.concat(task.keys) if task.stale_on_own?
      end

      # Worklist walk; a cursor instead of shift, which is O(n)
      worklist = stale_tasks.to_a
      cursor = 0
      while cursor < worklist.size
        stale_output = worklist[cursor]
        cursor += 1

        @reverse_deps[stale_output].each do |dependent|
          unless stale_tasks.includes?(dependent)
            stale_tasks << dependent
            worklist << dependent
          end
        end
      end

      # A multi-output task's keys are stale or fresh together
      each_unique_task do |task|
        task.stale = task.keys.any? { |key| stale_tasks.includes?(key) }
      end

      Log.debug { "Propagated staleness: #{stale_tasks.size} stale, #{tasks.size - stale_tasks.size} fresh" }
    end

    # Yield each registered task once: `tasks` holds a multi-output
    # task under each of its outputs.
    private def each_unique_task(&)
      seen = Set(Task).new
      tasks.each_value do |task|
        yield task if seen.add?(task)
      end
    end

    # Raise UnknownInputsError unless every input is a kv:// key, a
    # task, or an existing file. With `targets`, only the inputs of
    # their dependency closure are checked.
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
