module Croupier
  # TaskManagerType methods for the dependency graph, staleness
  # computation and dependency queries.
  class TaskManagerType
    # How many runs are executing. Two can overlap in one process (an
    # auto cycle and a manual run_tasks waiting on the state lock).
    # While it is above zero the task set can't change.
    @run_active = 0

    # Add `input` to the inputs of the task registered as `task_key`.
    #
    # The supported way to grow a task's dependencies, including from
    # task procs on parallel workers. The graph and staleness are
    # computed before tasks run, so the new dependency takes effect on
    # the next run: during a run the addition is queued, and applied
    # when the last overlapping run ends.
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

        if @run_active > 0
          # Runs iterate input sets without a lock
          return @pending_inputs.add?({task_key, input})
        else
          task.@inputs << input
          invalidate_graph_cache
        end
        true
      end
    end

    # add_input calls made during a run, applied in call order when
    # the last overlapping run ends. A Set keeps insertion order and
    # drops repeated calls.
    @pending_inputs = Set({String, String}).new

    # Apply the queued add_input calls. Called with @data_mutex held,
    # once no run is active. Invalidating the graph is what tells the
    # autorun loop to re-watch and run again.
    private def apply_pending_inputs_locked : Nil
      return if @pending_inputs.empty?
      @pending_inputs.each do |task_key, input|
        # Set#<< ignores an input queued under two of a task's keys
        if task = tasks[task_key]?
          task.@inputs << input
        end
      end
      @pending_inputs.clear
      invalidate_graph_cache
    end

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

    # Remove the task registered as `task_key`, under every key it is
    # registered under (one per output) and its id.
    #
    # Raises UnknownTaskError for an unknown key, and UsageError
    # during a run.
    def remove_task(task_key : String) : Nil
      @data_mutex.synchronize do
        reject_registry_mutation_during_run
        task = tasks[task_key]?
        raise UnknownTaskError.new("Unknown task #{task_key}") unless task
        tasks.remove(task)
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

    # Task keys in dependency order; nil when not computed yet.
    @sorted_keys : Array(String)? = nil
    # Task key => keys of the tasks that depend on it. Built by
    # propagate_staleness, reused by early cutoff.
    @reverse_deps = Hash(String, Set(String)).new { |h, k| h[k] = Set(String).new }

    # Every task key, each after the keys of the tasks producing its
    # inputs. Cached until the graph is invalidated. Raises CycleError
    # when tasks depend on each other in a cycle.
    private def sorted_task_keys : Array(String)
      cached = @sorted_keys
      return cached if cached && !@graph_invalidated
      @graph_invalidated = false
      @sorted_keys = topological_order
    end

    # Kahn's algorithm over task keys: place every key whose producer
    # tasks are all placed, then repeat. Each round is sorted by name,
    # so the order is deterministic. Keys never placed are on or
    # behind a cycle.
    private def topological_order : Array(String)
      # Key => keys of the tasks consuming it, and key => how many of
      # its task's inputs are task keys not placed yet
      consumers = {} of String => Array(String)
      pending = {} of String => Int32
      tasks.each do |key, task|
        pending[key] = 0
        task.@inputs.each do |input|
          next unless tasks.has_key?(input)
          (consumers[input] ||= [] of String) << key
          pending[key] += 1
        end
      end

      order = [] of String
      ready = pending.select { |_, count| count == 0 }.keys.sort!
      until ready.empty?
        order.concat(ready)
        next_ready = [] of String
        ready.each do |key|
          consumers.fetch(key, nil).try &.each do |consumer|
            pending[consumer] -= 1
            next_ready << consumer if pending[consumer] == 0
          end
        end
        ready = next_ready.sort!
      end
      return order if order.size == pending.size

      raise CycleError.new("Cycle detected in the task graph: #{cycle_members(pending, consumers).join(", ")}")
    end

    # The keys Kahn's algorithm left unplaced, minus those that are
    # only downstream of a cycle: repeatedly drop keys with no
    # unplaced consumer, so what remains is on a cycle (or between
    # two). Sorted for a stable message.
    private def cycle_members(pending : Hash(String, Int32), consumers : Hash(String, Array(String))) : Array(String)
      left = pending.select { |_, count| count > 0 }.keys.to_set
      loop do
        sinks = left.select { |key| consumers.fetch(key, [] of String).none? { |consumer| left.includes?(consumer) } }
        break if sinks.empty?
        left.subtract(sinks)
      end
      left.to_a.sort
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
      # Sort first: it raises CycleError on a cycle, where the
      # recursive closure walk below would never terminate
      order = sorted_task_keys
      result = _dependencies outputs
      order.select { |key| result.includes?(key) }
    end

    # Single-output convenience overload.
    def dependencies(output : String)
      dependencies([output])
    end

    # Unsorted transitive closure of `outputs`, memoized so shared
    # dependencies are visited once.
    #
    # Both closure queries run through memoized_closure: `dependencies`
    # walks upstream along a task's inputs, `depends_on` walks
    # downstream along an input's consumers. The memo holds each
    # node's own closure, never its siblings'.
    private def _dependencies(outputs : Array(String))
      self_if_task = ->(node : String) { tasks.has_key?(node) ? [node] : [] of String }
      inputs_of = ->(node : String) { tasks[node]?.try(&.inputs.to_a) || [] of String }
      memoized_closure(outputs, {} of String => Set(String), self_if_task, inputs_of)
    end

    # Memoized transitive closure over `nodes`: each node contributes
    # `seed(node)` (upstream: the node itself when it is a task;
    # downstream: the task's outputs), then `edges(node)` recurse. The
    # memo holds each node's own closure, never its siblings'. Raises
    # CycleError if the edges lead back to a node still being walked:
    # both callers sort first (which raises with the cycle's members),
    # so this is a backstop for future callers.
    private def memoized_closure(
      nodes : Array(String),
      memo : Hash(String, Set(String)),
      seed : String -> Array(String),
      edges : String -> Array(String),
      visiting : Set(String) = Set(String).new,
    ) : Set(String)
      result = Set(String).new
      nodes.each do |node|
        if cached = memo[node]?
          result.concat cached
          next
        end
        raise CycleError.new("Cycle detected in the task graph: #{visiting.to_a.sort.join(", ")}") unless visiting.add?(node)

        node_result = Set(String).new
        node_result.concat(seed.call(node))
        node_result.concat(memoized_closure(edges.call(node), memo, seed, edges, visiting))
        visiting.delete(node)
        memo[node] = node_result
        result.concat(node_result)
      end
      result
    end

    # Outputs of every task that depends, directly or not, on `input`.
    def depends_on(input : String)
      depends_on [input]
    end

    def depends_on(inputs : Array(String))
      # Raises CycleError on a cycle, where the recursive walk would
      # never terminate
      sorted_task_keys
      consumers = consumers_index
      # Walk the downstream graph in task space: a task seeds its
      # outputs, and the next nodes are the tasks consuming any of
      # those outputs. Nodes are the task's REPRESENTATIVE REGISTRY
      # KEY, not its id: ids are not unique task identities (an
      # output-less task may reuse an output-producing task's id, and
      # generated ids hash the comma-joined outputs, so ["a,b"] and
      # ["a", "b"] collide), while the representative key resolves
      # back to exactly this task object in `tasks`.
      outputs_of = ->(key : String) { tasks[key]?.try(&.outputs) || [] of String }
      downstream = ->(key : String) { downstream_task_keys(key, consumers) }
      starts = inputs.flat_map { |input| consumers.fetch(input, NO_CONSUMERS) }
        .compact_map { |task| task.keys.find { |key| tasks[key]?.same?(task) } }
        .uniq!
      memoized_closure(starts, {} of String => Set(String), outputs_of, downstream)
    end

    # Registry keys of the tasks consuming any output of the task
    # registered as `key` (each task is represented by the first of
    # its keys that resolves back to it)
    private def downstream_task_keys(key : String, consumers : Hash(String, Array(Task))) : Array(String)
      task = tasks[key]?
      return [] of String unless task
      task.outputs
        .flat_map { |output| consumers.fetch(output, NO_CONSUMERS) }
        .compact_map { |consumer| consumer.keys.find { |k| tasks[k]?.same?(consumer) } }
        .uniq!
    end

    # Shared empty for consumers.fetch misses
    NO_CONSUMERS = [] of Task

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
