# Changelog

All notable changes to this project will be documented in this file.

## [0.17.1] - 2026-10-08

### 🐛 Bug Fixes

- Early cutoff no longer skips a task whose input changed earlier in
  the same run. When one producer finished with changed outputs and
  another producer of the same task finished later with unchanged
  outputs, the recompute forgot the first change and the task was
  marked fresh. In nicolino this left `search.json` and `sitemap.xml`
  stale after editing a post. (#93)

## [0.17.0] - 2026-10-06

### ⚠️ Breaking Changes

- The master/subtask experiment is gone: `master_task:`, `subtask_ids`,
  `register_subtask`, `remove_subtask(s)` and `link_subtask` (including
  0.16.0's `remove_subtask`) are removed. To track a changing set of
  files, rebuild the task graph instead (see the next item)
- The task set is fixed while a run executes: `Task.new` and the new
  `TaskManager.remove_task` raise `UsageError` during a run, including
  from task procs. Build the graph before running; in auto mode, stop
  with `auto_stop`, rebuild and call `auto_run` again. Dependencies can
  still be discovered at runtime with `add_input`
- `add_input` called during a run is queued and applied when the last
  overlapping run ends (previously: immediately in serial runs, at the
  wave barrier in parallel ones), so the new dependency takes effect on
  the next run. It still returns false for an input the task already
  has, and raises `UnknownTaskError`/`CycleError` as before
- `TaskManager.tasks` is a read-only `TaskRegistry`: it keeps every
  read (`[]`, `[]?`, `fetch`, `has_key?`, `keys`, `values`, `size`,
  `empty?`, `each*`, `Enumerable`) but no Hash writes, so `tasks[k] =`,
  `tasks.delete` and friends no longer compile. `tasks=` is gone too.
  Use `Task.new` and `TaskManager.remove_task`
- `TaskManager.tasks_by_id` is removed; use `TaskManager.tasks.by_id?(id)`
- `Task#inputs` returns a read-only view (`Enumerable(String)`,
  `includes?`, `size`, `empty?`); `inputs <<`/`inputs.add` call
  `add_input`, so existing `inputs << x` code keeps compiling and is now
  safe during runs. `inputs=` is gone
- Every dependency cycle raises `CycleError`, naming only the tasks on
  the cycle. A cycle reachable from an input used to be sorted into an
  order that broke it (failing as "Waiting for ..." or, for targeted
  runs and `depends_on`, overflowing the stack). The deprecated
  top-level `topological_sort` is removed
- Serial and parallel runs share one scheduler, and parallel runs no
  longer proceed in waves; `TaskManager.file_exists?` is removed
- `Task.new` has two overloads (block and `proc:`) instead of four.
  Every existing call form still works (an array, a single string or
  nil as the first argument, or `output:` by name), but passing both
  `output:` and `outputs:` now raises `TaskDefinitionError`

### 🚀 Features

- `TaskManager.remove_task(key)` removes a task under every key and its
  id, so re-creating a task with the same id works
- `TaskManager.tasks.by_id?(id)`
- The `proc:` form of `Task.new` accepts `mutex:` (it was block-only)

### 🐛 Bug Fixes

- Auto mode on Linux: removing a watched input's whole directory no
  longer kills the inotify handler (which silently stopped all later
  change detection); nothing that runs in the handler, user callbacks
  included, can kill it anymore
- The Linux watcher's `watch` and editor-replacement re-watch can no
  longer race `close` into watching a closed descriptor
- Auto mode watches inputs added by `add_input` during a cycle
- Re-registering a mutex name keeps the existing lock instead of
  replacing one a running task may hold
- `depends_on` no longer confuses tasks whose ids collide (an
  output-less task reusing another task's id, or generated ids of
  `["a,b"]` and `["a", "b"]`)
- Overlapping runs in one process (an auto cycle and a manual
  `run_tasks`) are counted, so the last one to finish reopens task
  creation, and the run check and registration happen under one lock

### ⚡ Performance

- One dependency-counting ready-queue scheduler replaces the serial
  runner and the parallel wave runner: a task starts as soon as its
  producers finish, so a slow task no longer holds back unrelated
  ready ones (about 8–13% faster on mixed-duration parallel graphs).
  Inputs another task creates as a side effect are waited for instead
  of failing the run
- Staleness is evaluated once per task instead of once per output

### 🚜 Refactor

- The inotify and kqueue watchers share one interface
  (`new(on_change)`, `watch`, `close`)
- One worker-pool helper serves the scheduler and parallel input hashing
- One memoized closure helper backs `dependencies` and `depends_on`
- Duplicated code in `Task`, the runners and the graph is gone

### 🧪 Testing

- The work-stealing timing spec compares equal amounts of work (its
  parallel half used to rerun the serial tasks too), fixing a flake
  on small CI runners

### 📚 Documentation

- Source comments rewritten for accuracy; TODO.md matches the code

## [0.16.0] - 2026-10-05

### ⚠️ Breaking Changes

- Every deliberate exception now derives from `Croupier::Error`
  (`TaskDefinitionError`, `CycleError`, `UnknownTaskError`,
  `UnknownInputsError`, `TaskVerificationError`, `TaskFailure`,
  `RunFailure`, `UnreachableTaskError`, `UsageError`): the sites that
  used to raise anonymous `RuntimeError`s raise typed errors now.
  Messages are unchanged (the three identical "Cycle detected" raises
  gained context after the shared prefix), and a single
  `rescue ex : Croupier::Error` catches everything the library raises
  on purpose
- Real runs serialize on a cross-process flock (`.croupier.lock` next
  to the state file): two croupier processes in one directory now wait
  for each other instead of last-writer-wins on the state. The lock
  file is never unlinked and should be ignored by your VCS. A run may
  not be reentrant: starting another run from inside a task proc is
  not supported

### 🚀 Features

- Auto mode on macOS, backed by a kqueue watcher; the Linux inotify
  behavior and the task-manager API are unchanged
- Typed error hierarchy: callers can rescue by type instead of matching
  messages, and each cycle detector is distinguishable by its message
  context
- `TaskManager.remove_subtask(id)`: removes a subtask from every
  registry view (tasks, tasks_by_id and any master's tracking) — the
  supported way to do per-file subtask cleanup. Deleting from
  `TaskManager.tasks` directly leaves a stale `tasks_by_id` entry that
  then rejects re-registering the same deterministic subtask id

### 🐛 Bug Fixes

- Autorun: a programming error (in a `before_run_hook` or in croupier
  itself) is logged at error level with its backtrace, instead of
  masquerading as the routine "input not there yet" retry
- Autorun: kv changes marked by `set()` while a cycle's run was
  executing are no longer silently consumed by the end-of-cycle
  cleanup — the change re-runs on the next cycle
- `auto_stop` is safe under concurrent callers: exactly one runs the
  shutdown handshake, every caller returns only after the watcher is
  torn down, and a subsequent `auto_run` cannot race the old fiber's
  cleanup
- `scan_inputs` no longer silently drops fifo/socket/device inputs;
  they are hashed by metadata so they can be detected as modified
- Fast mode no longer holds `@modified_lock` across the per-input
  stat sweep
- The kv store migration in `use_persistent_store` goes through the
  public Kiwi API under `@store_lock`, instead of reaching into store
  internals while task workers may be writing
- Subtask registration and removal are wave-safe: during parallel runs
  the operations are queued and applied at the wave barrier in call
  order, and a subtask removed by an earlier wave can no longer run in
  a later wave
- State-file robustness: written state is fsynced before the atomic
  rename; a non-string state entry triggers a full rebuild instead of
  being coerced into garbage hashes; `auto_stop`'s running flag and
  the watcher slot are properly synchronized
- `all_inputs` caches on an explicit invalidation instead of
  emptiness, so task sets without inputs stop rescanning on every call
- Watch failures in auto mode are retried with backoff and categorized
  by loudness (quiet for missing inputs, warning for failed tasks,
  error with backtrace for bugs)

### ⚡ Performance

- `scan_inputs` classifies each input path with a single `File.info?`
  instead of three stat-based calls

### 🚜 Refactor

- The staleness scan's five-flag matrix is now one dispatcher over
  three self-contained mode methods (auto / fast / content) with named
  shared helpers, each carrying only the state it needs

### 🧪 Testing

- Coverage for parallel progress callbacks, auto mode over a persistent
  store, subtask wave deferral, and the fiber-count assertions no
  longer count stdlib thread noise (GC markers and thread-pool fibers),
  which made the parallel-run specs flaky on loaded machines

### 📚 Documentation

- The linting docs match the Makefile's ameba target, and the README's
  master-task example uses the supported `remove_subtask` API

<!-- generated by git-cliff -->

## [0.15.0] - 2026-09-05

### ⚠️ Breaking Changes

- `run_tasks(keep_going: true)` now raises `Croupier::RunFailure` at the
  end of the run when any task failed; failures used to be only logged.
  The run still completes everything it can and saves its state first,
  and `RunFailure#errors` carries every failure with its cause chained
- A failed run now raises `Croupier::RunFailure` for serial runs too
  (serial used to raise the raw `TaskFailure`, parallel a plain
  `RuntimeError`). Messages are unchanged, and the original failures
  are available from `RunFailure#errors`
- `TaskManager.previous_output_hash` was removed; it had shipped in
  0.14.3 and was immediately superseded by
  `TaskManager.swap_output_hash`
- Unsatisfiable-input failures raise `Croupier::UnknownInputsError`
  instead of a plain `RuntimeError`; the message is unchanged
- The task graph's root sentinel is now `Croupier::ROOT_VERTEX` instead
  of the literal `"start"`, so a task or file named "start" no longer
  collides with the root; consumers inspecting `sorted_task_graph`'s
  adjacency hash must use the constant
- `hash_directory` now hashes the real contents of directory inputs
  whose names contain glob metacharacters, or that are reached through
  a symlink; those inputs rebuild exactly once on the first run after
  upgrading

### 🚀 Features

- Report collected failures at the end of keep_going runs
- Raise UnknownInputsError for unsatisfied task inputs

### 🐛 Bug Fixes

- Reset fast_dirs in TaskManager cleanup
- Treat non-mapping state files as corrupt instead of crashing
- Take directory names literally when hashing directory inputs
- Stop misdiagnosing directory creation failures in save_file_output
- Synchronize queued_changes between watcher and autorun fibers
- Detect non-mapping state files explicitly instead of rescuing Exception
- Raise RunFailure for serial run failures too
- Rename the graph root so a vertex named start can't collide
- Use a PID-suffixed temp file for the state file save
- Report hashing failures instead of hanging the run
- Stop the autorun fiber before wiping the registry in cleanup
- Record a real scan time in auto mode
- Lock every access to the modified set

### 🚜 Refactor

- Simplify mutex handling in Task#call_procs
- Decompose auto_run and watch into focused methods
- Extract runnable?, run_one and next_batch from the runners
- Move topological_sort into the Croupier module

### 📚 Documentation

- Document the concurrency contract of the run-hash trio
- Document the failure model in the README
- Remove broken mutation testing badge
- Refresh CLAUDE.md and fix a README typo

### ⚡ Performance

- Walk the staleness worklist with a cursor instead of shift

### 🧪 Testing

- Add wait_until and auto_cycle_settled? spec helpers
- Replace fixed sleeps in auto_run specs with condition waits
- Wait on conditions instead of sleeping in features specs
- Prove parallelism relatively instead of wall-clock budgets
- Tolerate duplicate inotify events in auto_run specs
- Hold cleanup accountable for every mode flag and cache
- Give the unwatched-input settle check a bounded wait

### ⚙️ Miscellaneous Tasks

- Replace mdl with markdownlint-cli2
- Make markdownlint-cli2 behave the same standalone and in the hook
- Pin the crystal toolchain version
- Target main in the pull_request trigger
- Run ameba and markdownlint on every push and PR
- Upgrade to ameba 1.7.0 and fix its new violations

## [0.14.3] - 2026-08-30

### 🚜 Refactor

- Split croupier.cr, decompose complex methods and split the spec monolith

<!-- Entries below generated by git-cliff (see cliff.toml). -->
<!-- Earlier hand-written history (0.5.4 and below) is preserved further down. -->

## [0.14.2] - 2026-08-27

### 🐛 Bug Fixes

- `no_save` tasks with a directory output no longer fail with
  `read (<dir>): Is a directory`: directory outputs are hashed with the
  same Merkle-tree digest used for directory inputs, so dependents of
  the directory stay fresh across runs and re-stale only when its
  contents change

## [0.14.1] - 2026-08-21

### 🐛 Bug Fixes

- Only log early cutoff when a dependent is actually notified

### ⚡ Performance

- Reduce @data_mutex traffic from the 0.14 contention report
- Split @data_mutex by concern and index tasks by id
- Micro-optimizations for hashing, k/v writes and inotify matching (#48)

## [0.14.0] - 2026-08-18

Results of a full correctness-and-performance review of the codebase:
24 issues filed, investigated and fixed (GH issues #23-#46).

### 🚀 Features

- Add `TaskManager.add_input` for growing task dependencies between runs:
  thread-safe, invalidates the input/graph caches, documented as
  effective on the next run only (`tasks` and `Task#inputs` are now
  documented read-only during runs)
- Tasks declared with the single-output block initializer accept a
  `mutex:` parameter (the array form always did, though it never
  actually locked; see below)
- `TaskManager.swap_output_hash` records an output hash and returns
  the previous one in a single locked step; `TaskManager.file_exists?`
  is a per-run positive cache for file existence

### 🐛 Bug Fixes

- `run_tasks(targets)` no longer skips the topological sort when the
  target list merely has the right size: `auto_run` could wedge in a
  permanent retry loop, and unknown targets were silently dropped (#23)
- Dry runs no longer persist input hashes, so they no longer consume
  the changes they report on (#24)
- Failed or blocked tasks no longer consume their input changes:
  with `keep_going` they are retried on the next run instead of never
  again (#25)
- Dependents of failed tasks never run, in both runners: parallel
  marked failed tasks done (dependents ran against missing outputs),
  serial aborted the whole run on the first blocked task despite
  `keep_going` (#26)
- `waiting_for` reads the k/v store through the mutex-guarded
  accessor instead of racing parallel workers on the raw store (#27)
- The state file is written atomically (tmp + rename), carries a
  schema version, and a corrupted file is recovered by rebuilding
  instead of raising forever (#28)
- Fast mode preserves recorded input hashes and detects inputs
  modified mid-run (scan-start timestamps with a one-second grace
  window) (#29)
- Auto mode staleness is content-based: early cutoff works across
  cycles and a task rewriting a watched input with identical content
  no longer re-triggers forever (#30)
- `auto_run` re-watches when master tasks create subtasks mid-session,
  so changes to the new inputs are no longer invisible (#31)
- Targeted runs report missing inputs as "Unknown inputs" up front
  (auto mode keeps building whatever is buildable) and the auto retry
  loop backs off to one attempt per second (#32)
- `cleanup` stops the autorun fiber and resets session state (hooks,
  mutexes, state file, early cutoff) (#33)
- Merging tasks no longer silently drops the merged task's mutex and
  subtask ids (differing mutexes are refused), and the block
  initializer's `mutex:` parameter actually locks now (#34)
- A multi-way merge that fails partway no longer corrupts the
  registry: compatibility is validated across the whole set first (#35)
- `add_input` from parallel workers is queued and applied at wave
  boundaries instead of mutating input sets while the coordinator
  iterates them (#36)
- Task declaration semantics: input/output paths are normalized,
  empty `kv://` keys are rejected, dotfiles count in directory
  hashes, computed ids are 48 bits, explicit ids on output-ful tasks
  are unique, and surplus proc results log a warning (#37)
- `topological_sort` distinguishes cycles from unreachable vertices
  and visits siblings in a deterministic order (#38)
- k/v staleness is value-based: same-value `set`s don't re-stale
  dependents, and one-time changes no longer re-stale them forever (#39)
- Worker fibers terminate when their work queue drains instead of
  parking forever (each parallel wave and input scan leaked fibers) (#40)

### ⚡ Performance

- Targeted runs scan only the executed tasks' inputs, instead of
  re-hashing the whole registry (and consuming unrelated input
  changes) (#42)
- Read-through cache for the k/v store: hits do no IO under the data
  mutex, which matters on slow or network filesystems (#41)
- Single-pass staleness computation (the file/kv output partition is
  fused with early exit) and one locked round-trip per output hash (#43)
- Small wins: consumers index for `depends_on`, graph-rebuild flag off
  the k/v store, streaming input hashing, dead work removed (#44)
- Input file existence is cached per run; staleness scans are skipped
  under `run_all`

### ⚙️ Miscellaneous Tasks

- Merge `perf/skip-scan-on-run-all`; delete nine branches already in
  main via squash merges; TODO.md annotations updated (#45)
- Spec coverage gaps closed: YAML round-trip, progress_callback,
  no_save with kv outputs, and the `with_scenario` helper is
  deduplicated (#46)

## [0.13.0] - 2026-08-14

### 🚀 Features

- Task proc failures now raise `Croupier::TaskFailure` with the original
  exception chained as `#cause`, preserving its type and backtrace
  (message format unchanged)

### 🐛 Bug Fixes

- Fix parallel builds aborting with a Boehm GC "duplicate large block
  deallocation" or segfaulting under load, most reliably when tasks
  failed: the coordinating fiber now owns all run bookkeeping and
  TaskManager data access is lock-guarded
- Fix task graphs rooted at input-less or `always_run` tasks being
  unrunnable ("Can't run tasks: Waiting for ...") in both serial and
  parallel modes
- Fix latent compile error for tasks declared with `mutex:`

### 🚜 Refactor

- Drop the crystalline dependency; the task graph is a plain adjacency
  hash
- Task staleness is a single atomic field (`stale`/`stale?` behavior
  unchanged)

### ⚙️ Miscellaneous Tasks

- Require Crystal 1.20+ (Sync multithreading API)

## [0.12.4] - 2026-08-14

### 🐛 Bug Fixes

- Make run_all force fresh tasks and compile on older Crystal

## [0.12.3] - 2026-08-13

### 🐛 Bug Fixes

- Require Crystal 1.21+ for the parallelism resize

### ⚙️ Miscellaneous Tasks

- Ignore .zcode session directory

<!-- generated by git-cliff -->

## [0.12.2] - 2026-08-13

### ⚡ Performance

- Enable real parallelism for parallel task execution

<!-- generated by git-cliff -->

## [0.12.1] - 2026-08-13

### 🐛 Bug Fixes

- Record store path in use_persistent_store (#15)
- Invalidate all_inputs on task change; minor cleanups (#20)

### 🚜 Refactor

- Cache each node's own closure in _dependencies/depends_on (#16)

### ⚡ Performance

- Parallel hashing + hash-of-hashes directories (#19)
- Reuse reverse_deps map for early-cutoff scans (#18)

### 🎨 Styling

- Fix ameba lint violations

### ⚙️ Miscellaneous Tasks

- Ignore empty testcase scratch artifacts (#17)

## [0.12.0] - 2026-03-04

### 🚀 Features

- Make inotify dependency Linux-only
- Add non-Linux stub for auto_run with helpful error

### 🎨 Styling

- Remove unnecessary ameba disable directives

## [0.11.0] - 2026-02-26

### 🚀 Features

- Make state file path configurable

## [0.10.0] - 2026-02-25

### 🚀 Features

- Add early cutoff optimization for task execution

## [0.9.1] - 2026-02-24

### 🐛 Bug Fixes

- K/v store modifications now properly trigger task re-execution

### ⚙️ Miscellaneous Tasks

- Lint

## [0.9.0] - 2026-01-30

### 🚀 Features

- Add hierarchical (master/subtask) tasks feature

## [0.8.5] - 2026-01-26

### 🚀 Features

- Add before_run_hook for auto mode preparation tasks

## [0.8.3] - 2026-01-26

### ⚙️ Miscellaneous Tasks

- Reduce verbose logging in auto mode

## [0.8.1] - 2026-01-26

### 🐛 Bug Fixes

- Resolve auto mode not detecting file changes after editor replacement

## [0.8.0] - 2026-01-21

### ⚡ Performance

- Add memoization to _dependencies and depends_on to fix exponential blowup
- Eliminate redundant dependencies() calls and skip for build-all case

## [0.7.0] - 2026-01-15

### 🚀 Features

- Implement O(V+E) staleness propagation to replace expensive recursive checks

## [0.6.0] - 2026-01-13

### 🚀 Features

- Replace static chunking with work-stealing parallel algorithm (#14)

### 🐛 Bug Fixes

- Yield correctly so tasks actually parallelize over threads/fibers
- Resolve test blocking with nonblock inotify

### 📚 Documentation

- Fix markdown linting issues in CLAUDE.md and TODO.md

### ⚙️ Miscellaneous Tasks

- Inotify latest release has the nonblocking patch

---

## New in 0.5.4

* Handle race condition creating folders, makes parallel more reliable

## New in 0.5.3

* Fix #12 where tasks didn't keep their stale status after dependencies ran first

## New in 0.5.2

* Auto mode works for watched directories

## New in 0.5.1

* Add support for tasks depending on directories
* New `fast_dirs` mode in TaskManager for cheaper directory checks
* Add progress callback
* Add support for passing blocks to Tasks:

```crystal
Croupier::Task.new output: "fileA", inputs: ["input.txt"] do
  puts "task1 running"
  File.read("input.txt").downcase
end
```

## New in 0.5.0

* New `TaskManager.auto_mode?` property
* Yield on spawn to make concurrency more useful
* More efficient input handling
* Fix deadlocking bug in parallel runner
* Warn or error when tasks are next in line but not ready

## Version 0.4.1

* Add `mergeable` flag for tasks (default true)
* Added `TaskManager.depends_on` function

## Version 0.4.0

* Add trace level debug about why tasks run
* Implement k/v data as input/output for tasks
* Implement *persistent* k/v store
* Implement `fast_mode` for TaskManager, where it checks file
  timestamps instead of contents to decide if they should
  trigger tasks.
* Better logs

## Version 0.3.4

* Fixed bug saving .croupier, was missing all inputs
* Added tests for `TaskManager.save_run`
* Fixed bug in inotify watcher path lookup
* Fixed bug where auto_run would only run tasks once

## Version 0.3.3

* Implemented keep-going flag

## Version 0.3.2

* Add support for running only some tasks in auto_run
* Implemented `TaskManager.inputs` to get the inputs for
  a given list of targets.
* Implemented `TaskManager.stop_watch` and watcher cleanup
* Uncommented skipping run if queued changes are empty in `auto_run`
* Added support for calling `watch` only for the dependencies of
  specific targets
* Simpler one-watcher implementation of watch
* Only react to specific Inotify flags in watch

## Version 0.3.1

* Added auto_run / auto_stop that control a "watchdog" fiber that
  automatically runs tasks if their dependencies change.

## Version 0.3.0

* Removed name parameter
* Made autogenerated `Task.@id` shorter
* Implemented more complex task merging strategy (see
  "complex merge" in the spec)

## Version 0.2.3

* Made `TaskManager.all_inputs` more efficient
* Call `TaskManager.sorted_tas_graph` less and made it faster
* Make @id generation simpler

## Version 0.2.2

* Tasks without inputs should be treated like always_run tasks
  (found via bug in Hacé)
* Tasks without inputs and multiple outputs should not run twice
  (found via bug in Hacé)
* Made TaskManager an instance of a struct, and lost all the
  class variables, simplifying code.
* Removed some small methods from TaskManager.

## Version 0.2.1

* Fix bug that triggered too many builds in some cases in
  [nicolino](https://github.com/ralsina/nicolino)
* Bring the parallel runner up to date with the serial one,
  including equivalent tests

## Version 0.2.0

* Tasks are mostly YAML serializable (procs can't be serialized)
* Task class uses properties instead of instance variables
* TaskManager is now a struct
* Renamed argument `output` to `outputs` in `Task.initialize` where
  it makes sense.
* Fixed bug merging tasks with multiple outputs
* Fixed bug merging tasks with different flags
* Added missing always_run flag to overloaded `Task.initialize`

## Version v0.1.8

* Added `always_run` flag for tasks that run even if their dependencies
  are unchanged.
* Added `dry_run` flag for run_tasks, which will not actually run the
  procs.

## Version v0.1.7

* Improve handling of tasks without outputs.
  They now have an ID they can be referred by.

## Version v0.1.6

* Improved handling of Proc return types.

## Version v0.1.5

* Support tasks that generate multiple outputs
* Support tasks that generate no output
* Forbid merging tasks with different `no_save` settings
* Minor change in semantics of tasks with the same output

## Version v0.1.4

* Support multiple tasks with same output, which will be executed in creation order.
* Fail to run if any tasks depend on inputs that don't exist and are not outputs.

## Version v0.1.3

This is what it is, no records :-)
