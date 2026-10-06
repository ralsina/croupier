# TODO

## Things it may make sense to add

* Instrument the scheduler (`RunPlan` / `WorkerPool`) using
  [Fiber Metrics](https://github.com/didactic-drunk/fiber_metrics.cr)
* Add wildcard dependencies (depend on all files / tasks matching a pattern)
* Mark tasks as stale if the OUTPUT is modified since last run. Today
  only a missing output makes a task stale, though output hashes are
  already recorded (`next_run`).
* Hash each file once per scan: a file under two directory inputs (or
  also declared directly) is hashed once per occurrence.
* Opt-in mtime+size hash reuse for content mode (see `#6` below for
  why it must not be the default)
* Bring back mutation testing: crytic ran in CI until 6a2d26f removed it

* ~~Allow a "shallow" mode for directory dependencies, which hashes just a list
  of contents and not the contents of the files themselves.~~ (`fast_dirs`)
* ~~Add directory dependencies (depend on all files in the tree)~~
* ~~Fix parallel `run_all` flag~~
* ~~Add a faster stale input check using file dates instead of hashes
  (like make)~~
* ~~Support a persistant k/v store~~
* ~~Once it works fine with files, generalize to a k/v store using
  [kiwi](https://github.com/crystal-community/kiwi)~~
* ~~Decide what to do in auto_run when no task has inputs~~
* ~~Implement -k make option (keep going)~~
* ~~Implement a "watchdog" mode~~ (`auto_run`)
* ~~Rationalize id/name/output thing~~
* ~~Make it fast again :-)~~ [Sort of]
* ~~Implement the missing parts of the parallel runner~~
* ~~Use getters/setters/properties properly~~
* ~~Restructure tests~~
* ~~Implement dry runs~~
* ~~Tasks that *always* run~~
* ~~Provide a way to ask to run tasks without outputs (needed for hacé)~~
* ~~Refactor the Task registry into its own class separate from Task
  itself~~ (`TaskRegistry`, a read-only view; `Task#inputs` is one too)
* ~~Make `Task.run` able to return `Array(String) | String | Nil`~~
  ~~depending on number of outputs and handle it~~
* ~~Tasks with more than one output~~
* ~~Tasks without file output~~
* ~~More than one task with the same output~~
* ~~Run only tasks needed to produce specific outputs~~
* ~~Investigate using Earl and proper agents/pools/etc~~ (a small
  `WorkerPool` covers it)

## Things that look like a bad idea, and why

* Use state machines for tasks (see veelenga/aasm.cr)

  In fact this is probably a good idea BUT the current implementation
  is fairly simple and seems to be mostly correct, so there is not much
  to be gained from the switch. Staleness is a single atomic
  tri-state (`Unknown`/`Stale`/`Fresh`).

* Maybe migrate to crotest or microtest (Nicer)

  While there are a number of test frameworks, the default spec one
  is ... OK. And I already have written a bunch of tests which I
  really don't want to redo.

  Maybe for another project.

* Tasks where output is also input (self-cyclical)

  This feel very hard to get right and maybe unnecessary. `Task.new`
  raises `CycleError` when an input is also one of the task's outputs.

  If the file is always preexisting, then the task should run
  every time, which can be handled by "always run" tasks

  If the file is created by another previous task t1, then this one
  will be merged into it, which means it doesn't need to have the
  input declared, and it will always run after t1, which looks ok.

* Implement failed state for tasks

  Not really needed: a failed task stays stale, and its dependents
  don't run in that run.

* ~~Use a pool of Fibers to run parallel tasks~~

  *(now done)* This used to be listed as a bad idea: an experiment in
  commit f3b3042c0cc3038360deac11269e07ffec0145a3 found limiting the
  number of fibers ~8x slower. Since #89, parallel runs keep one
  `WorkerPool` of up to `System.cpu_count` workers for the whole run (input
  hashing uses one too), and benchmarks, nicolino's included, show no
  slowdown.

* ~~Switch the topological sort to Kahn's algorithm (in-degree + queue)~~

  *(done in #85)* The old DFS missed cycles reachable from an input
  (#83), and Kahn detects every cycle in the same pass that orders the
  tasks. That outweighs the speed difference below, which the cache
  makes noise anyway.

  Measured 2026-08-14 on synthetic DAGs: the DFS was already O(V+E)
  and Kahn ~1.7–2.4x slower at every size (5k vertices/15k edges:
  1.3ms vs 2.3ms per sort; 100k/300k: 79ms vs 191ms), since it hashes
  more, and on string-keyed graphs hashing is the cost. But
  `sorted_task_keys` caches the result: the sort runs once per
  invalidated graph, not per task.

* ~~Use RomainFranceschini/cgl instead of crystalline~~

  *(moot)* There's no graph library anymore: the graph is a plain
  hash, and the algorithms were always ours.

## Codebase review findings (2026-08)

Findings from a thorough review. The high/correctness items (#1, #2) and
the clear performance wins (#5, #6, #7) all landed on main via
squash-merged PRs (#15–#20); the rest are recorded here for later.

### Bugs / correctness

* `#1` `use_persistent_store` never assigns `@_store_path`, so the "can't
  change path" guard is dead and a second call will crash casting a
  `FileStore` back to `MemoryStore`. Only works today because tests call
  `cleanup` between scenarios. *(fixed in #15)*
* `#2` `_dependencies_impl` and `depends_on_impl` cache the *accumulating*
  result set rather than each node's own closure, so memoized entries
  over-approximate on diamond/DAG shapes. Still safe (runs extra tasks,
  never too few) because `dependencies()` re-selects against the graph,
  but the memoization is incorrect as a per-node cache. *(fixed in #16)*
* ~~`#3` `sorted_task_graph` reaches into `@graph.@vertice_dict`
  (crystalline private ivar).~~ *(fixed: crystalline is gone)*
* `#4` `scan_inputs` reads every file in a watched directory with no size
  guard or error handling — one unreadable file or symlink loop crashes
  the run. *(the throw-on-unreadable behavior is intentional: an
  unreadable declared input is a real error, not something to skip.
  Directories are walked once, without following symlinks, so there's
  no symlink loop)*
* `#8` `@all_inputs` cache is only rebuilt when empty; it's cleared during
  graph rebuild today but a task added between build and scan could see a
  stale set. Make registration clear it explicitly. *(fixed)*
* ~~Concurrency: early-cutoff path mutates `other_task` state from worker
  fibers without synchronization~~ *(fixed: workers only execute tasks
  and report results; the coordinating fiber owns all bookkeeping
  (failures, stale transitions, early cutoff, `RunPlan`), so it needs
  no lock. State workers do share has its own lock: `@store_lock`
  (k/v store), `@hashes_lock` (run hashes), `@modified_lock`
  (`modified`); see `spec/parallel_stress_spec.cr`)*
* ~~`stale?` short-circuited to `true` forever for input-less and
  `always_run` tasks, so `waiting_for` never released their dependents
  and graphs rooted at such tasks were unrunnable ("Waiting for ...")~~
  *(fixed: `stale?` now trusts the assigned tri-state and only treats
  such tasks as always-stale while staleness is unknown)*
* ~~`@stale` / `@stale_atomic` are two sources of truth kept in sync by
  hand — drift-bug magnet.~~ *(fixed: single `Atomic(Staleness)` field
  with tri-state Unknown/Stale/Fresh; `stale`/`stale=`/`stale?` are
  views over it)*
* ~~`Task#run` rescues broadly and re-raises wrapped, obscuring the
  original backtrace.~~ *(fixed: proc failures raise `TaskFailure`
  with the original exception chained as `#cause`, message format
  unchanged)*

### Performance left on the table

* `#6` `scan_inputs` re-hashes every input file on every run (non-fast
  mode). *(addressed in #19: each path is stat'ed once, directories are
  walked once, files are hashed in parallel. mtime+size hash reuse was
  implemented and then REMOVED: it made non-fast mode trust metadata
  instead of file contents, which is fast mode's corner to cut —
  content mode's guarantee is that staleness always rests on hashed
  bytes. Any future attempt must be opt-in; it's listed above.)*
* `#7` Early-cutoff notification was O(V) per output → O(V²·outs) per
  run. *(fixed in #18: it reuses the `reverse_deps` map built in
  `propagate_staleness`)*
* ~~`#10` the parallel runner reallocated a `Channel` + `WaitGroup` per
  wave~~ *(moot: waves are gone since #89; one ready-queue scheduler
  keeps a `WorkerPool` for the whole run)*
* ~~`#11` the serial runner built intermediate arrays before a single
  iteration~~ *(moot: since #89 serial and parallel runs share one
  scheduler, and `run_all` runs every planned task in both)*

### Housekeeping

* `#5` `spec/testcases/empty/` leaves `file1`–`file5` + `.croupier`
  artifacts; `.gitignore` only covers `input*`/`output*`. Broaden the
  ignore. *(fixed in #17)*
* ~~`~2000` lines in `croupier_spec.cr` — consider splitting by
  topic.~~ *(done: split into `task_spec`, `task_manager_spec`,
  `features_spec` and others)*
* Typo in comment `croupier.cr:33`: "SAH1" → "SHA1". *(fixed)*
