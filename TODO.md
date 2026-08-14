# TODO

## Things it may make sense to add

* Instrument the concurrent runner using
  [Fiber Metrics](https://github.com/didactic-drunk/fiber_metrics.cr)
* Add wildcard dependencies (depend on all files / tasks matching a pattern)
* Mark tasks as stale if the OUTPUT is modified since last run
* Investigate using Earl and proper agents/pools/etc

* ~~Allow a "shallow" mode for directory dependencies, which hashes just a list
  of contents and not the contents of the files themselves.~~
* ~~Add directory dependencies (depend on all files in the tree)~~
* ~~Fix parallel `run_all` flag~~
* ~~Add a faster stale input check using file dates instead of hashes
  (like make)~~
* ~~Support a persistant k/v store~~
* ~~Once it works fine with files, generalize to a k/v store using
  [kiwi](https://github.com/crystal-community/kiwi)~~
* ~~Decide what to do in auto_run when no task has inputs~~
* ~~Implement -k make option (keep going)~~
* ~~Implement a "watchdog" mode~~
* ~~Rationalize id/name/output thing~~
* ~~Make it fast again :-)~~ [Sort of]
* ~~Implement the missing parts of the parallel runner~~
* ~~Make TaskManager a struct~~
* ~~Use getters/setters/properties properly~~
* ~~Restructure tests~~
* ~~Implement dry runs~~
* ~~Tasks that *always* run~~
* ~~Provide a way to ask to run tasks without outputs (needed for hacé)~~
* ~~Refactor the Task registry into its own class separate from Task itself~~
* ~~Make `Task.run` able to return `Array(String) | String | Nil`~~
  ~~depending on number of outputs and handle it~~
* ~~Tasks with more than one output~~
* ~~Tasks without file output~~
* ~~More than one task with the same output~~
* ~~Run only tasks needed to produce specific outputs~~
* ~~Automate running crytic every now and then~~

## Things that look like a bad idea, and why

* Use state machines for tasks (see veelenga/aasm.cr)

  In fact this is probably a good idea BUT the current implementation
  is fairly simple and seems to be mostly correct, so there is not much
  to be gained from the switch.

* Use a pool of Fibers to run parallel tasks

  The current implementation just launches as many fibers
  as it can. Experimental tests in commit
  f3b3042c0cc3038360deac11269e07ffec0145a3 showed that limiting
  the number of fibers is **much** slower (~8x slower).

  Since fibers are cheap, and the OS scheduler is good, it seems
  like just launching as much as possible is optimal.

  On the other hand, the parallel task runner DOES use
  something akin to a pool of fibers.

* Maybe migrate to crotest or microtest (Nicer)

  While there are a number of test frameworks, the default spec one
  is ... OK. And I already have written a bunch of tests which I
  really don't want to redo.

  Maybe for another project.

* Tasks where output is also input (self-cyclical)

  This feel very hard to get right and maybe unnecessary.

  If the file is always preexisting, then the task should run
  every time, which can be handled by "always run" tasks

  If the file is created by another previous task t1, then this one
  will be merged into it, which means it doesn't need to have the
  input declared, and it will always run after t1, which looks ok.

* Implement failed state for tasks

  Not really needed.

* Using RomainFranceschini/cgl instead of crystalline which seems buggy

  What can I say, it works, and the gains look marginal since cgl doesn't
  implement algorithms, which is the part I would like to avoid doing
  myself. Without that, crystalline is basically a glorified hash thing.

## Codebase review findings (2026-08)

Findings from a thorough review. The high/correctness items (#1, #2) and
the clear performance wins (#5, #6, #7) are being addressed on feature
branches; the rest are recorded here for later.

### Bugs / correctness

* `#1` `use_persistent_store` never assigns `@_store_path`, so the "can't
  change path" guard is dead and a second call will crash casting a
  `FileStore` back to `MemoryStore`. Only works today because tests call
  `cleanup` between scenarios. *(fixed on branch)*
* `#2` `_dependencies_impl` and `depends_on_impl` cache the *accumulating*
  result set rather than each node's own closure, so memoized entries
  over-approximate on diamond/DAG shapes. Still safe (runs extra tasks,
  never too few) because `dependencies()` re-selects against the graph,
  but the memoization is incorrect as a per-node cache. *(fixed on branch)*
* `#3` `sorted_task_graph` reaches into `@graph.@vertice_dict` (crystalline
  private ivar). Works but couples us to crystalline internals; an
  upstream rename would break the build opaquely.
* `#4` `scan_inputs` reads every file in a watched directory with no size
  guard or error handling — one unreadable file or symlink loop crashes
  the run. Also `Dir.glob` is called twice per directory. *(double-glob
  fixed in #19; the throw-on-unreadable behavior is intentional: an
  unreadable declared input is a real error, not something to skip)*
* `#8` `@all_inputs` cache is only rebuilt when empty; it's cleared during
  graph rebuild today but a task added between build and scan could see a
  stale set. Make registration clear it explicitly. *(fixed)*
* ~~Concurrency: early-cutoff path mutates `other_task` state from worker
  fibers without synchronization~~ *(fixed: parallel bookkeeping and
  TaskManager data access are now serialized — `@bookkeeping_mutex`
  guards worker bookkeeping / early-cutoff / stale transitions,
  `@data_mutex` guards the k/v store, `modified`, `next_run`, `last_run`;
  see `spec/parallel_stress_spec.cr`. Longer term, having workers send
  results over a Channel to a coordinating fiber that owns all
  bookkeeping would remove the locks.)*
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
  mode). Reuse hashes when `mtime+size` is unchanged, dedupe the double
  `Dir.glob`, and hash files in parallel (fiber-per-file batch like the
  task runner). *(addressed on branch)*
* `#7` Early-cutoff notification in `_run_tasks` / `_run_tasks_parallel`
  and `find_and_mark_dependents_fresh` is O(V) per output → O(V²·outs)
  per run. Reuse the `reverse_deps` map already built in
  `propagate_staleness`. *(addressed on branch)*
* `#9` `topological_sort` allocates per vertex; for very large graphs
  Kahn's algorithm (in-degree + queue) would be simpler and faster.
* `#10` `_run_tasks_parallel` reallocates a `Channel` + `WaitGroup` per
  wave; reusing the channel across waves would reduce churn.
* `#11` `_run_tasks` (serial) builds intermediate arrays via
  `compact_map` + `reject` before a single iteration — easy to fuse.

### Housekeeping

* `#5` `spec/testcases/empty/` leaves `file1`–`file5` + `.croupier`
  artifacts; `.gitignore` only covers `input*`/`output*`. Broaden the
  ignore. *(fixed on branch)*
* `~2000` lines in `croupier_spec.cr` — consider splitting by topic.
* Typo in comment `croupier.cr:33`: "SAH1" → "SHA1". *(fixed)*
