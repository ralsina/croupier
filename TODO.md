# TODO

## Things it may make sense to add

* Fix parallel `run_all` flag
* Implement parallel `keep_going` flag or reject it
* Instrument the concurrent runner using 
  [Fiber Metrics](https://github.com/didactic-drunk/fiber_metrics.cr)
* Add directory dependencies (depend on all files in the tree)
* Add wildcard dependencies (depend on all files / tasks matching a pattern)
* Mark tasks as stale if the OUTPUT is modified since last run
* Check for using RomainFranceschini/cgl instead of crystalline which seems buggy

* ~~Add a faster stale input check using file dates instead of hashes (like make)~~
* ~~Support a persistant k/v store~~
* ~~Once it works fine with files, generalize to a k/v store using [kiwi](https://github.com/crystal-community/kiwi)~~
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
