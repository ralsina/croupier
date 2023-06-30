# TODO

## Things it may make sense to add

* Instrument the concurrent runner using [Fiber Metrics](https://github.com/didactic-drunk/fiber_metrics.cr)
* Once it works fine with files, generalize to a k/v store using [kiwi](ihttps://github.com/crystal-community/kiwi)
* Use state machines for tasks (see veelenga/aasm.cr)
* Add a faster stale input check using file dates instead of hashes (like make)
* Add directory dependencies (depend on all files in the tree)
* Add wildcard dependencies (depend on all files / tasks matching a pattern)
* Implement failed state for tasks
* Implement -k -i make options (keep going / ignore errors)

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
