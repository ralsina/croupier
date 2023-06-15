# Thinks it may make sense to add:

* Use getters/setters/properties properly
* Tasks that operate in-place (output is also input)
* Instrument the concurrent runner using https://github.com/didactic-drunk/fiber_metrics.cr
* Maybe migrate to crotest or microtest (Nicer)
* Once it works fine with files, generalize to a k/v store using [kiwi](ihttps://github.com/crystal-community/kiwi)
* ~~Refactor the Task registry into its own class separate from Task itself~~
* ~~Make `Task.run` able to return `Array(String) | String | Nil` depending on number of outputs and handle it~~
* ~~Tasks with more than one output~~
* ~~Tasks without file output~~
* ~~More than one task with the same output~~
* ~~Run only tasks needed to produce specific outputs~~
* ~~Automate running crytic every now and then~~
