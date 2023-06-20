# Changelog

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
