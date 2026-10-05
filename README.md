# Croupier

Croupier is a smart task definition and execution library, which
can be used for [dataflow programming](https://en.wikipedia.org/wiki/Dataflow_programming).

[![Docs](https://github.com/ralsina/croupier/actions/workflows/static.yml/badge.svg)](https://ralsina.github.io/croupier/)
[![License](https://img.shields.io/badge/License-MIT-green)](https://github.com/ralsina/croupier/blob/main/LICENSE)
[![Release](https://img.shields.io/github/release/ralsina/croupier.svg)](https://GitHub.com/ralsina/croupier/releases/)
[![News about Croupier](https://img.shields.io/badge/News-About%20Croupier-blue)](https://ralsina.me/categories/croupier.html)

[![Tests](https://github.com/ralsina/croupier/actions/workflows/ci.yml/badge.svg)](https://github.com/ralsina/croupier/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ralsina/croupier/branch/main/graph/badge.svg?token=YW23EDL5T5)](https://codecov.io/gh/ralsina/croupier)

## What does it mean

You use Croupier to define tasks. Tasks have:

* An id
* Zero or more input files or k/v store keys
* Zero or more output files or k/v store keys
* A `Proc` that consumes the inputs and returns a string
* After the `Proc` returns data which is saved to the output(s)
  unless the task has the `no_save` flag set to `true`, in which
  case it's expected to have already saved it.

  **Note:** the return value for procs depends on several factors, see below.
  **Note:** A reference to a k/v key is of the form `kv://mykey`

And here is the fun part:

Croupier will examine the inputs and outputs for your tasks and
use them to build a dependency graph. This expresses the connections
between your tasks and the files on disk, and between tasks, and **will
use that information to decide what to run**.

So, suppose you have `task1` consuming `input.txt` producing
`fileA` and `task2` that has `fileA` as input and outputs `fileB`.
That means your tasks look something like this:

```mermaid
  graph LR;
      id1(["📁 input.txt"])-->idt1["⚙️ task1"]-->id2(["📁 fileA"]);
      id2-->idt2["⚙️ task2"]-->id3(["📁 fileB"]);
```

Croupier guarantees the following:

* If `task1` has never run before, it *will run* and create `fileA`
* If `task1` has run before and `input.txt` has not changed, it *will not run*.
* If `task1` has run before and `input.txt` has changed, it *will run*
* If `task1` runs, `task2` *will run* and create `fileB`
* `task1` will run *before* `task2`

That's a very long way to say: Croupier will run whatever needs
running, based on the content of the dependency files and the
dependencies between tasks. In this example it may look silly
because it's simple, but it should work even for thousands of
tasks and dependencies.

The state between runs is kept in `.croupier` so if you delete
that file all tasks will run.

Further documentation at the [doc pages](https://ralsina.github.io/croupier/)

### Notes

### Notes about proc return types

* Procs in Tasks without outputs can return nil or a string,
  it will be ignored.

* Procs with one output and `no_save==false` should return a
  string which will be saved to that output.

  If `no_save==true` then the returned value is ignored.

* Procs with multiple outputs and `no_save==false` should
  return an `Array(String)` which will be saved to those outputs.

  If `no_save==true` then the returned value is ignored.

### No target conflicts

If there are two or more tasks with the same output they will be
merged into the first task created. The resulting task will:

* Depend on the combination of all dependencies of all merged tasks
* Run the procs of all merged tasks in order of creation

### Tasks without output

A task with no output will be registered under its `id` and is not expected
to create any output files. Other than that, it's just a regular task.

### Tasks with multiple outputs

If a task expects the TaskManager to create multiple files, it
should return an array of strings.

## Dynamic Task Creation

Tasks can create other tasks from inside their procs. This is handy
when the set of tasks is only known at runtime (e.g. one render task
per file in a folder):

```crystal
Task.new(inputs: ["content/"]) do
  Dir.glob("content/**/*.md").each do |md_file|
    output_file = md_file.sub("content", "output").sub(".md", ".html")
    Task.new(inputs: [md_file], outputs: [output_file]) do
      Markd.to_html(File.read(md_file))
    end
  end
  nil
end
```

Notes:

1. **Run twice (or use auto mode)**: the first run creates the tasks;
   the second runs them. Auto mode detects the graph change and runs
   again automatically.
2. **Safe in parallel runs**: task creation and `add_input` calls
   from worker fibers are deferred to the end of the current wave and
   applied by the coordinator, so the registries are never mutated
   while other workers read them.
3. **Same id merges**: creating a task whose id or outputs collide
   with an existing one merges the definitions (procs are appended).

For a complete working example, see `examples/ssg/`.

## Installation## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     croupier:
       github: ralsina/croupier
   ```

2. Run `shards install`

## Usage

This is the example described above, in actual code:

```crystal
require "croupier"

Croupier::Task.new(
  output: "fileA",
  inputs: ["input.txt"],
) {
  puts "task1 running"
  File.read("input.txt").downcase
}

Croupier::Task.new(
  output: "fileB",
  inputs: ["fileA"],
) do
  puts "task2 running"
  File.read("fileA").upcase
end

Croupier::TaskManager.run_tasks
```

If we create a `input.txt` file with some text in it and run this
program, it will print `task1 running` and `task2 running` and
produce `fileA` with that same text in lowercase, and `fileB`
with the text in uppercase.

The second time we run it, it will *do nothing* because all tasks
dependencies are unchanged.

If we modify `index.txt` or `fileA` then one or both tasks
will run, as needed.

## Auto Mode

Besides `run_tasks`, there is another way to run your tasks,
`auto_run`. It will run tasks as needed, when their input
files change. This allows for some sorts of "continuous build"
which is useful for things like web development.

You start the auto mode with `TaskManager.auto_run` and stop
it with `TaskManager.auto_stop`. It runs in a separate fiber
so your main fiber needs to do something else and yield. For
details on that, see [Crystal's docs.](https://crystal-lang.org/reference/1.8/guides/concurrency.html)

This feature is still under development and may change, but here
is an example of how it works, taken from the specs:

```crystal
# We create a proc that has a visible side effect
x = 0
counter = TaskProc.new { x += 1; x.to_s }
# This task depends on a file called "i" and produces "t1"
Task.new(output: "t1", inputs: ["i"], proc: counter)
# Launch in auto mode
TaskManager.auto_run

# We have to yield and/or do stuff in the main fiber
# so the auto_run fibers can run
Fiber.yield

# Trigger a build by creating the dependency
File.open("i", "w") << "foo"
Fiber.yield

# Stop the auto_run
TaskManager.auto_stop

# It should only have ran once
x.should eq 1
File.exists?("t1").should eq true
```

## When Tasks Fail

When a task's proc raises, the task fails with a `Croupier::TaskFailure`
that keeps the original exception available as its `#cause`.

A failed run surfaces as `Croupier::RunFailure`:

* Without `keep_going`, the run aborts on the first failure and its
  state is not saved. The `RunFailure` carries that single failure.
* With `keep_going: true`, the run completes everything it can and
  saves its state, then raises `RunFailure` at the end. Its `#errors`
  array carries every failure, each with the original exception as its
  `#cause`.

```crystal
begin
  TaskManager.run_tasks(keep_going: true)
rescue failure : Croupier::RunFailure
  failure.errors.each { |e| Log.error { e.message } }
  exit 1
end
```

Tasks that can't run because an input is missing (neither a task
output, an existing file, nor a `kv://` key) raise
`Croupier::UnknownInputsError`. In auto mode these are expected
(inputs appear incrementally) and retried with backoff. Failed tasks
leave their inputs marked as modified, so the next run retries them.

## Development

Let's try to keep test coverage good :-)

* To run tests: `make test` or `crystal spec`
* To check coverage: `make coverage`
* To run mutation testing: `make mutation`

Other than that, anything is fair game. In the TODO.md file there is a
section for things that were considered and decided to be a bad idea,
but that is conditional and can change when presented with a good
argument.

## Contributing

1. Fork it (<https://github.com/ralsina/croupier/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

* [Roberto Alsina](https://github.com/ralsina) - creator and maintainer
