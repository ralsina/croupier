# Croupier

Croupier is a Task definition and execution library.

## What does that mean?

You use Croupier to define tasks. Tasks have:

* A name
* Zero or more input files
* One output file
* A `Proc` that consumes the inputs and produces the output

And here is the fun part:

Croupier will examine the inputs and outputs for your tasks and
use them to build a dependency graph. This expresses the connections
between your tasks and the files on disk, and between tasks, and **will 
use that information to decide what to run**.

So, suppose you have `task1` consuming `input.txt` producing `fileA` and `task2` that has `fileA` as input and outputs `fileB`

Croupier guarantees the following:

* If `task1` has not run before, it *will run*
* If `task1` has run before and `input.txt` has not changed, it *will not run*.
* If `task1` has run before and ìnput.txt` has changed, it *will run*
* If `task1` runs, `task2` *will run*
* `task1` will run *earlier* than `task2`

That's a very long way to say: Croupier will run whatever needs running, based on the content of the dependency files and the dependencies between tasks.

The state between runs is kept in `.croupier` so if you delete that file
all tasks will run.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     croupier:
       github: your-github-user/croupier
   ```

2. Run `shards install`

## Usage

This is the example described above, in actual code:

```crystal
require "croupier"

Croupier::Task.new(
  name: "task1",
  output: "fileA",
  inputs: ["input.txt"],
  block: -> {
    File.open("fileA", "w") do |io|
      io.puts File.read("input.txt").downcase
    end
  }
)

Croupier::Task.new(
  name: "task2",
  output: "fileB",
  inputs: ["fileA"],
  block: -> {
    File.open("fileB", "w") do |io|
      io.puts File.read("fileA").upcase
    end
  }
)

Croupier::Task.run_tasks

```



TODO: Write usage instructions here

## Development

TODO: Write development instructions here

## Contributing

1. Fork it (<https://github.com/your-github-user/croupier/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Roberto Alsina](https://github.com/your-github-user) - creator and maintainer
