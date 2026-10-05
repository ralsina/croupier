# Every exception croupier raises on purpose, in one hierarchy, so
# callers can rescue by type (`rescue Croupier::Error`).
module Croupier
  # Base class of every exception croupier raises deliberately.
  class Error < Exception
  end

  # A task definition is invalid (empty kv:// key, no outputs and no
  # id, duplicate id), or two colliding definitions can't be merged.
  # Raised by Task.new.
  class TaskDefinitionError < Error
  end

  # A dependency cycle: a task whose inputs and outputs overlap,
  # add_input with one of the task's own keys, or a cycle found while
  # sorting the graph. The message says which.
  class CycleError < Error
  end

  # A referenced task, target or output name is not registered.
  class UnknownTaskError < Error
  end

  # An input can't be satisfied: it is not a task, an existing file
  # or a kv:// key (checked before a run), or a task is still waiting
  # for one mid-run. In auto mode this is expected while inputs
  # appear, so the autorun loop retries quietly.
  class UnknownInputsError < Error
  end

  # A task ran but did not deliver what it declared: a missing output
  # file, or the wrong number of data results for its outputs.
  class TaskVerificationError < Error
  end

  # A task's proc raised. The message names the task; the original
  # exception is `#cause`.
  class TaskFailure < Error
  end

  # A run had failing tasks. Without `keep_going` the run stops at
  # the first failure and doesn't save state. With `keep_going` it
  # runs what it can, saves state, then raises. `#errors` holds every
  # failure.
  class RunFailure < Error
    getter errors : Array(Exception)

    def initialize(@errors : Array(Exception))
      super(errors.join("\n") { |failure| failure.message || failure.class.name })
    end
  end

  # Unsupported use: changing the task set during a run, auto mode
  # on an unsupported platform or with nothing to watch, changing the
  # persistent k/v store path.
  class UsageError < Error
  end
end
