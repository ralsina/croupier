# Every exception croupier raises on purpose, in one hierarchy so
# callers can rescue by type (`rescue Croupier::Error`) instead of
# matching message strings.
module Croupier
  # Base class of every exception croupier raises deliberately.
  class Error < Exception
  end

  # A task definition is invalid (empty kv:// key, missing outputs and
  # id, duplicate id), or two colliding definitions can't be merged.
  # Raised at declaration time, before anything runs.
  class TaskDefinitionError < Error
  end

  # A dependency cycle. Three unrelated places detect one — a task
  # whose inputs and outputs overlap, adding one of a task's own keys
  # as its input, and a cycle found while sorting the whole graph —
  # and the message says which site fired.
  class CycleError < Error
  end

  # A referenced task, target or output name is not registered.
  class UnknownTaskError < Error
  end

  # Raised when a task can't run yet because an input is not
  # satisfiable: it is neither a fresh task, an existing file, nor a
  # kv:// key. In auto mode this is an expected transient state (inputs
  # appear incrementally), so the autorun loop rescues this class and
  # retries with backoff instead of logging a warning on every cycle.
  class UnknownInputsError < Error
  end

  # A task ran but did not deliver what it declared: a missing output
  # file, or the wrong number of data results for its outputs.
  class TaskVerificationError < Error
  end

  # Raised when a task's proc raises: it carries the task context
  # in its message and keeps the original exception (with its backtrace)
  # available as `#cause`.
  class TaskFailure < Error
  end

  # Raised when a run has failing tasks. Without `keep_going` the run
  # aborts on the first failure (state is not saved); with
  # `keep_going: true` the run completes everything it can and saves
  # its state, then this is raised at the end. `#errors` carries every
  # task failure, since `Exception#cause` can only chain one.
  class RunFailure < Error
    getter errors : Array(Exception)

    def initialize(@errors : Array(Exception))
      super(errors.join("\n") { |failure| failure.message || failure.class.name })
    end
  end

  # Tasks no dependency path leads to from the graph's root.
  class UnreachableTaskError < Error
  end

  # The library was configured or used in a way it does not support:
  # an unsupported platform for auto mode, changing the persistent
  # k/v store path, auto-running with nothing to watch.
  class UsageError < Error
  end
end
