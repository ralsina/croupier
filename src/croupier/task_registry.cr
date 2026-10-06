module Croupier
  # Every registered task, keyed by each of its outputs (or by id for
  # tasks without outputs), plus an index by task id. The value of
  # `TaskManager.tasks`.
  #
  # Read-only for users: the task set changes only through `Task.new`
  # and `TaskManager.remove_task`, which lock, refuse changes during a
  # run and keep both indexes in step.
  class TaskRegistry
    include Enumerable({String, Task})

    @by_key = {} of String => Task
    @by_id = {} of String => Task

    delegate :[], :[]?, :fetch, :has_key?, :keys, :values, :size, :empty?, :each_key, :each_value, to: @by_key

    def each(& : {String, Task} ->) : Nil
      @by_key.each { |entry| yield entry }
    end

    # The task with id `id`, or nil
    def by_id?(id : String) : Task?
      @by_id[id]?
    end

    def inspect(io : IO) : Nil
      @by_key.inspect(io)
    end

    # :nodoc:
    # Register `task` under every key and its id. Internal: callers
    # hold TaskManager's @data_mutex.
    def put(task : Task) : Nil
      task.keys.each { |key| @by_key[key] = task }
      @by_id[task.id] = task
    end

    # :nodoc:
    # Drop every entry that points at `task`. Internal: callers hold
    # TaskManager's @data_mutex.
    def remove(task : Task) : Nil
      task.keys.each { |key| @by_key.delete(key) if @by_key[key]?.same?(task) }
      @by_id.delete(task.id) if @by_id[task.id]?.same?(task)
    end

    # :nodoc:
    def clear : Nil
      @by_key.clear
      @by_id.clear
    end
  end
end
