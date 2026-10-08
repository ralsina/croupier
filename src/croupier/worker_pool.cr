module Croupier
  # :nodoc:
  # A bounded pool of named worker fibers running one block on
  # submitted items. Results come back through `receive` in completion
  # order, each paired with its item; an exception raised by the block
  # is returned in place of the result, so a failing item never kills
  # its worker or leaves the receiver waiting.
  #
  # Workers are spawned as items are submitted, up to `size`, so a
  # pool that gets fewer items starts fewer fibers. `close` lets them
  # exit once the queued items are done.
  #
  # The caller must never have more than `capacity` items outstanding
  # (submitted but not received): both channels are buffered to that
  # size, so `submit` never blocks, and a caller that stops receiving
  # (it raised) leaves no worker blocked on a send.
  class WorkerPool(I, O)
    @spawned = 0

    {% if !flag?(:preview_mt) && compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
      # Every pool — task workers and scan workers alike — shares one
      # context, so croupier never owns more scheduler threads than
      # the machine has cores, whatever pool names or widths come and
      # go (#95). The context grows to the widest core-count-capped
      # width ever requested and never shrinks. Locked because scan
      # pools are built from inside parallel task workers (see
      # HashState#hash_directory), which run on different threads.
      @@context : Fiber::ExecutionContext::Parallel?
      @@context_lock = Sync::Mutex.new

      # A same-context fiber spawn lands on the spawning thread's local
      # run queue, and parked schedulers are only woken by cross-context
      # enqueues: workers spawned onto the default context (as plain
      # spawn does) pile onto the submitting thread and idle schedulers
      # never wake. On a dedicated context every job hand-off crosses a
      # context boundary, going through the global queue and waking an
      # idle scheduler, so workers spread across their threads.
      private def worker_context : Fiber::ExecutionContext::Parallel
        width = Math.min(@size, System.cpu_count)
        @@context_lock.synchronize do
          context = @@context
          if context.nil?
            context = Fiber::ExecutionContext::Parallel.new("croupier-worker", width)
            @@context = context
          elsif width > context.capacity
            # Grow only: resize would also shrink, cooperatively
            # stopping schedulers that may still be running a pool's
            # workers.
            context.resize(width)
          end
          context
        end
      end
    {% end %}

    def initialize(@name : String, @size : Int32, capacity : Int32, &@work : I -> O)
      @jobs = Channel(I).new(capacity)
      @results = Channel({I, O | Exception}).new(capacity)
    end

    def submit(item : I) : Nil
      spawn_worker if @spawned < @size
      @jobs.send(item)
    end

    def receive : {I, O | Exception}
      @results.receive
    end

    def close : Nil
      @jobs.close
    end

    private def spawn_worker : Nil
      # Named so specs can tell croupier's fibers from runtime ones
      # (GC markers, scheduler loops)
      name = "#{@name}-#{@spawned}"
      {% if !flag?(:preview_mt) && compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
        worker_context.spawn(name: name) { work_loop }
      {% else %}
        spawn(name: name) { work_loop }
      {% end %}
      @spawned += 1
    end

    private def work_loop : Nil
      while item = @jobs.receive?
        result = begin
          @work.call(item)
        rescue ex
          ex
        end
        @results.send({item, result})
      end
    end
  end
end
