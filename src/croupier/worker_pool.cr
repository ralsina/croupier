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

    def initialize(@name : String, @size : Int32, capacity : Int32, &@work : I -> O)
      @jobs = Channel(I).new(capacity)
      @results = Channel({I, O | Exception}).new(capacity)
      WorkerPool.enable_parallelism(@size)
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
      spawn(name: "#{@name}-#{@spawned}") do
        while item = @jobs.receive?
          result = begin
            @work.call(item)
          rescue ex
            ex
          end
          @results.send({item, result})
        end
      end
      @spawned += 1
    end

    # Resize the default fiber execution context so worker fibers
    # spread across OS threads. Cheap and idempotent.
    #
    # The API exists only on Crystal >= 1.21 without -Dpreview_mt
    # (that flag selects the old runtime, which has no execution
    # contexts). Elsewhere this is a no-op and workers run
    # concurrently on one thread.
    def self.enable_parallelism(workers : Int) : Nil
      workers = 1 if workers < 1
      {% if !flag?(:preview_mt) && compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
        Fiber::ExecutionContext.default.resize(workers.to_i32)
      {% end %}
    end
  end
end
