module Croupier
  # TaskManagerType methods for the k/v store and its read-through cache.
  class TaskManagerType
    # Key/Value store
    @_store : Kiwi::Store = Kiwi::MemoryStore.new
    @_store_path : String? = nil

    # Read-through cache for the k/v store. With a FileStore every
    # lookup is a hash, a stat and a file read, and staleness checks
    # and readiness sweeps look up every kv key of every task. Values
    # and misses are both remembered; set() keeps them current. Both
    # are cleared when the store is swapped or cleaned up.
    @store_cache = Hash(String, String).new
    @store_misses = Set(String).new

    # Every key written through set() in this process. Kiwi stores
    # can't be iterated, so use_persistent_store needs this list to
    # migrate data through the public get/[]= API.
    @store_keys = Set(String).new

    # Store a value and return whether it changed. A same-value set
    # is a no-op, so identical kv outputs don't re-stale their
    # dependents.
    def set(key, value) : Bool
      Log.debug { "Setting k/v data for #{key}" }
      changed = false
      @store_lock.synchronize do
        changed = store_read(key) != value
        # Skip unchanged writes: the store already holds `value`
        if changed
          @_store.set(key, value)
          @store_keys << key
          @store_cache[key] = value
          @store_misses.delete(key)
        end
      end
      @modified_lock.synchronize { @modified << "kv://#{key}" } if changed
      changed
    end

    def get(key)
      @store_lock.synchronize { store_read(key) }
    end

    # Read-through lookup. Callers hold @store_lock.
    private def store_read(key) : String?
      if value = @store_cache[key]?
        return value
      end
      return if @store_misses.includes?(key)
      value = @_store.get(key)
      if value.nil?
        @store_misses << key
      else
        @store_cache[key] = value
      end
      value
    end

    # Use a persistent k/v store in this path instead of
    # the default memory store
    def use_persistent_store(path : String)
      return if path == @_store_path
      raise UsageError.new("Can't change persistent k/v store path") unless @_store_path.nil?
      # Swap under @store_lock so a concurrent set() can't write to
      # the old store mid-swap
      @store_lock.synchronize do
        new_store = Kiwi::FileStore.new(path)
        # Copy everything written so far. A pre-existing file store
        # may also hold keys from an earlier process; the cache picks
        # those up on first read.
        @store_keys.each do |key|
          if value = @_store.get(key)
            new_store[key] = value
          end
        end
        @_store = new_store
        @_store_path = path
        @store_cache.clear
        @store_misses.clear
      end
      Log.debug { "Storing k/v data in #{path}" }
    end
  end
end
