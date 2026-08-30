module Croupier
  # TaskManagerType methods for the k/v store and its read-through cache.
  class TaskManagerType
    # Key/Value store
    @_store : Kiwi::Store = Kiwi::MemoryStore.new
    @_store_path : String | Nil = nil

    # Read-through cache for the k/v store: with a persistent
    # (FileStore) store, every staleness check is a SHA1 of the key
    # plus a stat and a file read, all under @data_mutex — and
    # propagate_staleness / waiting_for check every kv output of every
    # task. Values (and misses, which are the hot case in a
    # from-scratch run: every unbuilt producer's key) are remembered
    # in memory; keys only appear through set(), which invalidates
    # both. Cleared whenever the store is swapped or cleaned up;
    # pre-existing keys on disk are picked up lazily on first read.
    @store_cache = Hash(String, String).new
    @store_misses = Set(String).new

    # Store a value, returning whether it CHANGED: a same-value set is a
    # no-op for staleness, so kv outputs holding identical values don't
    # re-stale their dependents on every run.
    def set(key, value) : Bool
      Log.debug { "Setting k/v data for #{key}" }
      changed = false
      @store_lock.synchronize do
        changed = store_read(key) != value
        # An unchanged write is skipped entirely: store_read answered
        # from the cache or the backing store, and either way the disk
        # already holds `value`, so with a persistent store this saves a
        # rewrite per identical kv output per run.
        if changed
          @_store.set(key, value)
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

    # Unsynchronized read-through lookup (callers hold @data_mutex).
    private def store_read(key) : String | Nil
      if value = @store_cache[key]?
        return value
      end
      return nil if @store_misses.includes?(key)
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
      raise "Can't change persistent k/v store path" unless @_store_path.nil?
      new_store = Kiwi::FileStore.new(path)
      # Convert from MemoryStore to FileStore, copying any data set so far
      old_store = @_store.as(Kiwi::MemoryStore)
      old_store.@mem.each { |k, v| new_store[k] = v }
      @_store = new_store
      @_store_path = path
      # New backing store: drop cached answers, lazily re-prime from
      # the file store (which may carry data from a previous process)
      @store_cache.clear
      @store_misses.clear
      Log.debug { "Storing k/v data in #{path}" }
    end
  end
end
