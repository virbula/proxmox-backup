# File Analysis: `async_lru_cache.rs`

## Purpose
Provides a thread-safe, asynchronous wrapper around a synchronous `LruCache`.

## Key Features

* **Concurrency**: Uses `Arc<Mutex<...>>` to share the cache state across threads.
* **Request Coalescing**:
    * Uses a `HashMap` of `BroadcastFuture` to track currently pending fetches.
    * **Scenario**: Task A asks for Key X (miss). Task A starts fetching. Task B asks for Key X (miss).
    * **Result**: Task B sees that Task A is already fetching X. Task B subscribes to Task A's result instead of starting a second fetch.
* **Trait `AsyncCacher`**: Defines the interface for the backing store (how to fetch data on a cache miss).
