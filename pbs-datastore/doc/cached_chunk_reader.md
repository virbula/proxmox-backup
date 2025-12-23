# File Analysis: `cached_chunk_reader.rs`

## Overview
**Role:** Restore Performance Optimizer

`cached_chunk_reader.rs` wraps any existing `AsyncReadChunk` implementation (usually the `LocalChunkReader`) and adds an in-memory **LRU (Least Recently Used) Cache**.

## The Problem
Deduplication causes data fragmentation. restoring a single large file (like a database) might require reading the same shared zero-chunk or metadata-chunk hundreds of times. Without a cache, this would trigger hundreds of identical IOPS to the disk, killing performance.

## The Solution: `CachedChunkReader`

### Mechanisms
* **`AsyncLruCache`**: Uses a hash map to store recently fetched decompressed chunks.
* **Hit/Miss Logic**:
    * **Hit:** Returns the data immediately from RAM.
    * **Miss:** Calls the underlying reader, waits for the disk I/O, stores the result in the cache, and then returns it.
* **Concurrency:** Implements logic to handle concurrent requests for the *same* missing chunk (Request Coalescing), ensuring that if 5 threads ask for Chunk A simultaneously, only 1 disk read occurs.

### Seek Support
This file also implements `AsyncSeek` logic. Since chunks are disparate files, "seeking" to a byte offset implies doing math to calculate *which* chunk contains that offset, rather than actually moving a file pointer.

---
*Analysis generated based on source code.*