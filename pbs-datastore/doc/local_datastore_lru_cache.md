# File Analysis: `local_datastore_lru_cache.rs`

## Overview
**Role:** Chunk Existence Cache (Stat Accelerator)

`local_datastore_lru_cache.rs` implements a specialized cache to speed up file system operations. In a deduplicated datastore with millions of chunks, the system frequently needs to ask: *"Do we already have this chunk?"*

Doing a filesystem `stat()` call for every single check is expensive (IOPS intensive). This file provides an in-memory layer to answer that question instantly.

## Core Abstractions

### 1. `LocalChunkCache`
* **Structure:** A wrapper around a simple `HashMap` (or LRU structure) that stores the SHA-256 digests of chunks known to exist.
* **Usage:**
    * **Insert:** When a chunk is created or read successfully, it is added to the cache.
    * **Lookup:** Before trying to open a file, the system checks the cache.
    * **Hit:** If present, we skip the `stat` syscall.
    * **Miss:** We check the disk.

## Use Case: Garbage Collection & Sync
This is particularly vital during:
1.  **Garbage Collection:** The GC scans millions of index references. Checking existence for each one without a cache would take hours.
2.  **S3/Remote Backends:** If the datastore is backed by S3 (or a slow NFS mount), checking file existence involves network latency. This cache keeps "hot" chunks known locally to prevent network spam.

---
*Analysis generated based on source code.*