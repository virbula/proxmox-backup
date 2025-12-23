# File Analysis: `index.rs`

## Overview
**Role:** Abstract Index Interface

`index.rs` defines the **Traits** that abstract the differences between Fixed Indexes (`.fidx` for VMs) and Dynamic Indexes (`.didx` for Files).

It allows the rest of the system (like the Restore engine or Garbage Collector) to treat all backups uniformly, regardless of whether they are block-based or file-based.

## Core Abstractions

### 1. `IndexFile` (Trait)
This is the primary interface. Any index type must implement these methods:
* `index_count()`: How many chunks are in this backup?
* `index_digest(pos)`: Get the SHA-256 hash of the chunk at position `X`.
* `chunk_info(pos)`: Get detailed info (digest, offset, length) for a chunk.
* `index_bytes()`: Total size of the backup in bytes.

### 2. `ChunkReadInfo`
A helper struct returned by the index. It tells the reader:
* **Digest:** "You need to fetch chunk `abc123...`"
* **Offset:** "This chunk represents bytes 0-4MB of the original file."
* **End:** "This chunk ends at byte 4MB."

## Architectural Significance

### Polymorphism in Storage
PBS supports two fundamentally different backup modes:
1.  **Block Level:** The image is a linear array of 4MB blocks. (`FixedIndex`)
2.  **File Level:** The archive is a stream of variable-sized chunks. (`DynamicIndex`)

`index.rs` hides this complexity.
* When the **Garbage Collector** runs, it just calls `index_digest()` to mark chunks as "in use." It doesn't care if they came from a VM or a Container.
* When the **Restore Engine** runs, it iterates from `0..index_count()` to reconstruct the file.

### Optimization: Binary Search
The file likely implements helpers for searching. Since indexes are sorted or sequential, finding which chunk contains byte offset `12345678` is an `O(log n)` operation, allowing for fast random access (seeking) inside huge backups.

---
*Analysis generated based on the source code provided.*