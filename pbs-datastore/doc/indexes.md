# File Analysis: `fixed_index.rs` & `dynamic_index.rs`

## Overview
**Role:** Logical-to-Physical Address Mapping

These two files implement the specific formats used to map a backup's logical content (e.g., "VM Disk") to its physical chunks (SHA-256 hashes).

## 1. `fixed_index.rs` (Block Device Backups)
* **Use Case:** Virtual Machine images (`.img`), Volumes.
* **Format:** A linear array of 32-byte Checksums.
* **Logic:**
    * Because the chunk size is fixed (usually 4MB), we don't need to store offsets.
    * Chunk 0 is bytes `0..4MB`. Chunk 1 is `4MB..8MB`.
    * To read byte `10,000,000`: Calculate `10M / 4M = Index 2`. Look up the digest at index 2.
* **Performance:** Extremely fast O(1) lookups.

## 2. `dynamic_index.rs` (File Archive Backups)
* **Use Case:** File archives (`.pxar`).
* **Format:** A 3-level Tree Structure (to handle potentially infinite file sizes).
* **Logic:**
    * Chunks have variable sizes (due to deduplication boundaries).
    * The index must store `(Offset, Size, Digest)` for every chunk.
    * **L1/L2/L3:** It uses a hierarchical index (like a B-Tree) to keep the index file itself manageable and loadable.
* **Performance:** O(log n) lookups via binary search on the offsets.

---
*Analysis generated based on source code.*