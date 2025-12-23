# File Analysis: `chunk_stat.rs`

## Overview
**Role:** Statistics Aggregator

`chunk_stat.rs` defines simple data structures used to collect metrics during backup or garbage collection runs.

## Core Structs
* **`ChunkStat`**:
    * `count`: Number of chunks.
    * `size`: Total logical size (uncompressed).
    * `compressed_size`: Total physical size (compressed/encrypted).
    * `duplicate_count`: How many chunks were skipped because they already existed.

## Usage
These structs are passed around during the backup process. At the end of a job, they are summed up to produce the "Summary" log line (e.g., "Backup size: 100GB. Deduplication factor: 15.4x").

---
*Analysis generated based on source code.*