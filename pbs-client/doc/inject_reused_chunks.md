# Source File Analysis: `inject_reused_chunks.rs`

## Overview
This module implements an optimization for the backup process. When a chunk stream is generated, this logic injects information about chunks that are already known to exist, potentially skipping read operations or hashing if the metadata allows.

## Functionality
* **`InjectReusedChunks`**: A stream transformer.
* **Input**: A stream of `(Offset, Chunk)`.
* **Logic**:
    * It checks a `KnownChunks` index (previous backup index).
    * If the current offset and size match a chunk in the previous snapshot, it can "inject" the Digest (Hash) immediately without re-reading or re-hashing the full data, assuming the file hasn't changed (based on mtime/inode check in previous steps).
* **Goal**: Speed up incremental backups significantly.

## Dependencies
* `pbs_datastore::index`: To read previous backup indexes.