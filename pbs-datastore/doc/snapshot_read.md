# File Analysis: `snapshot_reader.rs`

## Overview
**Role:** Virtual File Streamer

`snapshot_reader.rs` creates the illusion that a backup snapshot is a single, continuous file. It is the core component behind the "Restore" functionality, reassembling scattered chunks into a coherent stream.

## Core Struct: `SnapshotReader`

### Fields
* **`index`**: The `IndexFile` (Fixed or Dynamic) listing the order of chunks.
* **`reader`**: The `AsyncReadChunk` implementation (source of data).
* **`current_chunk_idx`**: Tracks which chunk we are currently processing.
* **`current_offset`**: Tracks how many bytes we have already read from the current chunk.

## Logic: `poll_read`
This implements the standard Rust `AsyncRead` trait:

1.  **Check Buffer:** Does the current chunk have unread bytes?
    * **Yes:** Copy bytes to the output buffer and update `current_offset`.
2.  **Chunk Exhausted:** Have we reached the end of the current chunk?
    * **Action:** Increment `current_chunk_idx`, lookup the next digest from the `index`, and request the new chunk from the `reader`.
3.  **End of Stream:** If `current_chunk_idx` exceeds the index count, return EOF (End of File).

This logic allows standard tools (like `tar`, `cp`, or API downloads) to consume the backup without knowing anything about deduplication or chunks.

---
*Analysis generated based on source code.*