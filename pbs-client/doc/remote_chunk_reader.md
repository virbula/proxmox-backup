# Source File Analysis: `remote_chunk_reader.rs`

## Overview
This module implements a reader that can read data from a *remote* chunk store. It acts effectively as a Virtual File System reader over the network.

## Key Structures

### `RemoteChunkReader`
* Implements `AsyncRead` + `Seek`.
* **Context**: Used when performing a file-level restore where the data resides on the PBS server.
* **Logic**:
    * Calculates which chunk contains the requested byte range.
    * Fetches the chunk via `BackupReader`.
    * Decripts/Decompresses if necessary.
    * Serves the bytes.
    * **Caching**: Likely implements a LRU cache (referenced by `LocalChunkCache` or `LookAheadCache`) to avoid re-downloading the same chunk for sequential reads.

## Dependencies
* `backup_reader`: To fetch chunks.
* `pbs_datastore`: For chunk blob handling.