# Source File Analysis: `chunk_stream.rs`

## Overview
This module implements the Content Defined Chunking (CDC) logic stream. It takes a raw byte stream (like a PXAR stream or a raw block device stream) and cuts it into variable-sized chunks.

## Key Structures

### `ChunkStream`
* Wrapper around a data source.
* Uses a chunker algorithm (likely BuzzHash/Rolling Hash) to find chunk boundaries.

## Core Logic
* **Chunking**: Reads input data and runs the sliding window hash. When the hash matches a specific pattern (zero bits), a chunk boundary is declared.
* **Limits**: Enforces generic minimum (e.g., 64KB) and maximum (e.g., 4MB) chunk sizes to prevent pathological cases.
* **Output**: Yields `(Offset, Data)` tuples ready to be hashed and uploaded by `BackupWriter`.

## Dependencies
* `pbs_datastore::chunker`: The actual chunking algorithm implementation.