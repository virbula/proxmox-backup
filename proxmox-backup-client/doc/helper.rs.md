# Proxmox Backup Client - helper.rs Analysis

## Overview
`helper.rs` contains utility functions that bridge the gap between the network client (`pbs_client`) and the file access abstraction (`pbs_pxar_fuse`). It is heavily used by `mount.rs` and `catalog.rs`.

## Key Functions

### `get_pxar_fuse_accessor`
- **Purpose**: Creates an `Accessor` trait object that allows random access to a remote `.pxar` archive.
- **Logic**:
    - It distinguishes between **Unified Archives** (metadata + data in one `.pxar` file) and **Split Archives** (metadata in `.mpxar`, data in `.pxar`).
    - It initializes the appropriate readers (e.g., `Accessor::new` vs `Accessor::new_with_payload`).

### `get_buffered_pxar_reader`
- **Purpose**: Creates an optimized, buffered reader for accessing remote chunks.
- **Logic**:
    - Downloads the **Dynamic Index** (`.didx`) for the specified archive.
    - Initializes a `RemoteChunkReader`. This component knows how to fetch individual chunks from the server by their hash.
    - Wraps the reader in a `BufferedDynamicReader` (LRU cache) to minimize network latency during sequential reads.

## Role in Architecture
This file is the critical "glue" layer that allows the `mount` command to treat a remote, chunked, deduplicated backup as if it were a local linear file.
