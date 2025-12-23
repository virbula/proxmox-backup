# File Analysis: `chunk_store.rs`

## Overview
**Role:** Physical Chunk Storage Manager

`chunk_store.rs` is the low-level filesystem abstraction for the `.chunks` directory. While `datastore.rs` manages the high-level backup concepts, `chunk_store.rs` strictly manages the storage and retrieval of immutable data blocks (chunks) on the disk.

## Directory Layout Strategy
To avoid filesystem performance issues associated with storing millions of files in a single directory, `chunk_store.rs` implements a **Two-Level Hashing Scheme**.

* **Root:** `<datastore_base>/.chunks/`
* **Subdirectories:** `00/` through `ff/` (256 folders).
* **Derivation:** The subdirectory name is derived from the **first byte** of the chunk's SHA-256 checksum.
* **Filename:** The full hexadecimal SHA-256 digest.

**Example:**
A chunk with digest `a948904f...` will be stored at:
`.../.chunks/a9/a948904f...`

## Core Functions

### 1. `create_chunk`
* **Input:** Raw bytes (data).
* **Process:**
    1.  Calculates the digest (SHA-256) of the data.
    2.  Determines the correct subdirectory (creating it if it doesn't exist).
    3.  Writes the file to a temporary location first.
    4.  Renames the temp file to the final path (Atomic Write) to ensure partial writes never corrupt the store.

### 2. `load_chunk`
* **Input:** Chunk Digest.
* **Process:** Locates the file and reads the raw bytes. Note that this function usually returns the *raw* on-disk data (which might be encrypted/compressed). It does not decrypt; that is the job of the `ChunkReader`.

### 3. `cond_touch_chunk` (Usage in Garbage Collection)
* **Role:** Updates the `atime` (access time) or modification time of a chunk.
* **Purpose:** This is critical for **Garbage Collection**. The GC algorithms often rely on file timestamps (specifically the "atime" or "mtime" depending on filesystem support) to determine if a chunk is still in use by an active backup, preventing race conditions where the GC deletes a chunk currently being written.

---
*Analysis generated based on source code.*