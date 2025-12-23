# File Analysis: `chunker.rs`

## Overview
**Role:** The Deduplication Engine

`chunker.rs` is responsible for breaking large streams of data into smaller, manageable pieces called **Chunks**. This is the fundamental technology that enables **Deduplication** in Proxmox Backup Server (PBS).

Instead of saving a 100GB disk image as a single file, the `chunker` slices it into thousands of small blocks (typically 4MB). If you run the backup again and only 5% of the data has changed, PBS only needs to save the new chunks, referencing the old ones for the rest.

## Core Abstractions

### 1. `Chunker` (The Iterator)
* **Concept:** A wrapper around a data stream (like a file reader).
* **Behavior:** It reads the raw data stream and yields `Chunk` objects one by one.
* **Flexibility:** It supports two distinct modes of operation depending on the source data:
    1.  **Fixed-Size Chunking** (Fast, simple).
    2.  **Dynamic-Size Chunking** (Content-aware, robust).

### 2. `ChunkerImpl` (The Strategy)
This enum defines the logic used to determine where to "cut" the stream.

* **`Fixed`:**
    * **Used for:** Virtual Machine disks (`.img`, `.raw`), Block Devices.
    * **Logic:** Cuts strictly at fixed offsets (e.g., every 4096 KB).
    * **Why:** Block devices usually change by overwriting existing blocks. Data rarely "shifts" position, so fixed boundaries align well with the underlying file system of the VM.

* **`Dynamic` (Buzhash):**
    * **Used for:** File Archives (`.pxar`), Container backups.
    * **Logic:** Calculates a rolling hash (Buzhash) of the byte stream window. When the hash matches a specific binary pattern (zero bits), it creates a cut.
    * **Why:** If you insert a file into a `.tar` or `.pxar` archive, all subsequent bytes shift. Fixed chunking would result in *every* subsequent chunk changing (0% deduplication). Dynamic chunking finds the same content boundaries even if the data has shifted, re-aligning the stream to maximize deduplication.

## The Algorithms

### 1. Rolling Hash (Buzhash)
The file implements a cyclic polynomial rolling hash.
* **Window:** It looks at a sliding window of bytes (e.g., 48 bytes).
* **Efficiency:** Calculating the hash for the *next* window only requires removing the byte that slid out and adding the byte that slid in. It does not re-calculate the whole window. This makes scanning gigabytes of data extremely fast.

### 2. Boundary Constraints
To prevent edge cases that hurt performance, the chunker enforces limits:
* **Min Size:** (e.g., 64KB) Prevents creating millions of tiny 1-byte chunks which would bloat the index and slow down the database.
* **Max Size:** (e.g., 16MB) Ensures chunks don't become too large to manage or decrypt in memory.
* **Target Size:** (e.g., 4MB) The algorithm tunes the hash mask to aim for this average size.



## Architectural Relationships

The Chunker acts as the "Meat Grinder" at the start of the backup pipeline:

| Related File | Interaction |
| :--- | :--- |
| **`client` (External)** | The Backup Client (running on the source machine) actually runs this code. It chunks data locally before sending it to the server. |
| **`data_blob.rs`** | Once `chunker.rs` defines a chunk (e.g., "bytes 0 to 4MB"), that data is handed to `data_blob.rs` to be compressed and encrypted. |
| **`fixed_index.rs`** | If using Fixed chunking, the resulting list of chunk digests is stored in a `.fidx` file. |
| **`dynamic_index.rs`** | If using Dynamic chunking, the resulting list is stored in a `.didx` file. |

## Why is this file critical?

`chunker.rs` defines the **Storage Efficiency** of the entire system.

1.  **Storage Savings:** By intelligently slicing data, it ensures that only unique data is stored.
2.  **Network Savings:** Since this logic runs on the client, PBS doesn't even send chunks that the server already has (the client sends the hash, server says "got it", client skips upload).
3.  **Speed:** The rolling hash implementation must be highly optimized (using bitwise operations) because every single byte of every backup passes through this logic.

---
*Analysis generated based on the source code provided.*