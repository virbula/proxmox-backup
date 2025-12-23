# File Analysis: `datastore.rs`

## Overview
**Role:** Central Storage Controller (The "God Object")

`datastore.rs` is the most critical file in the module. It defines the `DataStore` struct, which represents a physical backup location (e.g., a mount point on `/mnt/backup`). It orchestrates all high-level storage operations, manages the connection to the backend (Local Filesystem or S3), and enforces consistency.

## Core Abstractions

### 1. `DataStore` Struct
* **State:** Holds the configuration (`chunk_store`, `verify_new`, `gc_status`), the base path, and a cache of usage statistics.
* **Singleton-like Access:** Uses a `HashMap` (`DATASTORE_MAP`) behind a `RwLock` to ensure that only one instance of a `DataStore` exists per physical path per process. This is crucial for thread safety.

### 2. Backend Abstraction
The `DataStore` wraps a `DatastoreBackend` enum:
* **`Filesystem`**: Standard POSIX storage. Backups are directories; chunks are files.
* **`S3`**: Offloads data to an S3-compatible object store.
    * *Note:* Even with S3, PBS often keeps a local cache of metadata (indexes) to speed up listing and browsing.

## Key Responsibilities

### 1. Garbage Collection (GC)
The `garbage_collection` function is the engine that reclaims space.
* **Mark:** It iterates through *every* index file (`.fidx`, `.didx`) of every snapshot. It extracts the Chunk Digests (hashes) and marks them as "Alive" in the `chunk_store`.
* **Sweep:** It tells the `chunk_store` to delete any chunk on the disk that was *not* marked alive.
* **Safety:** It uses the `generation` (generation number) logic to ensure it doesn't delete chunks that are currently being written by a running backup.

### 2. Consistency Checks
* **`check_consistency`**: Scans snapshots to ensure that all required files (`.chunks`, `index.json`, `.blob`) exist and are readable.

### 3. Verification
* **`verify_new`**: A setting that forces the datastore to re-verify chunks immediately after they are uploaded, ensuring no corruption occurred during transfer.

---
*Analysis generated based on source code.*