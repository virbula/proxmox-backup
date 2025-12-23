# Proxmox Backup Server (PBS) Storage Module Analysis

## Overview
This directory contains the core storage logic for Proxmox Backup Server. It acts as the "Engine" responsible for:
1.  **Deduplication:** Splitting data into chunks and storing them uniquely.
2.  **Indexing:** Tracking which chunks belong to which backup snapshot.
3.  **Hierarchy:** Managing the `Datastore > Group > Snapshot` folder structure.
4.  **Security:** handling encryption and integrity verification.

## File Breakdown

### 1. Core Hierarchy & Management
* **`datastore.rs`**: The main interface for storage operations. Manages the physical directory, garbage collection, and backend abstraction (Filesystem vs S3).
* **`backup_info.rs`**: Defines `BackupGroup` and `BackupDir`. Handles directory creation, locking, and lifecycle (pruning/deletion).
* **`chunk_store.rs`**: Low-level manager for the `.chunks` folder (hashing layout).
* **`hierarchy.rs`**: Iterators for traversing namespaces, groups, and snapshots.
* **`s3.rs`**: Helper functions for S3 object storage compatibility.

### 2. Indexes & Metadata
* **`manifest.rs`**: Handles `index.json` (the snapshot inventory).
* **`catalog.rs`**: Handles `.cat` files (fast file search index).
* **`index.rs`, `fixed_index.rs`, `dynamic_index.rs`**: Manages the mapping of logical data to physical chunk hashes (`.fidx`, `.didx`).
* **`file_formats.rs`**: Definitions of binary file headers and magic numbers.

### 3. Data Processing (IO)
* **`chunker.rs`**: Logic for splitting data streams into chunks (Buzhash).
* **`data_blob.rs`**: The container format for storing chunks (includes compression/encryption headers).
* **`read_chunk.rs`, `local_chunk_reader.rs`**: Interfaces for reading chunks from disk/network.
* **`cached_chunk_reader.rs`**: Adds LRU caching to chunk reading for performance.
* **`snapshot_reader.rs`**: Reassembles chunks into a linear data stream.

### 4. Encryption & Integrity
* **`crypt_reader.rs` / `crypt_writer.rs`**: Stream wrappers for on-the-fly AES-256-GCM encryption/decryption.
* **`checksum_reader.rs` / `checksum_writer.rs`**: Stream wrappers for CRC/SHA verification.
* **`paperkey.rs`**: Utilities for exporting encryption keys as printable text/QR codes.

### 5. Maintenance & Utils
* **`prune.rs`**: Implementation of backup retention policies (keep-daily, keep-weekly, etc.).
* **`task_tracking.rs`**: Tracks active read/write operations by process ID.
* **`store_progress.rs`**: Helpers for reporting progress of long tasks.
* **`chunk_stat.rs`**: structures for collecting storage statistics.