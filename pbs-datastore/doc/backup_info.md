# File Analysis: `backup_info.rs`

## Overview
**Role:** Metadata Controller & ORM Layer

`backup_info.rs` acts as the **Object Relational Mapping (ORM)** layer between the logical concepts of backups (Groups, Snapshots) and the physical storage on disk or S3.

While `datastore.rs` manages the storage backend and `chunk_store.rs` handles the raw data, `backup_info.rs` defines **organization**, **ownership**, and **lifecycle**.

## Core Abstractions

### 1. `BackupGroup`
* **Path:** `<datastore>/<namespace>/<type>/<id>`
* **Example:** `mnt/backup/ns/IT/vm/100`
* **Concept:** Represents a "Backup Series" or a specific entity (VM, Container, Host).
* **Key Responsibilities:**
    * **Listing:** Enumerates all snapshots belonging to this entity (`list_backups`).
    * **Ownership:** Manages the `owner` file (ACLs) to determine which user owns this backup series.
    * **Group Locking:** Implements exclusive locks to prevent concurrent deletions or modifications of the entire group.
    * **S3 Integration:** If running on an S3 backend, `destroy()` triggers the deletion of S3 prefixes via `s3_client`.

### 2. `BackupDir` (The Snapshot)
* **Path:** `<datastore>/<namespace>/<type>/<id>/<timestamp>`
* **Example:** `.../vm/100/2023-01-01T12:00:00Z`
* **Concept:** A specific Point-in-Time backup.
* **Key Responsibilities:**
    * **Manifest Access:** It is the *only* valid entry point to load `index.json` (via `BackupManifest`).
    * **Protection:** Checks for the presence of the `.protected` file (preventing pruning).
    * **Data Loading:** Uses `load_blob` to read config files (e.g., `qemu-server.conf.blob`), handling decryption transparently.
    * **Destruction:** Handles the safe removal of a snapshot, including:
        1. Locking the directory.
        2. Verifying protection status.
        3. Removing index files (`.fidx`, `.didx`).
        4. Removing the manifest.

### 3. `BackupInfo`
* **Concept:** A lightweight data-transfer object (DTO).
* **Usage:** Primarily used by the API and UI to list backups efficiently without loading full heavy metadata.
* **Content:** Contains the `BackupDir` reference, a list of files, and a simple `protected` boolean.

---

## Architectural Relationships

`backup_info.rs` does not exist in a vacuum. It sits at the center of the storage module:

| Related File | Interaction |
| :--- | :--- |
| **`datastore.rs`** | Acts as the parent container. `BackupGroup` holds a reference to `DataStore` to calculate absolute paths and access backend config (S3 vs Local). |
| **`manifest.rs`** | `BackupDir` creates the lock context required to safely load and update the `BackupManifest` struct. |
| **`hierarchy.rs`** | Provides the iterators to find these directories on disk; `backup_info.rs` wraps those raw paths into safe structs. |
| **`s3.rs`** | When `backup_info.rs` deletes a group/snapshot, it calls `s3.rs` logic to ensure remote objects are also scrubbed. |

---

## Key Mechanisms

### 1. Hierarchical Locking
The file implements a sophisticated locking system in `/run/proxmox-backup/locks` to control the "Control Plane" (metadata operations).

* **Double-Stat Logic:** Uses `lock_helper` to perform a `stat` check before and after opening a lock file to prevent race conditions during lock deletion.
* **Namespace Hashing:**
    * **Problem:** Nested namespaces (`A/B/C/D...`) can create paths longer than the Linux limit (255 chars).
    * **Solution:** `lock_file_path_helper` detects long paths and hashes them to generate a safe, fixed-length lock filename.

### 2. The `destroy()` Lifecycle
Deleting a backup is a high-risk operation. The `destroy` method enforces a strict sequence:
1.  **Acquire Lock:** Blocks other readers/writers.
2.  **Check Protection:** Aborts immediately if `.protected` exists.
3.  **Delete Local Files:**