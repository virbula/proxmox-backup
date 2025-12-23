# Proxmox Backup Client - Source Code Documentation

## Overview

This directory contains the source code for the `proxmox-backup-client`, the primary command-line interface (CLI) used to interact with the Proxmox Backup Server (PBS). This client is responsible for creating backups, restoring data, managing encryption keys, and performing maintenance tasks like pruning or garbage collection triggers.

## Source File Analysis

### 1. `main.rs`
* **Functionality**: This is the entry point for the `proxmox-backup-client` binary. It sets up the asynchronous runtime (Tokio), handles signal interrupts, and processes the command-line arguments.
* **Role**: It acts as the central dispatcher. It registers all subcommands (e.g., `backup`, `restore`, `login`, `mount`) and routes execution to the specific modules described below.
* **Integration**: It initializes the connection to the Backup Server by parsing the repository URL (user@host:datastore) and setting up the authentication context before handing off control to specific command handlers.

### 2. `benchmark.rs`
* **Functionality**: Implements the `benchmark` subcommand. It runs a series of performance tests including TLS connection speed, SHA256 hashing, ZStd compression, AES256-GCM encryption, and chunk verification.
* **Role**: A diagnostic tool to help users identify bottlenecks (CPU vs. Network vs. Disk) in their backup pipeline.
* **Integration**:
    * **Client-Side**: Uses local CPU resources to test cryptographic and compression libraries (`openssl`, `zstd`).
    * **Server-Side**: Connects to the server to perform a real network upload speed test (optionally bypassing local caches) to measure raw throughput to the datastore.

### 3. `catalog.rs`
* **Functionality**: Manages the backup catalog, which is an index of all files contained within a backup snapshot. It includes the `dump_catalog` command (to print the index) and the `catalog_shell` (interactive navigation).
* **Role**: Allows users to inspect the contents of a backup without downloading the full archive.
* **Integration**:
    * **Server-Side**: Fetches dynamic index files (`.didx`) and the `catalog.pcat1` file from the server.
    * **Client-Side**: decoding the catalog locally to present a virtual file structure to the user in the interactive shell.

### 4. `group.rs`
* **Functionality**: Manages backup groups (a collection of snapshots for a specific host/ID). It primarily implements the `forget_group` command.
* **Role**: Administrative cleanup. It allows users to remove entire history chains for a specific backup ID.
* **Integration**: Sends API requests to the server (specifically `DELETE /api2/json/admin/datastore/{store}/groups`) to trigger the deletion of metadata and chunks on the server side.

### 5. `key.rs`
* **Functionality**: Handles all encryption key operations. This includes creating (random), importing, and changing passwords for client-side encryption keys. It also manages "Master Keys" (RSA) which allow admins to recover backups even if the specific backup password is lost.
* **Role**: Security enforcer. It ensures that encryption keys are managed safely and stored in the correct format (`.key` files).
* **Integration**:
    * **Client-Side**: Reads/writes key files locally. It integrates with the `crypt_config` module to provide the keys needed by `BackupWriter` to encrypt chunks *before* they leave the client.

### 6. `mount.rs`
* **Functionality**: Implements the `mount` command, which allows a remote backup snapshot (specifically `.pxar` archives) to be mounted as a local read-only filesystem (FUSE).
* **Role**: Restoration helper. It allows users to use standard tools (`ls`, `cp`, `grep`) to browse and recover single files from a remote backup.
* **Integration**:
    * **Client-Side**: Uses the FUSE (Filesystem in Userspace) bindings to expose the data to the OS kernel.
    * **Server-Side**: dynamically fetches only the required data chunks from the server as the user reads files, acting as a network filesystem.

### 7. `snapshot.rs`
* **Functionality**: Manages individual snapshots. It includes logic to list snapshots and forget (delete) specific ones.
* **Role**: Granular backup management.
* **Integration**: Queries the PBS API to retrieve lists of snapshots and sends deletion requests for specific backup times.

### 8. `task.rs`
* **Functionality**: Provides tools to interact with long-running tasks on the server.
* **Role**: Monitoring. Allows the user to see the status of garbage collection, verify jobs, or active backup sessions.
* **Integration**: Connects to the `api2/json/nodes/{node}/tasks` endpoints on the server to stream log files or check status.

### 9. `namespace.rs`
* **Functionality**: Manages the namespace hierarchy (e.g., `marketing/workstations/john`).
* **Role**: Organization. Allows creating and listing namespaces to keep backups organized in large deployments.
* **Integration**: interacting with the server's namespace API to create or list logical separation layers within a datastore.

### 10. `helper.rs`
* **Functionality**: Contains shared utility functions, particularly for setting up FUSE accessors and handling split archives (where metadata and data are separated).
* **Role**: Glue code. It abstracts the complexity of initializing readers for different archive types (unified vs. split pxar).
* **Integration**: Used heavily by `mount.rs` and `catalog.rs` to prepare the `BackupReader` and `Accessor` structures needed to read remote data.

### 11. `bin/dump-catalog-shell-cli.rs`
* **Functionality**: A specialized binary wrapper for the catalog shell.
* **Role**: Debugging/Standalone. It allows the catalog shell logic to be invoked independently of the main `proxmox-backup-client` binary, likely for specific internal use cases or development testing.

---

## System Integration Summary

### Client-Side Integration
The client operates on a **"Client-Encrypted, Chunk-Based"** model.
1.  **Input**: The client reads local files.
2.  **Processing**:
    * **Chunking**: Files are split into dynamic chunks.
    * **Deduplication**: Hashes are calculated to detect duplicate data.
    * **Encryption**: `key.rs` provides the configuration to encrypt chunks locally (AES-256-GCM) using the user's password/key.
3.  **Output**: Only encrypted chunks and metadata indices are sent to the network.

### Server-Side Integration
The client interacts with Proxmox Backup Server purely via the **REST API**:
* **Authentication**: Handled via Ticket/Token exchange.
* **Data Transfer**: Uses HTTP/2 for efficient streaming of chunks.
* **Control Plane**: Modules like `task.rs`, `group.rs`, and `snapshot.rs` send control commands (JSON payloads) to manage the lifecycle of data stored on the server.