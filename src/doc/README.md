# Proxmox Backup Server (PBS) Source Code Overview

This document provides a high-level overview of the source code structure provided in `all.rs`. It details the roles and functions of the core daemons, API endpoints, background jobs, and subsystems that make up the Proxmox Backup Server.

## 1. Binaries & Entry Points (`bin/`)

These files contain the `main()` functions for the system services and command-line tools.

### Daemons

* **`bin/proxmox-backup-proxy.rs`**: The public-facing HTTPS server (listens on port 8007). It handles initial connection acceptance, TLS termination, user authentication, and forwards API requests to the API daemon. It also manages the scheduler for background tasks.

* **`bin/proxmox-backup-api.rs`**: The internal, privileged API server (listens on localhost:82). It runs with specific permissions (usually the `backup` user) to perform operations on datastores, such as reading/writing chunks, garbage collection, and tape operations.

* **`bin/proxmox-daily-update.rs`**: A utility likely run via cron to handle daily maintenance tasks (updates, subscription checks).

### CLI Tools

* **`bin/proxmox-backup-manager.rs`**: The main CLI tool (`proxmox-backup-manager`) for administering the server (user mgmt, datastore config, garbage collection trigger).

  * *Sub-modules* (e.g., `bin/proxmox_backup_manager/datastore.rs`): Implement the specific CLI sub-commands.

* **`bin/proxmox-tape.rs`**: The CLI tool (`proxmox-tape`) for managing tape drives, media, and changers.

* **`bin/proxmox-backup-debug.rs`**: Debugging tools (`proxmox-backup-debug`) for inspecting chunks, recovering indexes, or diffing snapshots.

* **`bin/sg-tape-cmd.rs`**: A low-level helper binary to execute SCSI Generic (SG) commands on tape drives.

## 2. API Implementation (`api2/`)

This directory contains the implementation of the REST API endpoints. It is structured hierarchically, matching the URL path (e.g., `/api2/json/...`).

### Core Protocols

* **`api2/backup/`**: Implements the actual data transfer protocol used by clients.

  * **`environment.rs`**: Sets up the environment (locks, permissions) for a backup session.

  * **`upload_chunk.rs`**: **Critical Path**. Handles the POST request to upload a data chunk, verifies its digest, and saves it to the chunk store (if not already present).

* **`api2/reader/`**: Implements the restore protocol (downloading chunks and files).

### Administration (`api2/admin/`)

* **`datastore.rs`**: Endpoints to manage datastore status, mounting, and verifying.

* **`gc.rs`**, **`prune.rs`**, **`verify.rs`**: Endpoints to trigger Garbage Collection, Pruning, and Verification jobs manually.

* **`sync.rs`**: Endpoints to manage and run "Pull" sync jobs from remote servers.

* **`traffic_control.rs`**: Configuration for rate limiting.

### Configuration (`api2/config/`)

CRUD (Create, Read, Update, Delete) endpoints for system configuration files stored in `/etc/proxmox-backup`.

* **`datastore.rs`**, **`remote.rs`**, **`user.rs`**, **`acme.rs`**: configuration for respective subsystems.

### Node Management (`api2/node/`)

Endpoints that interact with the host OS.

* **`disks/`**: Management of physical disks (ZFS, LVM, formatting).

* **`network.rs`**, **`time.rs`**, **`dns.rs`**: System network/time configuration.

* **`services.rs`**: Start/Stop systemd services.

* **`apt.rs`**: Package updates.

## 3. Core Server Logic (`server/`)

This directory contains the "heavy lifting" logic, specifically long-running background tasks.

### Maintenance Jobs

* **`server/gc_job.rs`**: The **Garbage Collector**. It implements the Mark-and-Sweep algorithm.

  1. **Mark**: Iterates all index files (`.fidx`, `.didx`) to find referenced chunks.

  2. **Sweep**: Deletes chunks from the store that were not marked and are older than the safety cutoff.

* **`server/prune_job.rs`**: Implements retention policies (keep-last, keep-daily, etc.) and deletes expired snapshot indexes.

* **`server/verify_job.rs`**: Reads all chunks in a snapshot, calculates their SHA-256 checksums, and compares them against the index to detect bit-rot.

### Synchronization

* **`server/sync.rs`**: Orchestrator for Sync Jobs.

* **`server/pull.rs`**: The logic to connect to a remote PBS instance, download the manifest, compare chunk lists, and download missing chunks (Pull Replication).

* **`server/push.rs`**: Logic for pushing local snapshots to a remote target.

### Infrastructure

* **`server/auth.rs`**: Server-side authentication caching and context management.

* **`server/jobstate.rs`**: Manages the persistence of job states (upids) to disk, allowing the UI to track running and finished tasks.

* **`server/metric_collection/`**: Background task that gathers system stats and sends them to external metric servers (InfluxDB).

## 4. Tape Backup Subsystem (`tape/`)

Implements support for LTO Tape Drives and Tape Libraries (Changers).

### Drivers

* **`tape/drive/lto/`**: Low-level driver for LTO drives using SCSI commands.

* **`tape/changer/`**: Drivers for robotic tape libraries (SCSI Media Changer).

  * **`mtx/`**: Wrapper around the `mtx` system tool.

### Logic

* **`tape/pool_writer/`**: Logic to write a stream of data (snapshots) onto a set of tapes (Media Pool), handling tape spanning and encryption.

* **`tape/inventory.rs`**: Manages the database of known tapes and their location.

* **`tape/encryption_keys.rs`**: Management of tape-specific encryption keys.

## 5. Authentication & ACME

* **`auth.rs`** / **`auth_helpers.rs`**: Core authentication primitives. Handles CSRF token generation, verifying API tickets, and PAM interaction.

* **`acme/`**: Client implementation for the ACME protocol (Let's Encrypt).

  * **`client.rs`**: Handles order creation and challenge fetching.

  * **`plugin.rs`**: Interface for DNS verification plugins.

## 6. Tools & Utilities (`tools/`)

Shared helper functions used across the server.

* **`tools/disks/`**: wrappers for `lsblk`, `zpool`, `lvm` commands to manage storage.

* **`tools/systemd/`**: wrappers for `systemctl` to manage services.

* **`tools/ticket.rs`**: Logic for generating and verifying signed RSA auth tickets.

* **`tools/fs.rs`**: Filesystem helpers (safe file creation, ownership).

* **`tools/statistics.rs`**: Math helpers for calculating RRD stats.

## Summary of Data Flow

1. **Backup**: Client connects to `proxmox-backup-proxy` -> Forwards to `proxmox-backup-api` -> `api2/backup/upload_chunk.rs` writes data to disk.

2. **Maintenance**: `proxmox-backup-proxy` spawns a scheduler -> Triggers `server/gc_job.rs` -> Scans disk and removes unused chunks.

3. **Restore**: Client requests file -> `api2/reader/` fetches chunks -> Decrypts/Decompresses -> Streams back to client.
