# Proxmox Backup Server Config (`pbs-config`) Analysis

This directory contains the analysis of the `pbs-config` crate. This library is the backbone of the Proxmox Backup Server's configuration management. It is responsible for reading, parsing, validating, updating, and writing all persistent configuration files (usually located in `/etc/proxmox-backup/`).

## Core Responsibilities

1.  **Configuration Persistence**: It maps Rust structs to configuration files (e.g., `datastore.cfg`, `user.cfg`) using the `proxmox-section-config` parser.
2.  **Access Control**: It implements the logic for ACLs, checking if a user has the required privileges (e.g., `Datastore.Read`) on a specific path.
3.  **Job Scheduling**: It defines the configuration for background jobs like Prune, Sync, Verification, and Tape backups.
4.  **System State**: It manages network interfaces and encryption key locations.

## Module Overview

### Authentication & Permissions
* **`acl.rs`**: The ACL tree implementation. It checks permissions by propagating roles down the path hierarchy.
* **`user.rs`**: Manages the `user.cfg` file (users, emails, expiration).
* **`token_shadow.rs`**: Handles the secure storage of API token secrets (hashed) in `token.shadow`.
* **`domains.rs`**: Configures authentication realms (PAM, OpenID Connect).

### Storage & Backups
* **`datastore.rs`**: Defines Datastores (path, retention, GC schedule, maintenance mode).
* **`prune.rs`**: Configures automated pruning schedules to clean up old backups.
* **`verify.rs`**: Configures scheduled verification jobs to ensure data integrity.

### Remotes & Sync
* **`remote.rs`**: Stores credentials and connection info for remote Proxmox Backup Servers.
* **`sync.rs`**: Defines Pull jobs (namespaces, owners, schedules) to sync data from Remotes.

### Tape Backup System
* **`drive.rs`**: Configures Tape Drives and Changers.
* **`media_pool.rs`**: Defines Media Pools (retention policies, encryption keys for tapes).
* **`tape_job.rs`**: Configures Tape Backup and Restore jobs.

### System
* **`network.rs`**: Parses and writes `/etc/network/interfaces`.
* **`metrics.rs`**: Configures external metric servers (InfluxDB) to send stats to.
