# File Analysis: `hierarchy.rs`

## Overview
**Role:** Directory Traversal & Iterators

`hierarchy.rs` decouples the "Walking" logic from the "Processing" logic. It provides optimized iterators to navigate the deep folder structure of a Datastore (`ns/type/id/time`) without the caller needing to manually handle `read_dir` loops or recursion.

## Core Iterators

### 1. `ListNamespaces`
* **Function:** Recursively finds all `BackupNamespace`s.
* **Logic:** It looks for the specific structure of a namespace (optional `ns` subdirectories) and yields them.

### 2. `ListGroups`
* **Function:** Finds all `BackupGroup`s within a namespace.
* **Logic:** Iterates the `type` directories (`vm`, `ct`, `host`), then the `id` directories inside them.
* **Output:** Yields `BackupGroup` objects.

### 3. `ListSnapshots`
* **Function:** Finds all `BackupDir`s (snapshots) within a group.
* **Logic:** Scans a group directory for timestamps conforming to the RFC3339 format.
* **Output:** Yields `BackupDir` objects.

## Architectural Value
This file prevents code duplication. Without it, `prune.rs`, `garbage_collection`, and `list_backups` would all implement their own fragile directory crawling loops. `hierarchy.rs` centralizes this, ensuring that if the directory layout changes, only one file needs to be updated.

---
*Analysis generated based on source code.*