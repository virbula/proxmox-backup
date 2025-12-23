# Proxmox Backup Client - snapshot.rs Analysis

## Overview
This module manages operations on individual **Snapshots**. A snapshot is a specific point-in-time backup.

## Commands

### `list_snapshots`
- **Purpose**: Lists all snapshots found in the repository.
- **Filters**: Can filter by `group` (backup ID).
- **Output**: Shows Snapshot Name, Size, and Verification Status.

### `forget_snapshots`
- **Purpose**: Deletes a specific snapshot.
- **Difference from Group Forget**: This removes *one* specific time point (e.g., `2023-01-01T00:00:00Z`), whereas `group.rs` removes the entire history for a host.

## API Integration
- Uses `api2/json/admin/datastore/{store}/snapshots`.
- The `prune` logic (retention policy) is related but often handled by a separate algorithm that calls these delete endpoints programmatically.
