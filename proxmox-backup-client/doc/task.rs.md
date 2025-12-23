# Proxmox Backup Client - task.rs Analysis

## Overview
The `task.rs` module provides CLI tools for monitoring long-running tasks on the server (e.g., garbage collection, sync jobs, active backups).

## Commands

### `task_list`
- **Endpoint**: `GET /nodes/{node}/tasks`
- **Purpose**: Displays a list of recent tasks.
- **Columns**: Start Time, End Time, Status (OK/Error), Type (GC, Backup, Restore), User.

### `task_log`
- **Endpoint**: `GET /nodes/{node}/tasks/{upid}/log`
- **Purpose**: Streams the text log of a specific task.
- **Usage**: Useful for debugging why a server-side job (like a sync) failed.

### `task_stop`
- **Endpoint**: `DELETE /nodes/{node}/tasks/{upid}`
- **Purpose**: Aborts a running task.

## Integration
These commands map directly to the `pbs_api_types::UPID` (Unique Process ID) system used by Proxmox to track async operations.
