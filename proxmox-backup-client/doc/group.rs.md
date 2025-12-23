# Proxmox Backup Client - group.rs Analysis

## Overview
This module manages **Backup Groups**. In Proxmox Backup Server, a "Group" is the collection of all snapshots for a specific backup ID (e.g., `vm/100` or `host/webserver`).

## Commands

### `forget_group`
- **Purpose**: Completely removes a backup group and **all** its snapshots from the server.
- **Signature**: `pub async fn forget_group(param: Value) -> Result<Value, Error>`
- **Workflow**:
    1.  **Safety Check**: It performs a `GET` request to list snapshots in the group first.
    2.  **User Confirmation**: It prints "Delete group '...' with X snapshot(s)?" and requires the user to type "y" (unless `--force` is implied/used in non-interactive modes).
    3.  **Execution**: Sends a `DELETE` HTTP request to `/api2/json/admin/datastore/{store}/groups`.
- **Error Handling**: It explicitly handles the `ENOENT` (Entity Not Found) error to exit gracefully if the group was already deleted.

## Integration
- Maps directly to the PBS API endpoint for group management.
- Uses `proxmox_router` for CLI argument parsing.
