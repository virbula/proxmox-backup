# Proxmox Backup Client - namespace.rs Analysis

## Overview
The `namespace.rs` module manages **Backup Namespaces**. Namespaces allow for a hierarchical organization of backups (e.g., `marketing/servers/web01` instead of just `web01`).

## Commands

### `create_namespace`
- **Purpose**: Creates a new logical directory in the datastore.

### `list_namespaces`
- **Purpose**: Lists sub-namespaces.
- **Logic**: Can recurse to show the full tree structure.

### `delete_namespace`
- **Purpose**: Removes a namespace.
- **Safety**: Usually requires the namespace to be empty (no groups inside) before deletion, or a recursive delete flag.

## Integration
- Maps to `api2/json/admin/datastore/{store}/namespaces`.
- Namespaces were a later addition to PBS, allowing for better multi-tenant organization.
