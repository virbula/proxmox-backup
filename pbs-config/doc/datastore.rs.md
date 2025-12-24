# File Analysis: `datastore.rs`

## Purpose
Defines the configuration for Backup Datastores, which are the primary storage locations for backup data. Parses `datastore.cfg`.

## Key Configuration Options

* **`path`**: The filesystem path to the chunk store.
* **`gc-schedule`**: When to run Garbage Collection.
* **`prune-schedule`**: Default pruning schedule.
* **`keep-*`**: Retention options (keep-last, keep-daily, etc.).
* **`maintenance-mode`**: Allows marking a datastore as read-only or offline for maintenance.
* **`notify`**: Notification settings for datastore events.

## Implementation
It uses `SectionConfig` to register the `datastore` plugin, enforcing schema validation on the config file.
