# File Analysis: `linux_list_drives.rs`

## Purpose
Provides utilities to discover tape hardware on a Linux host.

## Functionality

* **`scan_drives`**: Iterates through sysfs (class `scsi_tape`) to find LTO drives.
* **`scan_changers`**: Iterates through sysfs (class `scsi_generic`) to find changers.
* **Correlation**: Attempts to match drives to changers (often needed to know which drive index in the changer corresponds to which `/dev/nstX` device).
* **Serial Numbers**: Retrieves unique identifiers (WWN/Serial) to ensure configuration persists even if `/dev/` node names change.
