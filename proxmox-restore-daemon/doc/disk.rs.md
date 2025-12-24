# File Analysis: `proxmox_restore_daemon/disk.rs`

## Purpose
This is the "heavy lifting" module responsible for making backup data accessible. It abstracts away the complexity of Linux storage subsystems.

## Key Structures

### `struct DiskState`
A singleton that maintains the current state of detected disks and mount points.

## Core Capabilities

### 1. Disk Scanning
* Iterates through `/sys/block` to find virtual drives attached by QEMU.
* Ignores system drives (like the rootfs) and focuses on the backup snapshots.

### 2. Partition & Volume Management
* **Partitions**: Parses MBR and GPT tables to find partitions.
* **LVM**: Scans for LVM Physical Volumes (PVs), activates Volume Groups (VGs), and maps Logical Volumes (LVs).
* **ZFS**: Detects ZFS label information and attempts to import ZFS pools in read-only mode (`zpool import -o readonly=on`).

### 3. Mounting
* **`mount()`**: The primary function called by the API.
* It identifies the filesystem type (ext4, xfs, ntfs, etc.) by reading superblocks.
* It creates a temporary mount point (e.g., `/mnt/restore/...`) and mounts the device.
* It handles errors gracefully (e.g., dirty journals) where possible.
