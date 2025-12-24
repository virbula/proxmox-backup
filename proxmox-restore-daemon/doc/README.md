# Proxmox Restore Daemon (`proxmox-restore-daemon`)

This directory contains the source code for the `proxmox-restore-daemon`. This binary is designed to run **inside** a lightweight, temporary Virtual Machine (micro-VM) spun up by the `proxmox-file-restore` tool.

## Architecture Overview

When a user requests a single-file restore:
1.  The Host spawns a QEMU VM.
2.  The backup snapshots are mapped as block devices to this VM.
3.  This daemon (`proxmox-restore-daemon`) starts automatically inside the VM.
4.  It scans the attached disks, detects filesystems (ZFS, LVM, ext4, xfs, etc.), and mounts them.
5.  It listens on a **Virtio-VSOCK** socket (instead of TCP/IP) for API commands from the host.
6.  It streams requested file data back to the host.

## Module Breakdown

* **`main.rs`**: The application entry point. It initializes the environment, sets up the VSOCK listener, and starts the REST API server.
* **`api.rs`**: Defines the API endpoints (RPC) that the host client calls. This includes commands to list directory contents, extract files, and check status.
* **`disk.rs`**: The most complex module. It handles the low-level logic of scanning `/sys/block`, identifying partition tables, activating LVM groups or ZFS pools, and mounting filesystems.
* **`auth.rs`**: Manages authentication. Since the VM is ephemeral and single-purpose, it uses a simple ticket-based system to ensure only the authorized host process can command it.
* **`watchdog.rs`**: A safety mechanism. If the host disconnects or the operation hangs, this watchdog ensures the VM shuts itself down automatically to save resources.
