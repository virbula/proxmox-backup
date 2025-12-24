# Proxmox File Restore (`proxmox-file-restore`)

This directory contains the source code for the `proxmox-file-restore` tool. This CLI utility allows users to list, browse, and extract individual files from Proxmox Backup Server (PBS) snapshots without needing to fully restore the entire backup archive.

It primarily achieves this by spinning up a lightweight, temporary Virtual Machine (QEMU) that mounts the backup snapshot as a block device. A minimal agent running inside this VM then accesses the filesystem and streams the requested data back to the client.

## Source Code Analysis

The codebase is modular, separating the CLI interface, the abstract driver logic, the concrete QEMU implementation, and low-level helpers.

### Core Components

* **`main.rs`**
    The entry point of the application. It handles command-line argument parsing (subcommands like `list`, `extract`, `shell`) and dispatches requests to the appropriate driver.

* **`block_driver.rs`**
    Defines the `BlockDriver` trait. This is an abstraction layer allowing for different methods of accessing backup data. Currently, the primary implementation is QEMU, but the design supports others.

* **`block_driver_qemu.rs`**
    The heavy lifter. This file implements the `BlockDriver` trait using QEMU. It manages the lifecycle of restoration VMs, handles state (tracking running VMs via `restore-vm-map.json`), and establishes communication via VSOCK.

### Utilities

* **`qemu_helper.rs`**
    Contains the low-level logic for executing the QEMU binary. It constructs the complex command-line arguments required to boot the restore VM, including kernel parameters, initramfs paths, and device mappings.

* **`cpio.rs`**
    A utility module for creating CPIO archives. This is used to dynamically build a minimal `initramfs` containing the restore agent, which is then injected into the QEMU VM at runtime.
