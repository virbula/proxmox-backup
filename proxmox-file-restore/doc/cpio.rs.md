# File Analysis: `cpio.rs`

## Purpose
This utility module provides functionality to create CPIO (Copy In, Copy Out) archives. In the context of `proxmox-file-restore`, its primary purpose is to dynamically generate an **initramfs** (Initial RAM Filesystem) image.

## Why is this needed?
To perform a file restore, the system boots a tiny Linux kernel in QEMU. This kernel needs a userspace environment to run the restore agent. Instead of managing a large, static ISO image, the tool:
1.  Takes the restore agent binary from the host.
2.  Uses `cpio.rs` to wrap it into a valid CPIO archive structure in memory.
3.  Passes this CPIO blob to QEMU as the `initrd`.

## Key Functionality

### Header Generation
It generates the standard CPIO binary header (New ASCII Format), which includes:
* Magic numbers (to identify the file format).
* File metadata (inode, mode, uid, gid, size).
* File name length.

### Archive Construction
* **`append_file`**: Takes a local file path and adds it to the archive stream.
* **`append_dir`**: Creates directory entries within the archive.
* **Trailer**: Writes the standard `TRAILER!!!` entry that marks the end of a CPIO archive.

This lightweight approach allows the restore VM to always run the exact same version of the agent as the host tool, avoiding version mismatches.
