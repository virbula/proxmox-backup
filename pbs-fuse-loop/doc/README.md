# Proxmox Backup Server FUSE Loop (`pbs-fuse-loop`) Analysis

This directory contains the analysis of the `pbs-fuse-loop` crate. This library provides a specialized mechanism to map a generic asynchronous reader (like a remote backup chunk stream) to a local Linux Block Device (`/dev/loopX`).

## Overview

Restoring individual files from a VM disk image backup requires the OS to recognize the partition table and filesystems within that image. However, the image data resides on the backup server, not the local disk.

`pbs-fuse-loop` solves this by:
1.  **FUSE Layer**: Exposing the remote data stream as a standard file in a temporary FUSE mount.
2.  **Loop Layer**: Attaching a kernel Loop Device to that FUSE file.

Once mapped, the kernel reads from `/dev/loopX`, which triggers a read on the FUSE file, which triggers a network request to the backup server.

## Module Breakdown

* **`fuse_loop.rs`**: The high-level logic. It manages the `FuseLoopSession`, handling the FUSE request loop and the lifecycle of the loop device mapping.
* **`loopdev.rs`**: The low-level driver. It uses `ioctl` to talk to `/dev/loop-control` and `/dev/loopX` to create, configure, and teardown loop devices.
