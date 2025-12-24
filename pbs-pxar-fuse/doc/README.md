# Proxmox Backup Server PXAR FUSE (`pbs-pxar-fuse`)

This library implements a FUSE (Filesystem in Userspace) driver for the Proxmox Archive (`.pxar`) format. It allows users and tools to mount `.pxar` archives and browse their contents transparently as if they were a standard read-only part of the OS filesystem.

## Overview

The core purpose of this crate is to bridge the gap between the sequential/indexed nature of a `.pxar` archive and the random-access requests of a standard POSIX filesystem. It relies on the `proxmox-fuse` crate for low-level FUSE communication and the `pxar` crate for decoding the archive format.

## Key Features

* **Read-Only Mounting**: Provides a read-only view of the archive.
* **Asynchronous I/O**: Built on top of `tokio` and `futures` to handle FUSE requests asynchronously, allowing for high concurrency when multiple processes access the mount.
* **Metadata Support**: Fully exposes file metadata, including:
    * File permissions and modes.
    * Ownership (UID/GID).
    * Timestamps (mtime, ctime, atime).
    * Extended Attributes (xattrs).
    * Symlinks and Hardlinks.
* **Efficient Inode Mapping**: Generates stable Inode numbers based on the byte offsets of entries within the archive, ensuring consistency without needing an external database.

## Architecture & Design

### The `Session` Struct
The central entry point is the `Session` struct. It initializes the FUSE connection and manages the event loop (`main_loop`) that listens for kernel requests.

### State Management (`SessionImpl`)
The internal state is protected by an `RwLock` and includes:
* **`accessor`**: An instance of `pxar::accessor::Accessor`. This provides random access to the archive data without needing to read it sequentially.
* **`dir_cache`**: A Least Recently Used (LRU) style cache (implemented via `LruCache` logic or simple maps) for directory contents. This speeds up `readdir` and `lookup` operations by avoiding repeated decoding of directory blocks.
* **`lookups`**: Tracks the reference count of inodes as required by the FUSE protocol to know when an inode can be forgotten.

### Inode Logic
To create a valid filesystem, every file needs a unique ID (Inode).
* **Directories**: Uses the byte offset of the directory definition in the archive.
* **Files**: Uses the byte offset of the file entry combined with a high-bit flag (`NON_DIRECTORY_INODE`) to distinguish them from directories.
* **Root**: The root directory is always mapped to `ROOT_ID` (1).

### Request Handling
The library implements standard FUSE operations:
* **`lookup`**: Resolves a filename in a directory to an Inode. Uses the `dir_cache`.
* **`getattr`**: Reads metadata from the archive entry (decoding `stat` info).
* **`readdir`**: Iterates over entries in a directory, populating the FUSE buffer.
* **`read`**: Reads file content. It maps the requested offset and size to the `payload` section of the PXAR entry and reads directly from the source reader.
* **`readlink`**: Reads the target of a symbolic link.
* **`listxattr` / `getxattr`**: Decodes extended attributes stored in the PXAR entry.

## Usage

This library is primarily used by the `proxmox-backup-client` tool to implement the `mount` command:

```bash
proxmox-backup-client mount host:datastore/snapshot.pxar /mnt/mountpoint