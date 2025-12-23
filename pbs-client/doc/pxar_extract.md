# Source File Analysis: `pxar/extract.rs`

## Overview
This module provides the functionality to **extract** (restore) a PXAR (Proxmox Archive) archive onto the local filesystem. It bridges the gap between the `pxar` library format and the OS file system calls.

## Key Functions

### `extract_archive`
* The main entry point. Iterates over the entries in a PXAR archive and restores them to a target directory.

### `extract_entry`
* Handles a single entry (File, Directory, Symlink, Device, FIFO, etc.).
* **Logic**:
    * **Directories**: Creates the directory.
    * **Files**: Creates the file and writes the contents.
    * **Metadata**: Restores ownership (UID/GID), permissions (mode), and timestamps (mtime/atime/ctime).
    * **Extended Attributes (xattrs)**: Restores extended attributes if present in the archive.
    * **ACLs**: Restores Access Control Lists.

## Notable Logic
* **Error Handling**: It is designed to be robust. If it can't restore specific metadata (e.g., due to lack of permission), it might log a warning rather than failing the whole restore, depending on flags.
* **Security**: Care must be taken to prevent directory traversal attacks (writing outside the target directory), though the PXAR format usually handles relative paths.

## Dependencies
* `pxar`: The core library for parsing the archive format.
* `nix`: For POSIX filesystem calls (chmod, chown, mknod).