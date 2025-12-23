# Source File Analysis: `pxar/metadata.rs`

## Overview
Handles the conversion between Rust's `std::fs::Metadata` (or `nix::sys::stat::FileStat`) and the metadata structures required by the PXAR format.

## Functionality
* **Extraction**: Reading `uid`, `gid`, `mode`, `mtime`, `atime`, `ctime`, `fcaps` (capabilities), `xattr`, `acls` from the filesystem.
* **Application**: Applying these attributes back to files during restore.
* **Platform Specifics**: Handles Linux-specific metadata fields which might not exist on other OSes.

## Dependencies
* `nix`: For low-level system stat calls.