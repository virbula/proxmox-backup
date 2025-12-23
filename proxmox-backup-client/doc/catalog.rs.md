# Proxmox Backup Client - catalog.rs Analysis

## Overview
The `catalog.rs` module handles interactions with the backup catalog. The catalog is an index allowing users to see what files are inside a backup snapshot without downloading the entire archive.

## Commands

### `dump_catalog`
- **Purpose**: Downloads and prints the file listing of a snapshot to stdout.
- **Workflow**:
    1.  Connects to the `BackupRepository`.
    2.  Locates the manifest for the specific snapshot.
    3.  **Primary Method**: Attempts to download the optimized `catalog.pcat1` binary file.
    4.  **Fallback**: If the binary catalog is missing, it downloads the `.didx` (dynamic index) and uses `pxar_metadata_catalog_dump_dir` to walk the metadata archive and reconstruct the file list.
- **Output**: A text tree of directories and files.

### `catalog_shell`
- **Purpose**: Launches an interactive shell (similar to `ftp` or `sftp`) to browse a remote backup.
- **Workflow**:
    1.  Resolves the manifest.
    2.  Uses `helper::get_pxar_fuse_accessor` to create a virtual filesystem view of the remote archive.
    3.  Instantiates a `Shell` object (from `pbs_client::tools::Shell`) which accepts commands like `ls`, `cd`, `pwd`, and `find`.
- **Key Feature**: It uses a **FUSE-like accessor** but runs entirely in userspace without mounting to the kernel, making it safe and portable.

## Helper Functions
- `dump_catalog_one`: Internal helper to process a single archive within a snapshot (e.g., `root.pxar`).
