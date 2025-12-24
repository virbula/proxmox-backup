# File Analysis: `src/main.rs`

## Purpose
The entry point for the `pxar` binary. It uses the `proxmox-router` library to define a CLI interface and dispatches commands to specific handler functions.

## CLI Commands

### 1. `create`
Creates a new `.pxar` archive from a source directory.
* **Options**:
    * `--all-file-systems`: If set, recurses into mount points. Otherwise, stays on the same filesystem.
    * `--no-xattrs`, `--no-acls`, `--no-fcaps`: Flags to disable archiving specific metadata.
    * `--payload-output`: Supports creating split archives (metadata in one file, raw data payloads in another).
    * `--exclude`: Accepts glob patterns to exclude files.
* **Implementation**: Uses `pbs_client::pxar::create_archive`. It sets up a `PxarCreateOptions` struct and streams data to the output writer (stdout or file).

### 2. `extract`
Extracts a `.pxar` archive to a target directory.
* **Options**:
    * `--pattern`: Extract only files matching specific patterns.
    * `--overwrite`: Allow overwriting existing files.
    * `--strict`: Fail on metadata errors (like unsupported xattrs).
* **Implementation**: Opens the archive file (and optional payload file) and calls `extract_archive_from_reader`.

### 3. `list`
Lists the contents of an archive.
* **Implementation**: Iterates through the root accessor of the archive, printing paths. Can optionally print file modes and sizes.

### 4. `mount`
Mounts the archive as a FUSE filesystem.
* **Implementation**: Delegates to the `pbs_pxar_fuse` library to run the FUSE session.

## Error Handling
The tool uses `anyhow::Error` for consistent error reporting to the console.
