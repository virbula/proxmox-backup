# Proxmox Backup Client - main.rs Analysis

## Overview
`main.rs` is the entry point for the binary. It handles the application lifecycle, argument parsing, and command dispatch.

## Key Components

### `main` Function
- Initializes the `tokio` async runtime.
- Sets up the `RpcEnvironment` (logging, user identity).
- Parses command line args using `clap` (via `proxmox_router`).

### `CliCommandMap`
- This is the central registry where all subcommands are linked to their implementation modules.
- **Mappings**:
    - `benchmark` -> `benchmark::benchmark`
    - `change-owner` -> (Inline or imported handler)
    - `group` -> `group::group_cli()`
    - `key` -> `key::cli()`
    - `mount` -> `mount::mount_cmd_def`
    - `catalog` -> `catalog::catalog_cli()`
    - `snapshot` -> `snapshot::snapshot_cli()`

### Global Arguments
- Handles `--repository`: The target PBS server (user@host:datastore).
- Handles `--keyfile`: Path to the encryption key.
- Handles `--output-format`: JSON vs Text.

## Integration
It serves as the shell that invokes all other modules (`benchmark.rs`, `mount.rs`, etc.) based on user input.
