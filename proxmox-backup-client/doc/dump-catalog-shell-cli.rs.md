# Proxmox Backup Client - bin/dump-catalog-shell-cli.rs Analysis

## Overview
This file (`bin/dump-catalog-shell-cli.rs`) defines a **standalone binary** separate from the main `proxmox-backup-client` executable.

## Purpose
It exposes the `catalog_shell` functionality as a distinct executable.
- **Use Case**: This is often used for debugging, scripting, or in restricted environments where the full client suite isn't needed, but the ability to inspect a catalog index is required.

## Implementation
- It imports `pbs_client::catalog_shell::catalog_shell_cli`.
- It sets up a minimal `CliCommandMap` containing only the catalog-related commands.
- It reuses the exact same logic as the main client but packaged independently.
