# File Analysis: `completion.rs`

## Purpose
Generates shell completion scripts (e.g., for bash or zsh).

## Functionality
* It likely imports the CLI schemas (via `proxmox_router`) and uses a helper to output completion definitions.
* This allows users to press `TAB` in their terminal to autocomplete commands like `proxmox-backup-manager datastore ...`.
