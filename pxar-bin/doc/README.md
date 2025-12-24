# Proxmox Archive Utility (`pxar-bin`) Analysis

This directory contains the source code for the `pxar` command-line tool. This tool is a standalone utility for creating, extracting, listing, and inspecting Proxmox Archive (`.pxar`) files. It serves as a reference implementation and a handy tool for interacting with the archive format outside of the full Proxmox Backup Server environment.

## Overview

The `pxar` tool exposes the functionality of the `pxar` library to the command line. It supports creating archives from directories, extracting archives to the filesystem, listing archive contents, and mounting archives via FUSE (if supported).

## Files

* **`src/main.rs`**: The main entry point and implementation of the CLI commands (`create`, `extract`, `list`, `mount`).
* **`tests/pxar.rs`**: Integration tests ensuring data integrity during creation and extraction cycles.
