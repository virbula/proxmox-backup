# Proxmox Backup Server Package Build Process

This document outlines how the Debian packages for Proxmox Backup Server are built, based on the configuration found in the `debian/` directory.

## Overview

The build process is orchestrated by `debian/rules`, which is a standard Debian makefile using the `debhelper` suite. It compiles the entire Rust workspace once and then distributes the resulting binaries into separate Debian packages.

## Build Steps

### 1. Dependency Resolution

The `debian/control` file defines the build-time requirements. Before the build starts, the system ensures tools like `cargo`, `rustc`, `debhelper`, and libraries like `libacl1-dev` and `libfuse3-dev` are installed.

### 2. Compilation (The "Build" Phase)

The `rules` file executes the actual compilation in the `override_dh_auto_build` target:

```
override_dh_auto_build:
    cargo build --release --workspace

```

* **`--release`**: Compiles with optimizations enabled.

* **`--workspace`**: Crucially, this tells Cargo to build **all** members of the workspace (server, client, restoration tools, docs) in a single pass.

### 3. Installation (The "Distribute" Phase)

Once compiled, the binaries are installed into a temporary staging area (`debian/tmp`) via the `override_dh_auto_install` target.

### 4. Package Separation (The "Split" Phase)

This is the key mechanism that allows for separate Server and Client packages. The build system (`dh_install`) reads the `.install` files to decide which files from the staging area belong to which final `.deb` package.

* **`proxmox-backup-server.install`**:

  * Selects server-specific binaries: `proxmox-backup-manager`, `proxmox-backup-proxy`, `proxmox-backup-api`, `sg-tape-cmd`.

  * Selects server config templates and assets.

  * **Result**: The `proxmox-backup-server` package.

* **`proxmox-backup-client.install`**:

  * Selects client-specific binaries: `proxmox-backup-client`, `pxar`.

  * **Result**: The `proxmox-backup-client` package.

* **`proxmox-backup-file-restore.install`**:

  * Selects restoration tools: `proxmox-file-restore`, `proxmox-restore-daemon`.

  * **Result**: The `proxmox-backup-file-restore` package.

* **`proxmox-backup-docs.install`**:

  * Selects generated HTML and PDF documentation.

  * **Result**: The `proxmox-backup-docs` package.

### 5. Post-Processing

After the files are distributed, `debhelper` runs several finishing steps defined in `rules`:

* **Stripping**: `dh_strip` removes debug symbols to reduce package size.

* **Dependency Cleanup**: The custom script `debian/scripts/elf-strip-unused-dependencies.sh` is run on binaries to remove unused shared library links (`ldd -u` + `patchelf`).

* **Systemd**: `dh_installsystemd` configures the service units (`.service`, `.timer`).

## Summary: Single vs. Separate Packages

**Does this support separate packages? Yes.**

While the software is built from a **single source tree** (monorepo style), the Debian packaging metadata is explicitly configured to produce **multiple, distinct binary packages**.

This allows administrators to install:

* **Only the Client**: `apt install proxmox-backup-client` (small footprint, no daemon).

* **Only the Restore Tool**: `apt install proxmox-backup-file-restore`.

* **The Full Server**: `apt install proxmox-backup-server`.
