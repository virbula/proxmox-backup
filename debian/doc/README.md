# Debian Packaging Directory

This directory contains the configuration and scripts required to build the Proxmox Backup Server software into Debian packages (`.deb`). It defines the build process, dependencies, file locations, and system integration (systemd, udev, shell completions).

## Core Packaging Files

* **`changelog`**
  * **Purpose**: Records the history of the package versions (`4.1.1-1`, `4.1.0-1`, etc.), release distribution (e.g., `trixie`), and changes made by the maintainers.
  * **Content**: List of changes like "datastore: s3: use owner from local store cache" and "api: notification: correctly enumerate known push-sync-jobs".

* **`control`**
  * **Purpose**: The central metadata file. It defines the **Source** package and multiple **Binary** packages produced from this source (`proxmox-backup-server`, `proxmox-backup-client`, etc.).
  * **Content**: Lists build dependencies (`rustc`, `cargo`, `debhelper`, `libacl1-dev`, etc.) and runtime dependencies for each package.

* **`copyright`**
  * **Purpose**: Legal compliance file.
  * **Content**: Lists the licenses (AGPL-3.0, Apache-2.0, MIT) and copyright holders for the Proxmox code and all vendored Rust crates linked into the binaries.

* **`rules`**
  * **Purpose**: The Makefile that drives the build process.
  * **Content**: Instructions to compiling the Rust binaries, running tests, installing files into the package directories, and stripping debug symbols. It specifically calls `scripts/elf-strip-unused-dependencies.sh`.

* **`source/format`**
  * **Purpose**: Defines the source package format (likely `3.0 (native)` or `3.0 (quilt)`).

* **`source/lintian-overrides`**
  * **Purpose**: Suppresses specific warnings from the Debian package checker (Lintian) regarding the source package.

## Binary Package Definitions (.install)

These files tell the build system which files go into which `.deb` package.

* **`proxmox-backup-server.install`**: Files for the main server package (daemons, configuration templates, JS assets).
* **`proxmox-backup-client.install`**: Files for the standard client (`proxmox-backup-client`, `pxar`).
* **`proxmox-backup-client-static.install`**: Files for the statically linked client package (useful for recovering systems without installing dependencies).
* **`proxmox-backup-file-restore.install`**: Files for the single-file restore tool and daemon (`proxmox-file-restore`, `proxmox-restore-daemon`).
* **`proxmox-backup-docs.install`**: Documentation files (HTML, PDF).

## Maintainer Scripts (Lifecycle)

Scripts that run during package installation (`postinst`), removal (`prerm`), or updates.

* **`postinst`** (Generic): Likely applies to the main package (`proxmox-backup-server`), handling user creation (`backup` user) and initial setup.
* **`prerm`** (Generic): Handles stopping services before removal.
* **`proxmox-backup-file-restore.postinst`**: Specific setup for the file restore package.
* **`proxmox-backup-file-restore.triggers`**: Registers triggers to update system states (like man-db) when this package is installed.
* **`proxmox-backup-server.maintscript`**: Helper to handle renaming or removing configuration files during package upgrades.

## Shell Completions (.bc & .bash-completion)

These files provide tab-completion for the command-line tools. The `.bc` extension in this context typically refers to bash completion snippets managed by `dh_bash-completion`.

* **Client Tools**:
  * `proxmox-backup-client.bash-completion` / `.bc`
  * `proxmox-backup-client-static.bash-completion`
  * `pxar.bc` (for the archive tool)

* **Server & Admin Tools**:
  * `proxmox-backup-server.bash-completion`
  * `proxmox-backup-manager.bc`
  * `proxmox-backup-debug.bc`
  * `proxmox-backup-file-restore.bash-completion` / `.bc`

* **Tape Tools**:
  * `proxmox-tape.bc`
  * `pmt.bc` (Proxmox Magnetic Tape tool)
  * `pmtx.bc` (Proxmox Tape Changer tool)

## System Configuration & Integration

* **`proxmox-backup-server.udev`**: Udev rules, likely to manage permissions for tape drives or other hardware devices used by the server.
* **`proxmox-backup-docs.links`**: Creates symbolic links for documentation files (e.g., linking a generic man page name to a specific tool).

## Build Helpers

* **`scripts/elf-strip-unused-dependencies.sh`**:
  * **Purpose**: Optimization script.
  * **Content**: It runs `ldd -u` on compiled binaries to find unused shared library dependencies and removes them using `patchelf`. This minimizes the package dependency footprint.

## Lintian Overrides

These files suppress specific warnings from the Debian package checker that are known false positives or intentional design choices.

* `lintian-overrides` (Global)
* `proxmox-backup-client-static.lintian-overrides`
* `proxmox-backup-docs.lintian-overrides`

## Package Build & Separation

### Build Process
The build is orchestrated by the `rules` file, which is a standard Debian `Makefile`. The typical flow is:
1.  **Dependencies**: `debhelper` and `cargo` ensure all build requirements are met.
2.  **Compilation**: `cargo build --release` compiles all binaries from the source tree at once.
3.  **Optimization**: Scripts like `elf-strip-unused-dependencies.sh` optimize the resulting binaries.

### Server vs. Client Separation
This code specifically supports creating **separate packages**.
* **Single Source**: All code is built from the same Rust workspace.
* **Split Packaging**: The `.install` files are the key mechanism here.
    * `proxmox-backup-server.install` grabs only the server binaries (`proxmox-backup-proxy`, etc.).
    * `proxmox-backup-client.install` grabs only the client binaries (`proxmox-backup-client`, `pxar`).
* **Outcome**: A user can run `apt install proxmox-backup-client` to get just the backup tool without installing the heavy server daemons.
