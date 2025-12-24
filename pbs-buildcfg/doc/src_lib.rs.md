# File Analysis: `src/lib.rs`

## Purpose
Exposes system paths and version information as public constants.

## Versioning
* **`PROXMOX_BACKUP_CRATE_VERSION`**: Full version from `Cargo.toml`.
* **`PROXMOX_PKG_REPOID`**: The git commit hash (injected by `build.rs`).

## System Paths
Centralizes the locations of critical system directories. Uses macros to possibly support compile-time overrides (though currently hardcoded strings are mostly used).

* **`CONFIGDIR`**: `/etc/proxmox-backup`
* **`JS_DIR`**: `/usr/share/javascript/proxmox-backup`
* **Run Directory**: `/run/proxmox-backup` (via macro)
* **Log Directory**: `/var/log/proxmox-backup` (via macro)
* **Cache Directory**: `/var/cache/proxmox-backup` (via macro)

## Specific Files
Defines full paths for specific operational files:
* **`PROXMOX_BACKUP_API_PID_FN`**: Path to the API server PID file.
* **`PROXMOX_BACKUP_PROXY_PID_FN`**: Path to the Proxy server PID file.
* **`PROXMOX_BACKUP_INITRAMFS_FN`**: Location of the restore VM initramfs.

## Macros
* **`configdir!`**: A helper macro that takes a filename and prepends the configuration directory path.
