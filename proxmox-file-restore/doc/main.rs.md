# File Analysis: `main.rs`

## Purpose
This file serves as the command-line interface (CLI) entry point for the `proxmox-file-restore` binary. It parses user arguments and delegates execution to the underlying drivers.

## Key Features

### CLI Parsing
Likely uses the `clap` crate or Proxmox's wrapper `proxmox-router` to define subcommands:
* `list`: Browse the contents of a backup snapshot.
* `extract`: Recover specific files or directories.
* `status`: Show the status of running restore VMs.
* `stop`: Forcefully stop running restore VMs.

### Driver Dispatch
It initializes the `QemuBlockDriver` (or others if configured) and passes the parsed arguments to the driver's `list` or `data` methods.

### Output Formatting
* **Text Mode**: Prints human-readable file lists for the terminal.
* **JSON Mode**: Formats output as JSON, which is essential when this tool is called by the Proxmox web GUI to populate the file browser.

### Security & Privileges
The `main` function likely handles privilege dropping or user switching, ensuring that the heavy lifting (QEMU) runs with appropriate permissions (often `backup:backup` user), accessing keys and datastores securely.
