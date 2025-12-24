# File Analysis: `network.rs`

## Purpose
A high-level interface for reading and writing the system network configuration (`/etc/network/interfaces`).

## Functionality
* **Parsing**: Reads the Debian-style interfaces file.
* **Types**: Supports IPv4/IPv6, Bonds (LACP, Active-Backup), Bridges, and VLANs.
* **Validation**: Ensures configuration consistency before applying changes.
