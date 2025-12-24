# File Analysis: `proxmox_restore_daemon/auth.rs`

## Purpose
Provides a lightweight authentication mechanism for the daemon.

## Context
Since the VM is created dynamically for a specific user session, complex user management (like `/etc/shadow` or LDAP) is unnecessary. However, we must ensure that other processes or users on the host cannot hijack the VSOCK connection.

## Implementation

### Ticket System
* **Setup**: When the VM starts, a random "ticket" (a secret string) is passed to it (often via kernel command line or a file).
* **Verification**: Every API request must include this ticket in the `Authorization` header.
* **Check**: The `check_auth` function compares the provided ticket against the stored secret. If they match, the request is granted root-equivalent access to the data.
