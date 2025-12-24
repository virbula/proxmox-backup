# File Analysis: `domains.rs`

## Purpose
Manages Authentication Realms stored in `domains.cfg`.

## Functionality
* **Realm Types**: Supports `pam` (Linux PAM), `pbs` (Internal Proxmox auth), and `openid` (OIDC/Keycloak).
* **Default Realm**: Handles the logic for the default realm if none is specified during login.
* **Sync**: For OpenID/LDAP (if added), it might hold configuration for syncing users.
