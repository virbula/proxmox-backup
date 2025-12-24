# Proxmox Backup Server Banner Generator

This Rust program generates the "Welcome" banner displayed on the server's login console (`/etc/issue`). Its primary purpose is to help administrators easily find the URL for the web-based management interface.

## Functionality

1.  **Hostname Discovery**:
    * Calls `uname()` to retrieve the system's nodename.
    * Extracts the short hostname from the FQDN.

2.  **URL Generation**:
    * Resolves the hostname to find all associated IP addresses.
    * Filters out loopback addresses (localhost).
    * Appends the default port **8007**.
    * Formats valid IPs into HTTPS URLs (e.g., `https://192.168.1.50:8007/`).

3.  **File Update**:
    * Constructs a user-friendly message with the list of discovered URLs.
    * Writes this message to `/etc/issue`, updating the pre-login screen shown on the TTY.



The banner looks like this

```
----------------------------------------------------------

Welcome to the Proxmox Backup Server. 

Please use your web browser to configure this server - connect to:

 https://10.0.0.5:8007/

----------------------------------------------------------
```