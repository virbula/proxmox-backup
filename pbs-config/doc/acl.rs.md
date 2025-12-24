# File Analysis: `acl.rs`

## Purpose
Manages the Access Control List (ACL) system. It is responsible for parsing `acl.cfg` and determining if a specific user or token has permission to perform an action on a specific object path.

## Key Features

* **ACL Tree**: Implements a tree structure where permissions on a parent path (e.g., `/datastore`) propagate to children (e.g., `/datastore/store1`), unless overridden.
* **Role Mapping**: Maps high-level Roles (e.g., `Admin`, `DatastoreReader`) to low-level Privileges (bitmasks).
* **Atomic Updates**: Provides functions to atomically update the ACL file using lockfiles.
* **Check Logic**: The `check_acl` function is the core gatekeeper used throughout the API to authorize requests.
