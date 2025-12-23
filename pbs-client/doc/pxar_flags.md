# Source File Analysis: `pxar/flags.rs`

## Overview
Defines bitflags and constants used within the PXAR format handling.

## Functionality
* **Flags**:
    * `O_NOATIME`: Flag to not update access time during backup.
    * File attribute flags (DOS attributes, Linux attributes like `immutable`, `append-only`).
* **Mapping**: Maps OS specific flags (from `stat` or `ioctl`) to PXAR format flags.

## Dependencies
* `bitflags`: Crate for typesafe bitmask handling.