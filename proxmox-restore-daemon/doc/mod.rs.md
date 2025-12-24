# File Analysis: `proxmox_restore_daemon/mod.rs`

## Purpose
This file acts as the module root for the `proxmox_restore_daemon` library.

## Functionality
It declares the sub-modules so they can be used by `main.rs`.

```rust
pub mod api;
pub mod auth;
pub mod disk;
pub mod watchdog;
```

It ensures that the directory structure maps correctly to the Rust module system hierarchy.
