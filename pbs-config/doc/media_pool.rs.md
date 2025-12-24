# File Analysis: `media_pool.rs`

## Purpose
Part of the Tape Backup system. Defines "Media Pools" in `media-pool.cfg`.

## Concepts
* **Media Pool**: A logical group of tapes.
* **Allocation Policy**: How to assign new tapes (e.g., "always use empty tapes" vs "continue appending").
* **retention**: How long data on a tape is protected from overwrite.
* **encryption**: Links to an encryption key fingerprint to encrypt data written to tapes in this pool.
