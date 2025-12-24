# File Analysis: `lib.rs`

## Purpose
The root of the `pbs-tape` library. It exposes the public modules and defines shared data structures.

## Key Definitions

* **`BlockHeader`**: The C-repr struct definition for the header added to every tape block.
    * `magic`: `[u8; 8]`
    * `flags`: `u32`
    * `size`: `u32`
    * `seq_nr`: `u32`
* **`TapeWriteSettings`**: Configuration struct passed to writers.
