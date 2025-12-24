# File Analysis: `tests/pxar.rs`

## Purpose
Integration testing for the `pxar` binary. Instead of unit testing individual functions, this test compiles the binary and runs it as a subprocess to verify end-to-end behavior.

## Test Workflow (`run_test`)

1.  **Preparation**:
    * Defines a source directory (`tests/catar_data`) containing various file types.
    * Defines paths for the output archive (`tests/archive.mpxar`) and extraction target (`tests/catar_target`).

2.  **Creation**:
    * Invokes `cargo run --bin pxar -- create ...`.
    * Captures the output archive.

3.  **Extraction**:
    * Invokes `cargo run --bin pxar -- extract ...`.
    * Restores the archive to the target directory.

4.  **Verification (Rsync)**:
    * This is a clever verification step. Instead of writing custom diff logic, it invokes `rsync` with:
        * `--dry-run`: Don't actually copy anything.
        * `--itemize-changes`: Output a concise code for every difference found.
        * `--archive`: Compare permissions, times, owners, etc.
    * If `rsync` outputs any lines, it means there is a difference between the source and the restored data, causing the test to panic.

5.  **Cleanup**:
    * Deletes the generated archive and the target directory to leave the workspace clean.
