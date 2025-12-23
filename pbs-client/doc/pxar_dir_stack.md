# Source File Analysis: `pxar/dir_stack.rs`

## Overview
A utility data structure used during the creation or extraction of PXAR archives to track the directory hierarchy.

## Functionality
* **Stack Management**: When walking a directory tree (recursively), this stack keeps track of open file descriptors for parent directories.
* **Security**: Helps in ensuring that file operations are performed relative to the correct parent directory (using `openat` style logic) to avoid race conditions or symlink attacks.