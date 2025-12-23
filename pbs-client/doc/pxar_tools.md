# Source File Analysis: `pxar/tools.rs`

## Overview
Helper functions for PXAR operations.

## Functionality
* **Format Verification**: Utilities to check if a file is a valid PXAR archive (checking magic numbers).
* **Path Handling**: Utilities to sanitize paths stored in the archive before extracting them to disk (preventing `../` traversals).