# File Analysis: `file_formats.rs`

## Overview
**Role:** Binary Format Constants

`file_formats.rs` serves as the "Source of Truth" for the on-disk binary formats. It prevents magic numbers from being hardcoded in multiple places.

## Key Definitions
* **Magic Numbers:** 8-byte identifiers at the start of files.
    * `[0, 1, 2, 3]`: Uncompressed Blob.
    * `[0, 1, 2, 4]`: Compressed Blob.
    * `[0, 1, 2, 5]`: Encrypted Blob.
* **Extensions:** Standard file extensions (`.fidx`, `.didx`, `.blob`).
* **Header Sizes:** Defines the fixed size of headers (e.g., `sizeof(head)`) so readers know exactly where the payload begins.

---
*Analysis generated based on source code.*