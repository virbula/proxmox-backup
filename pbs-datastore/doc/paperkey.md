# File Analysis: `paperkey.rs`

## Overview
**Role:** Master Key Export Utility

`paperkey.rs` handles the formatting of encryption keys for offline physical storage ("Paper Backups"). Losing the master key means losing all data, so a physical copy is essential.

## Functionality
* **HTML Generation:** Generates a standalone HTML page containing the key details.
* **QR Codes:** embeds an SVG QR code of the key JSON, allowing for easy restoration by scanning the paper with a mobile device or webcam.
* **Formatting:** Ensures the key is presented in a human-readable font (often monospaced) to distinguish ambiguous characters (like `O` vs `0` or `l` vs `1`) if the user needs to type it in manually.

---
*Analysis generated based on source code.*