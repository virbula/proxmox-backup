# File Analysis: `crypt_reader.rs`

## Overview
**Role:** Transparent Decryption Stream

`crypt_reader.rs` provides a wrapper around an `AsyncRead` stream that decrypts data on-the-fly. It is primarily used when restoring older backup formats or specific encrypted logs that are not stored as standard `DataBlob`s.

## Mechanism
* **Algorithm:** AES-256-GCM (Galois/Counter Mode).
* **Buffering:** AES-GCM is a block cipher. The reader must buffer incoming data until it has a full block (or the authentication tag) before it can verify the tag and release the plaintext.
* **Safety:** It ensures that if the authentication tag (integrity check) at the end of the stream is invalid, the stream returns an error, preventing the application from trusting corrupted or tampered data.

---
*Analysis generated based on source code.*