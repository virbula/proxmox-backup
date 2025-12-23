# File Analysis: `data_blob.rs`

## Overview
**Role:** Data Container & Format Enforcer

`data_blob.rs` defines the envelope for every piece of data stored in PBS. Whether it's a raw data chunk, a config file, or a manifest, it is wrapped in a `DataBlob`.

## Key Features

### 1. Magic Numbers & Headers
Every blob starts with a Magic Number identifying its type:
* `UNCOMPRESSED_BLOB_MAGIC_1_0`
* `COMPRESSED_BLOB_MAGIC_1_0` (Zstd)
* `ENCRYPTED_BLOB_MAGIC_1_0` (AES-GCM)

### 2. Verification
* **CRC32:** The header contains a CRC checksum to detect bit-rot or partial writes immediately upon loading.
* **Length Checks:** Ensures the file size on disk matches the expected data length.

### 3. Encryption/Compression Handling
* **Builders:** Provides a `DataBlobBuilder` to easily construct blobs.
* **Loaders:** `load_from_reader` automatically detects the magic number, decompresses the data (if needed), and decrypts it (if a key is provided), abstracting this complexity away from the caller.

---
*Analysis generated based on source code.*