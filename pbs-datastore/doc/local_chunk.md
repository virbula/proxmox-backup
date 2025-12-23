# File Analysis: `local_chunk_reader.rs`

## Overview
**Role:** The Standard Data Fetcher

`local_chunk_reader.rs` is the concrete implementation of `AsyncReadChunk` for standard on-disk Datastores. It serves as the "Pipeline" that transforms raw disk files into usable data.

## Struct: `LocalChunkReader`

### Fields
* **`store`**: A reference to the `ChunkStore` (to find the file).
* **`crypt_config`**: (Optional) The encryption keys.
* **`datastore_name`**: For logging purposes.

## The Read Pipeline
When `read_chunk(digest)` is called, the reader performs the following steps in order:

1.  **Fetch:** Calls `chunk_store.load_chunk(digest)` to get the file content.
2.  **Verify Header:** Checks the `DataBlob` header (Magic Number).
3.  **Verify CRC:** Calculates the CRC32 of the loaded data to ensure the disk didn't bit-rot.
4.  **Decrypt (If required):**
    * If the blob is encrypted and `crypt_config` is present: Decrypts using AES-256-GCM.
    * If the blob is encrypted but *no* key is present: Returns an error (cannot read data).
5.  **Decompress:** If the blob is compressed (Zstd), it decompresses it.
6.  **Return:** Returns the clean, original plaintext data.

---
*Analysis generated based on source code.*