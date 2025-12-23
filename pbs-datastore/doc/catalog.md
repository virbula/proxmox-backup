# File Analysis: `catalog.rs`

## Overview
**Role:** Backup Indexing & Search Engine

`catalog.rs` implements the **File Catalog**, a specialized indexing system. It provides a lightweight, searchable index of the file system hierarchy contained within a backup archive (specifically `.pxar` archives).

While `manifest.rs` tracks the high-level blobs (e.g., "root.pxar"), `catalog.rs` tracks the **individual files inside those blobs** (e.g., `/etc/passwd`, `/var/www/index.html`). This allows the Proxmox Backup Server (PBS) web UI to browse backup contents and perform single-file restores without needing to download or scan the entire multi-gigabyte archive.

## Core Abstractions

### 1. `DirEntry`
* **Concept:** The fundamental unit of the catalog. It represents a single node in the file system tree.
* **Attributes:**
    * **Name:** The filename (bytes).
    * **Attr:** File attributes (mode, permissions).
    * **Mtime:** Modification time (essential for UI display).
    * **Size:** File size (for non-directories).
    * **Type:** Defines what the entry is (see `CatalogEntryType`).
* **Purpose:** It strips away the actual file content and strictly keeps the metadata needed for browsing.

### 2. `CatalogEntryType`
An enum defining the structure of the entry:
* `Directory`: Marks the start of a folder.
* `File`: A standard file.
* `Symlink`: A symbolic link.
* `BlockDevice` / `CharDevice` / `Fifo` / `Socket`: Special file system nodes.
* `Hardlink`: References to existing inodes.

### 3. `CatalogWriter`
* **Role:** The "Encoder".
* **Usage:** Used during the backup process. As the client streams the `pxar` archive to the server, a parallel process feeds metadata into the `CatalogWriter`.
* **Mechanism:** It writes entries sequentially to a binary stream. It handles the hierarchy by writing a `Directory` entry, then its contents, and implicitly "closing" the directory when the tree traversal moves back up.

### 4. `CatalogReader`
* **Role:** The "Decoder".
* **Usage:** Used by the API and Restore wizards.
* **Mechanism:** It reads the binary catalog file (`.cat`) and provides methods to navigate the tree, find specific files, or list the contents of a specific directory.

---

## Architectural Relationships

The Catalog sits between the high-level metadata and the raw data:

| Related File | Interaction |
| :--- | :--- |
| **`backup_info.rs`** | The `BackupDir` manages the `.cat` file (blob) on disk. When `backup_info.rs` sees a file named `catalog.pcat1.blob`, it knows this corresponds to `catalog.rs`. |
| **`data_blob.rs`** | The binary output of `CatalogWriter` is usually wrapped in a `DataBlob` for compression (Zstd) and encryption before being written to disk. |
| **`pxar` (External)** | The Catalog is essentially a "Shadow" of the `.pxar` archive. It mirrors the structure exactly but excludes the payloads. |
| **`api` (Server)** | The API uses `catalog.rs` to implement the `/file-restore` endpoints, allowing the frontend to lazily load directory listings. |

---

## Data Structure & Format

The catalog uses a custom **Binary Serialization Format** optimized for compactness and speed. It is not JSON or XML.

1.  **Compactness:** It encodes file types and attributes into minimal byte sequences.
2.  **Tree Structure:** The catalog is stored as a Depth-First Traversal dump.
    * *Directory Start* (`/etc`)
    * *File* (`passwd`)
    * *File* (`shadow`)
    * *Directory End*
3.  **Optimization:** Because it is a linear dump, finding a specific file deep in the tree might require scanning, but `catalog.rs` includes logic to skip over subdirectories efficiently when searching.

---

## Key Workflows

### 1. The Backup Process (Writing)
1.  The Backup Client starts scanning the local filesystem.
2.  It sends file data (chunks) to `chunk_store.rs`.
3.  Simultaneously, it sends metadata to the `CatalogWriter`.
4.  `CatalogWriter` serializes `DirEntry` structs into a binary buffer.
5.  When the backup finishes, this buffer is signed and saved as `index.cat.blob` (or similar) inside the `BackupDir`.

### 2. The File Restore (Reading)
1.  User clicks "Browse" on a snapshot in the UI.
2.  `catalog.rs` (`CatalogReader`) opens the `.cat` blob.
3.  The UI requests listing for `/`. The Reader scans the top-level entries.
4.  User clicks `/var`. The Reader scans until it matches the entry `/var`.
5.  It then iterates purely over the *children* of `/var` to return the list.
6.  This ensures that even for backups with millions of files, browsing is responsive because the server only reads the relevant section of the index.

---

## Why is this file critical?

Without `catalog.rs`, Proxmox Backup Server would behave like a "dumb" tape drive. To restore a single 1KB text file from a 10TB backup, you would have to download/mount the entire 10TB archive to find it.

`catalog.rs` transforms PBS from a block-storage system into a **file-aware** backup solution, enabling:
* Granular File Restore.
* Fast Search.
* Visual Browsing in the Web UI.

---
*Analysis generated based on the source code provided.*