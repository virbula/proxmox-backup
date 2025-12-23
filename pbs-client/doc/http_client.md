# Source File Analysis: `http_client.rs`

## Overview
This file provides the networking foundation for the PBS client. It implements a wrapper around the `hyper` crate to handle HTTP/2 connections, authentication, and specific Proxmox Backup Server API requirements.

## Key Structures

### `HttpClient`
* A high-level client that manages the connection state, authentication info, and configuration.

### `ProxyConfig`
* Struct to hold proxy configuration (host, port, credentials).

## Core Functionality

### 1. Connection Management
* Establishes TLS connections.
* Upgrades connections to HTTP/2 (`h2`).
* Handles certificate validation (custom CAs or system roots).

### 2. Authentication
* **Ticket/Token Handling**: Manages the `Authorization` header. It likely handles the Proxmox specific auth ticket format or API tokens.
* **CSRF Prevention**: May handle CSRF tokens if interacting with the API where required (though often less relevant for pure API clients compared to browsers).

### 3. Request Helper
* Provides wrapper methods (`get`, `post`, `put`, `delete`) that automatically inject authentication headers and handle common error codes.
* **Rate Limiting/Retry**: Logic to handle transient network failures or server busy responses.

## Dependencies
* `hyper`: The underlying HTTP library.
* `openssl` / `native-tls`: For TLS encryption.
* `serde`: For JSON serialization of API bodies.