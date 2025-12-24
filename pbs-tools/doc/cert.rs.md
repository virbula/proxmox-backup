# File Analysis: `cert.rs`

## Purpose
Manages X.509 Certificates and Public Key Infrastructure (PKI) tasks.

## Functionality
* **Certificate Generation**: `generate_self_signed_cert`. Creates a valid self-signed certificate and private key for the server instance. This is used to bootstrap TLS support on a fresh install.
* **PEM Handling**: Reading and writing certificates in PEM format.
* **Sanity Checks**: Verifying that a certificate matches a private key.
