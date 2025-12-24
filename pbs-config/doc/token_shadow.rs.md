# File Analysis: `token_shadow.rs`

## Purpose
Securely manages API Token secrets. Unlike passwords, API tokens often have generated secrets that need to be verified.

## Functionality
* **Storage**: Stores the *hash* of the API token secret in `token.shadow`.
* **Verification**: Provides `verify_secret` to check a provided secret against the stored hash.
* **Separation**: Kept separate from `user.cfg` to restrict read access strictly to the authentication subsystem.
