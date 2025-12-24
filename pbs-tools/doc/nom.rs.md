# File Analysis: `nom.rs`

## Purpose
Common parsing combinators for the `nom` library.

## Context
PBS uses `nom` for parsing configuration files, headers, and protocol messages. This module extracts common patterns to prevent code duplication.

## Examples
* Parsers for quoted strings.
* Parsers for key-value pairs.
* Parsers for whitespace/comments handling.
