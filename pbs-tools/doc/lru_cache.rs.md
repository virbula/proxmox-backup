# File Analysis: `lru_cache.rs`

## Purpose
A synchronous implementation of a Least Recently Used (LRU) cache.

## Functionality
* **Capacity**: Fixed size. When full, inserting a new item evicts the least recently accessed item.
* **Access Tracking**: Every `get` or `insert` promotes the item to the "most recently used" position.
* **Backing**: Typically implemented using a `HashMap` for lookups and a `LinkedList` (or similar structure) for ordering.
