
# First Commit
1. Contains persistence to disk
2. Contains basic searching
3. Contains insertion

### Limitations
1. Flushes entire table to disk even when a single row has changed
2. Loads entire file into memory before doing operation
3. For searching does a linear search, making it extremely slow