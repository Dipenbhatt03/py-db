
# First Commit
1. Contains persistence to disk
2. Contains basic searching
3. Contains insertion

### Limitations
1. Flushes entire table to disk even when a single row has changed
2. Loads entire file into memory before doing operation
3. For searching does a linear search, making it extremely slow



# Second Commit
1. Implemented binary tree
2. Row insert immediately flushes to disk for persistence
3. Searching happens through binary tree
4. Wrote tests to validate implementation

### Pending Improvements
    1. Tree becomes unbalance with certain type of data resulting in a worst case O(N) time complexity.
        Need AVL or Red Black tree implementation
    2. Insertion have become very slow, due to page fetch and close during every insert.
       Two things come to mind to solve this:
       1. Keep the file open and dont close it to persist in disk, rather do flush
       2. Implement page system, where one page can hold multiple rows and then we do operation on a page rather than a row
           and flush page after that
    3. When scanning binary tree, keep the already scanned nodes in memory.
        So that next time the same row is asked for, we dont have to fetch it from disk

