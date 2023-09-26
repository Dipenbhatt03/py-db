# Py-DB
Inspired by sqlite, an attempt at building a ACID compliant embedded database.



### First PR
1. Contains persistence to disk
2. Contains basic searching
3. Contains insertion

##### Limitations
1. Flushes entire table to disk even when a single row has changed
2. Loads entire file into memory before doing operation
3. For searching does a linear search, making it extremely slow


### Second PR
1. Implemented binary tree
2. Row insert immediately flushes to disk for persistence
3. Searching happens through binary tree
4. Wrote tests to validate implementation

##### Pending Improvements
    1. Tree becomes unbalance with certain type of data resulting in a worst case O(N) time complexity.
        Need AVL or Red Black tree implementation
    2. Insertion have become very slow, due to page fetch and close during every insert.
       Two things come to mind to solve this:
       1. Keep the file open and dont close it to persist in disk, rather do flush
       2. Implement page system, where one page can hold multiple rows and then we do operation on a page rather than a row
           and flush page after that
    3. When scanning binary tree, keep the already scanned nodes in memory.
        So that next time the same row is asked for, we dont have to fetch it from disk

### Third PR
Implemented the improvements listed in 2nd pr
1. Enabling file descriptor for the database file to do io rather than opening closing it all time
2. Implemented avl tree
3. Updated table page layout, by saving offset of root row, coz in avl tree root row might change during rebalancing
4. Added helper method to Row Class
5. Added SmallInt col to represent 2 bit integer and updated IntCol to inherit from int class
6. Fixed and updated test to validate all rotations of AVL tree
7. Set row offset as negative to denote offset not set instead of -1

##### Pending Improvements
    1. Insertion can still be improved further, maybe implementing B-Tree would help
    2. Need to implement pager module. Although first we need to validate the point that just playing around with rows
       involved too much io thus requiring the need of pager. Check the possibility that maybe too much io is also
       leading to bad relatively bad insert performance. 