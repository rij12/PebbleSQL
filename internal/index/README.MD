## Blink Tree 

A Blink tree is a version of B+Tree but handles on concurrency very well. 

#### Properties
* Locking is done at a node level 
* Internal nodes (like the root) have keys and pointers to child nodes. 
* Leaf nodes store actual values (or pointers to values) and include a * pointer to their right sibling — that's the B-link.
* The rightward arrows (----->) show the sibling links used to support concurrent access and simplify re-balancing.
* Each node has its own write and read lock. Which allow for concurrent Reads 

Example of a Blink Tree 
```            
             +--------+        +--------+        +--------+
Root  --->   |  20 |* | -----> |  40 |* | -----> |  60 |* |
             +--------+        +--------+        +--------+
                /                  |                  \
               /                   |                   \
     +--------+--------+   +--------+--------+   +--------+--------+
     |  5  | 10 | 15 |*|   | 25 | 30 | 35 |*|   | 45 | 50 | 55 |*|
     +-----------------+   +-----------------+   +-----------------+

(* = right sibling pointer)
```

### Insert 

1. Traverse to Leaf Node:
* Follow child pointers from root to the appropriate leaf node.
* At each level, acquire a read lock on the current node, then a read lock on the child.
* Upgrade to write lock at the leaf when ready to modify.

2. Insert the Key:
* If the node has room, insert the key and release the lock.
* If node is full, perform a split:
* Allocate a new right sibling.
* Move half the keys to the new node.
* Set the new node’s B-link to point to the old node’s right sibling.
* Update the old node’s B-link to point to the new node.
* Push the separator key up to the parent.

3. Update Parent:
* Recursively insert separator key up the tree.
* If the root splits, create a new root.
* Concurrency Benefit: Because of the right sibling pointer, other threads can continue traversing or inserting even if a split is happening on a nearby node.

### Delete 

1. Find the Key:
* Traverse the tree as in insert, using read locks.
* Acquire Write Lock:
* Lock the target leaf node and remove the key.
* If the node still has enough keys, you're done.

2. Re-balance (if needed):
* If the node is under-full, consider merging with or borrowing from a sibling.
* Update B-links as necessary.
In high-concurrency environments, deletion is usually lazy or deferred to avoid locking too many nodes.

### Search 

1. Start at Root:
* Use a read lock at each level.
* Descend through internal nodes by comparing keys.

2. At Leaf:
* Search for the target key.
* If key not found and the search key is greater than the largest key in the node, follow the B-link to the next sibling and repeat.
Readers never block, even if a writer is in the middle of modifying the tree — they can just follow the B-links to continue safely.