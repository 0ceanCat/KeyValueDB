# KeyValueDB
无聊练手

Just for fun

A naive implementation of a key-value based database.

I've been working on implementing a basic key-value database, trying out algorithms and data structures such as `SSTable` and `LSM`.

So far `SSTable` is more or less implemented. The data is written to disk in sorted order when the size of the in-memory table reaches a certain threshold.

A merger thread is run in the background to detect index files on disk and merge them when needed.        
