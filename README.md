# KeyValueDB
无聊练手

Just for fun.


A naive implementation of Key-Value based database.

# How to run
run `Server.kt` to start the Database

run `Client.kt` to communicate with the DB.

Supporting operations: `set`, `get` and `del`

# Example
```
set language kotlin # stores the key-value pair (language, kotlin)
> done
get language # get the associated value with the key `kotlin`
> kotlin
del language # delete the key-value pair from the DB
> done 
```


# Index file structure
![img.png](img.png)
