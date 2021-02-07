# miniredis
mini redis server.
Started from forking https://gist.github.com/coleifer/dbbedc287605dcc22990a6e549de9f36

Will add a queue mechanism into this mini redis server.

Also add MULTI/EXEC for transaction (atomic execution)


Version 0.2
TODO:
1, redis-benchmark
2, BGET: block get, and remove once received.
3, SETLENGTH: limit the length of list
4, MULTI/EXEC (client)
5, FLUSHALL: remove all.
6, SELECT: maybe decline this request. User can achieve this by running multiple instances in different ports.
