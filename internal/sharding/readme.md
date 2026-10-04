Implements consistent hashing ring in golang.

This package sits inside the Go proxy/router and map any incoming userID (UUIDv7) to one of the 3 Postgres shards (pg-shard-0, pg-shard-1, pg-shard-2).

A simpler option could have been to just use modulo (userID % 3), but if we add another shard, it would cause 75% of the keys to remap to new shards.

With a Consistent Hash Ring, the keys and nodes are placed on a $2^{32}-1$ integer ring space. Adding or removing a shard only shifts $\frac{1}{N}$ (25%) of the keys to the new node, leaving 75% of user data where it was.

Additionally, we divide the three shards into several virtual nodes (configurable, defaults to 50). This is done to ensure we avoid data "hotspots" and keys are distributed more evenly across the physical shards. 