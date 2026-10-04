Your expense tracker domain model is an ideal candidate for horizontal sharding. It contains a strong Tenant Root Entity (user_id), a Global Reference Entity (ForexRate), and Co-located Hierarchies (Accounts, Transactions, TransactionLineItems, Categories).

Below is a step-by-step tutorial roadmap to set up a Dockerized, multi-shard PostgreSQL simulation with a Go routing proxy using your exact schema.

Step 1: Design the Distributed Data Strategy

Before starting Docker containers, categorize your models by how they will be placed across shards.

1. Co-located Sharded Entities (user_id Key)

⚬ Entities: User, Account, Transaction, TransactionLineItem, Category.
⚬ Strategy: Shard strictly by user_id. Every table (except User itself) already has a user_id foreign key.
⚬ Why: 99% of queries in an expense tracker are scoped to a logged-in user. When user_id = X is hashed to Shard 0, that user's accounts, categories, transactions, and line items all live on Shard 0. This allows PostgreSQL to perform multi-table joins locally on a single shard.

2. Global Reference Entity (Broadcast Replication)

⚬ Entity: ForexRate.
⚬ Strategy: Replicate the ForexRate table completely across all shards.
⚬ Why: Forex rates do not belong to a single user, but account balance calculations require joining Account or TransactionLineItem with ForexRate. Broadcasting ForexRate onto every shard allows every shard to execute currency conversion queries without making cross-network RPC calls.

Step 2: Set Up Local Multi-Shard Infrastructure (Docker)

Spin up an isolated network with three separate PostgreSQL containers acting as independent database nodes.

                  ┌──────────────────────┐
                  │   Go Proxy Router    │
                  └──────────┬───────────┘
                             │
       ┌─────────────────────┼─────────────────────┐
       ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  pg-shard-0  │      │  pg-shard-1  │      │  pg-shard-2  │
│ (Port 5431)  │      │ (Port 5432)  │      │ (Port 5433)  │
└──────────────┘      └──────────────┘      └──────────────┘


What to configure:

1. Docker Network: Create a bridge network (e.g., expense-db-net) so the containers can communicate using container names as hostnames.
2. PostgreSQL Containers: Run 3 instances of postgres:16-alpine. Map their internal port 5432 to distinct host ports (5431, 5432, 5433) for local debugging via psql or a GUI client.
3. Database Initialization: Run the schema migrations on all 3 instances. Each database instance must have the identical SQL DDL structure.

Step 3: Implement the Ring Router in Go

Create a Go package (e.g., pkg/sharding) that maps any incoming user_id to a target database connection.

Overview of what to build:

1. Consistent Hash Ring:
  ⚬ Create a ring data structure in Go that hashes strings (e.g., using fnv or crc32 from standard library).
  ⚬ Map virtual nodes for each shard (shard-0-vnode-1, shard-0-vnode-2, etc.) onto a 2^{32}-1 ring space.
  ⚬ Add a function GetShardNode(userID uuid.UUID) string that hashes the userID string and traverses clockwise on the ring to return the matching shard identifier (shard-0, shard-1, or shard-2).
2. Multi-Shard Connection Manager:
  ⚬ Instead of maintaining a single *sql.DB or *pgxpool.Pool, create a wrapper ShardManager struct containing a map of pools: map[string]*pgxpool.Pool.
  ⚬ Initialize connection pools to all three PostgreSQL containers on application startup.

Step 4: Implement the DataStore Interface Layer

Wrap your DataStore interface implementation (PGDataStore) around the ShardManager so that caller functions do not need to know which shard holds the data.

Method Implementation Guidelines:

1. Targeted Single-Shard Queries (e.g., CreateTransaction, GetAccounts)

⚬ Logic:
  1. Extract the userID parameter from the function arguments.
  2. Call shardKey := ring.GetShardNode(userID).
  3. Fetch the corresponding connection pool: db := shardManager.GetPool(shardKey).
  4. Execute standard SQL query against db.

2. Cross-Shard / User Lookup Queries (e.g., GetUserByUsername)

⚬ Problem: When logging in, you have a username string, but username is not the shard key (user_id).
⚬ Approach A (Scatter-Gather):
  ⚬ Execute SELECT * FROM users WHERE username = $1 across all 3 shards concurrently using errgroup or goroutines.
  ⚬ Return the first non-nil result.
⚬ Approach B (Global Index Table):
  ⚬ Maintain a dedicated mapping table UserAuth (username -> user_id) on a designated primary/router database or key-value store. Look up user_id first, then route directly to the user's shard.

3. Handling Self-Referential & Cross-Entity References (Settles []uuid.UUID)

⚬ Analysis: A transaction on Shard 0 may settle a transaction that belongs to the same user.
⚬ Guarantee: Because both transactions belong to the same userID, both transaction records are guaranteed to reside on Shard 0. The settlement lookup can be resolved using a local WHERE id = ANY($1) query on Shard 0 without cross-shard network overhead.

Step 5: Validate and Test Edge Cases

Once your proxy router and Docker instances are running, write tests to verify your distributed invariants:

1. Data Isolation Test:
  ⚬ Insert User A and User B.
  ⚬ Assert that User A's data exists only in pg-shard-0's disk and User B's data exists only in pg-shard-1's disk using direct psql connections to host ports 5431 and 5432.
2. Scatter-Gather Test for Analytics:
  ⚬ Implement a global admin function (e.g., GetTotalSystemTransactions()).
  ⚬ Verify that the Go proxy correctly fans out queries across all 3 shards concurrently, aggregates the integer counts in Go memory, and returns the total sum.
3. Resharding Simulation:
  ⚬ Add a 4th container (pg-shard-3) to your Docker environment.
  ⚬ Update the Hash Ring in Go and observe how virtual nodes re-assign a slice of key space to shard-3 while leaving the remaining key space intact.

Step 6: Explore Advanced Distributed Patterns

Once basic sharding and routing work, extend your simulation to cover production system design challenges:

1. Broadcast Replications for ForexRate:
  ⚬ Write a background worker in Go that fetches new forex rates once a month and issues a BEGIN...COMMIT transaction across all shards simultaneously so every shard maintains an updated copy of ForexRate.
2. Distributed Transactions (Two-Phase Commit):
  ⚬ Imagine a future scenario where User A transfers money to User B across different shards. Practice implementing PostgreSQL's PREPARE TRANSACTION and COMMIT PREPARED primitives inside your Go proxy to guarantee ACID properties across shard boundaries.
