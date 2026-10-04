package datastore

import (
	"context"
	"distributed-postgres/internal/sharding"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// the sql schema in init-scripts was mainly for docker to use in setup. Keeping datastore generic to ensure this is
// as much plug and play as possible.

type DistributedEngine struct {
	manager *sharding.ShardManager
}

func NewDistributedEngine(manager *sharding.ShardManager) *DistributedEngine {
	return &DistributedEngine{
		manager: manager,
	}
}

// 1. ROUTED EXECUTION (Single-Shard targeting using shardKey)
// ExecOnShard executes an INSERT/UPDATE/DELETE query on the shard mapped to the given shard key.
func (e *DistributedEngine) ExecOnShard(ctx context.Context, shardKey string, sql string, args ...any) (int64, string, error) {
	pool, shardName, err := e.manager.GetPoolForShardKey(shardKey)
	if err != nil {
		return 0, "", fmt.Errorf("routing failure: %w", err)
	}

	cmdTag, err := pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, shardName, fmt.Errorf("exec error on shard %s: %w", shardName, err)
	}

	return cmdTag.RowsAffected(), shardName, nil
}

func (e *DistributedEngine) QueryRowOnShard(ctx context.Context, shardKey string, sql string, args ...any) (pgx.Row, string, error) {
	pool, shardName, err := e.manager.GetPoolForShardKey(shardKey)
	if err != nil {
		return nil, "", fmt.Errorf("routing failure: %w", err)
	}

	return pool.QueryRow(ctx, sql, args...), shardName, nil
}

// QueryOnShard executes a multi-row query on the shard mapped to the given userID.
func (e *DistributedEngine) QueryOnShard(ctx context.Context, shardKey string, sql string, args ...any) (pgx.Rows, string, error) {
	pool, shardName, err := e.manager.GetPoolForShardKey(shardKey)
	if err != nil {
		return nil, "", fmt.Errorf("routing failure: %w", err)
	}

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, shardName, fmt.Errorf("query error on shard %s: %w", shardName, err)
	}

	return rows, shardName, nil
}

// 2. SCATTER-GATHER EXECUTION (Fan-out to ALL shards concurrently)

type ShardResult[T any] struct {
	ShardName string
	Data      T
	Error     error
}

// ScatterGather executes a query worker function concurrently across ALL shards.
// It uses golang.org/x/sync/errgroup for concurrent safety and context cancellation.
func ScatterGather[T any](ctx context.Context, engine *DistributedEngine, worker func(ctx context.Context, shardName string, pool *pgxpool.Pool) (T, error)) ([]ShardResult[T], error) {
	pools := engine.manager.GetAllPools()
	results := make([]ShardResult[T], len(pools))

	g, ctx := errgroup.WithContext(ctx)
	idx := 0
	for name, pool := range pools {
		shardName := name
		shardPool := pool
		i := idx

		g.Go(func() error {
			data, err := worker(ctx, shardName, shardPool)
			results[i] = ShardResult[T]{
				ShardName: shardName,
				Data:      data,
				Error:     err,
			}
			return err
		})
		idx++
	}
	if err := g.Wait(); err != nil {
		return results, fmt.Errorf("scatter-gather execution failed: %w", err)
	}
	return results, nil
}
