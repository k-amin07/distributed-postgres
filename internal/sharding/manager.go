package sharding

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ShardConfig struct {
	Name string
	DSN  string
}

type ShardManager struct {
	mu     sync.RWMutex
	ring   *HashRing
	pools  map[string]*pgxpool.Pool
	shards map[string]string // Name -> DSN mapping
}

func NewShardManager(ring *HashRing) *ShardManager {
	return &ShardManager{
		ring:   ring,
		pools:  make(map[string]*pgxpool.Pool),
		shards: make(map[string]string),
	}
}

func (m *ShardManager) RegisterAndConnect(ctx context.Context, shardName string, dsn string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	config, err := pgxpool.ParseConfig(dsn)

	if err != nil {
		return fmt.Errorf("failed to parse DSN for shard %s: %w", shardName, err)
	}

	// Performance tuning parameters for sharded pools
	config.MaxConns = 25
	config.MinConns = 2
	config.MaxConnIdleTime = 15 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, config)

	if err != nil {
		return fmt.Errorf("failed to create pool for shard %s: %w", shardName, err)
	}

	// Verify connectivity via Ping
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return fmt.Errorf("ping failed for shard %s: %w", shardName, err)
	}

	m.pools[shardName] = pool
	m.shards[shardName] = dsn
	m.ring.AddShard(shardName)

	return nil
}

func (m *ShardManager) GetPool(shardName string) (*pgxpool.Pool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pool, exists := m.pools[shardName]
	if !exists {
		return nil, fmt.Errorf("shard pool %q not registered", shardName)
	}
	return pool, nil
}

func (m *ShardManager) GetPoolForShardKey(shardKey string) (*pgxpool.Pool, string, error) {
	shardName, err := m.ring.GetShard(shardKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to route user %s: %w", shardKey, err)
	}

	pool, err := m.GetPool(shardName)
	if err != nil {
		return nil, shardName, err
	}

	return pool, shardName, nil
}

func (m *ShardManager) GetAllPools() map[string]*pgxpool.Pool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copyMap := make(map[string]*pgxpool.Pool, len(m.pools))
	for name, pool := range m.pools {
		copyMap[name] = pool
	}
	return copyMap
}

func (m *ShardManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, pool := range m.pools {
		pool.Close()
		delete(m.pools, name)
	}
}
