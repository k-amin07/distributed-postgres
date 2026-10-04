package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"distributed-postgres/internal/sharding"
)

func TestDistributedEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Initialize Ring and Connection Manager
	ring := sharding.NewHashRing(150)
	manager := sharding.NewShardManager(ring)
	defer manager.Close()

	shardConfigs := []sharding.ShardConfig{
		{Name: "pg-shard-0", DSN: "postgres://admin:admin_123@localhost:5431/expenses?sslmode=disable"},
		{Name: "pg-shard-1", DSN: "postgres://admin:admin_123@localhost:5434/expenses?sslmode=disable"},
		{Name: "pg-shard-2", DSN: "postgres://admin:admin_123@localhost:5433/expenses?sslmode=disable"},
	}

	for _, cfg := range shardConfigs {
		if err := manager.RegisterAndConnect(ctx, cfg.Name, cfg.DSN); err != nil {
			t.Fatalf("Failed registering shard %s: %v", cfg.Name, err)
		}
	}

	engine := NewDistributedEngine(manager)

	// -------------------------------------------------------------------------
	// TEST 1: Routed Targeted Write & Read
	// -------------------------------------------------------------------------
	userID, _ := uuid.NewV7()
	username := "distributed_user_" + userID.String()[:8]

	insertSQL := `INSERT INTO users (id, username, password) VALUES ($1, $2, $3)`
	rowsAffected, shardUsed, err := engine.ExecOnShard(ctx, userID.String(), insertSQL, userID, username, "secret_hash")
	if err != nil {
		t.Fatalf("Failed ExecOnShard: %v", err)
	}

	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}
	t.Logf("Successfully inserted user %s into targeted shard -> %s", username, shardUsed)

	// Query row back from the target shard
	selectSQL := `SELECT username FROM users WHERE id = $1`
	row, _, err := engine.QueryRowOnShard(ctx, userID.String(), selectSQL, userID)
	if err != nil {
		t.Fatalf("Failed QueryRowOnShard: %v", err)
	}

	var fetchedUsername string
	if err := row.Scan(&fetchedUsername); err != nil {
		t.Fatalf("Failed scanning row: %v", err)
	}

	if fetchedUsername != username {
		t.Errorf("Expected username %s, got %s", username, fetchedUsername)
	}

	// -------------------------------------------------------------------------
	// TEST 2: Scatter-Gather Global Count Across All Shards
	// -------------------------------------------------------------------------
	countWorker := func(ctx context.Context, shardName string, pool *pgxpool.Pool) (int64, error) {
		var count int64
		err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
		return count, err
	}

	results, err := ScatterGather[int64](ctx, engine, countWorker)
	if err != nil {
		t.Fatalf("ScatterGather failed: %v", err)
	}

	var totalUsers int64
	for _, res := range results {
		t.Logf("Shard %s count = %d", res.ShardName, res.Data)
		totalUsers += res.Data
	}

	if totalUsers < 1 {
		t.Errorf("Expected at least 1 total user across all shards, got %d", totalUsers)
	}
	t.Logf("Scatter-Gather Total Users across all shards = %d", totalUsers)
}
