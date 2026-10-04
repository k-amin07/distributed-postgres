package sharding

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestShardManagerIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ring := NewHashRing(150)
	manager := NewShardManager(ring)
	defer manager.Close()

	// DSNs connecting to your local Docker Postgres shards on mapped host ports
	shardConfigs := []ShardConfig{
		{Name: "pg-shard-0", DSN: "postgres://admin:admin_123@localhost:5431/expenses?sslmode=disable"},
		{Name: "pg-shard-1", DSN: "postgres://admin:admin_123@localhost:5434/expenses?sslmode=disable"},
		{Name: "pg-shard-2", DSN: "postgres://admin:admin_123@localhost:5433/expenses?sslmode=disable"},
	}

	for _, cfg := range shardConfigs {
		err := manager.RegisterAndConnect(ctx, cfg.Name, cfg.DSN)
		if err != nil {
			t.Fatalf("Failed to connect to shard %s: %v", cfg.Name, err)
		}
	}

	// Create a test user and route it to its target shard pool
	userID, _ := uuid.NewV7()
	username := "test_user_" + userID.String()[:8]

	pool, targetShard, err := manager.GetPoolForShardKey(userID.String())
	if err != nil {
		t.Fatalf("Failed to resolve pool for user: %v", err)
	}

	t.Logf("User %s routed to shard -> %s", userID, targetShard)

	// Insert the user directly into the routed shard pool
	query := `INSERT INTO users (id, username, password) VALUES ($1, $2, $3)`
	_, err = pool.Exec(ctx, query, userID, username, "hashed_secret_123")
	if err != nil {
		t.Fatalf("Failed to insert user on shard %s: %v", targetShard, err)
	}

	// Verify that the record exists on the assigned target shard
	var dbUsername string
	err = pool.QueryRow(ctx, "SELECT username FROM users WHERE id = $1", userID).Scan(&dbUsername)
	if err != nil {
		t.Fatalf("Failed to query inserted user from target shard %s: %v", targetShard, err)
	}

	if dbUsername != username {
		t.Errorf("Expected username %s, got %s", username, dbUsername)
	}

	// Verify that the user DOES NOT exist on the other shards
	for shardName, otherPool := range manager.GetAllPools() {
		if shardName == targetShard {
			continue
		}
		var exists bool
		err := otherPool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)", userID).Scan(&exists)
		if err != nil {
			t.Fatalf("Failed checking non-target shard %s: %v", shardName, err)
		}
		if exists {
			t.Errorf("Data leak! User %s was found on %s, but should only exist on %s", userID, shardName, targetShard)
		}
	}
}
