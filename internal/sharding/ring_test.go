package sharding

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestHashRingDistribution(t *testing.T) {
	ring := NewHashRing(200)

	shards := []string{"pg-shard-0", "pg-shard-1", "pg-shard-2"}

	for _, s := range shards {
		ring.AddShard(s)
	}

	counts := make(map[string]int)
	totalKeys := 10000

	for i := 0; i < totalKeys; i++ {
		userId, _ := uuid.NewV7()
		targetShard, err := ring.GetShard(userId)
		if err != nil {
			t.Fatalf("unexpected errror: %v", err)
		}
		counts[targetShard]++
	}

	fmt.Printf("Distribution across 10,000 UUIDv7 keys (150 VNodes + MD5):\n")
	for shard, count := range counts {
		pct := float64(count) / float64(totalKeys) * 100
		fmt.Printf(" - %s: %d keys (%.2f%%)\n", shard, count, pct)

		// Assert that no single shard gets overloaded beyond 40%
		if pct > 40.0 || pct < 25.0 {
			t.Errorf("Shard %s received skewed load: %.2f%%", shard, pct)
		}
	}
}
