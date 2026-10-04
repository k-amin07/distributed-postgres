package sharding

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
)

type HashRing struct {
	mu      sync.RWMutex
	vnodes  int               // number of virtual nodes per physical shard
	ring    []uint32          // sorted array of hashed vnode positions
	nodeMap map[uint32]string // maps hashed vnode position to physical shard name
}

var ErrNoShardsAvailable = errors.New("no shards configured in hash ring")

func NewHashRing(vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = 200
	}

	return &HashRing{
		vnodes:  vnodes,
		nodeMap: make(map[uint32]string),
	}
}

// hash uses MD5 to generate an evenly distributed 32-bit unsigned integer.
func (h *HashRing) hash(key string) uint32 {
	hasher := md5.New()
	hasher.Write([]byte(key))
	digest := hasher.Sum(nil)
	// Take first 4 bytes of MD5 digest and convert to uint32
	return binary.BigEndian.Uint32(digest[:4])
}

func (h *HashRing) AddShard(shard string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for i := 0; i < h.vnodes; i++ {
		vnodeKey := fmt.Sprintf("%s-vnode-%04d", shard, i)
		vnodeHash := h.hash(vnodeKey)

		h.ring = append(h.ring, vnodeHash)
		h.nodeMap[vnodeHash] = shard
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

func (h *HashRing) GetShard(userId uuid.UUID) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.ring) == 0 {
		return "", ErrNoShardsAvailable
	}

	userHash := h.hash(userId.String())

	idx := (sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= userHash
	})) % len(h.ring)

	vnodeHash := h.ring[idx]

	return h.nodeMap[vnodeHash], nil
}
