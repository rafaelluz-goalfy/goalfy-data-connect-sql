package locks

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Manager manages distributed locks via Redis
type Manager struct {
	client  *redis.Client
	ttl     time.Duration
	retries int
}

// New creates a new Manager
func New(client *redis.Client, ttl time.Duration, retries int) *Manager {
	return &Manager{client: client, ttl: ttl, retries: retries}
}

// Acquire tries to acquire an exclusive lock for the given key.
// Returns a release function and nil on success, or an error if the lock is held.
func (m *Manager) Acquire(ctx context.Context, tenantID, sourceID, datasetID string) (func(), error) {
	key := lockKey(tenantID, sourceID, datasetID)

	var acquired bool
	var err error

	for i := 0; i <= m.retries; i++ {
		acquired, err = m.client.SetNX(ctx, key, "locked", m.ttl).Result()
		if err != nil {
			return nil, fmt.Errorf("redis setnx error: %w", err)
		}
		if acquired {
			release := func() {
				m.client.Del(context.Background(), key)
			}
			return release, nil
		}
		if i < m.retries {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return nil, fmt.Errorf("execution already running for tenant=%s source=%s dataset=%s", tenantID, sourceID, datasetID)
}

// Refresh extends the lock TTL (call periodically for long-running jobs)
func (m *Manager) Refresh(ctx context.Context, tenantID, sourceID, datasetID string) error {
	key := lockKey(tenantID, sourceID, datasetID)
	ok, err := m.client.Expire(ctx, key, m.ttl).Result()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("lock key not found, may have expired")
	}
	return nil
}

func lockKey(tenantID, sourceID, datasetID string) string {
	return fmt.Sprintf("sync:lock:%s:%s:%s", tenantID, sourceID, datasetID)
}
