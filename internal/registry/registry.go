package registry

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Registry struct {
	redisClient *redis.Client
	aliveKey    string
	ttl         time.Duration
}

func (r *Registry) Refresh(ctx context.Context, worker string) error {
	err := r.redisClient.Set(ctx, r.aliveKey+worker, 1, r.ttl).Err()
	if err != nil {
		return err
	}
	return nil
}

func (r *Registry) Alive(ctx context.Context, worker string) (bool, error) {
	result, err := r.redisClient.Exists(ctx, r.aliveKey+worker).Result()
	if err != nil {
		return false, err
	}

	return result == 1, nil
}

func New(r *redis.Client) *Registry {
	aliveKey := "asyn-rev:alive:"
	ttl := 10 * time.Second
	return &Registry{redisClient: r, aliveKey: aliveKey, ttl: ttl}
}
