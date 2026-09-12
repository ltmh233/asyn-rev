package queue

import (
	"asyn-rev/internal/config"
	"context"

	"github.com/redis/go-redis/v9"
)

func newRedisClient(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	var client *redis.Client
	client = redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + cfg.Port,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return client, err
	}
	return client, nil
}
