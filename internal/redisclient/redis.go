package redisclient

import (
	"asyn-rev/internal/config"
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func Init(ctx context.Context, cfg config.RedisConfig) (*redis.Client, error) {
	var client *redis.Client
	client = redis.NewClient(&redis.Options{
		Addr:     cfg.Host + ":" + cfg.Port,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return client, fmt.Errorf("error occur where redis init client.Ping(ctx): %w", err)
	}
	return client, nil
}
