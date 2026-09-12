package queue

import (
	"asyn-rev/internal/config"
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	redisClient *redis.Client
	stream      string
	group       string
}

func (queue *Queue) Produce(ctx context.Context, value string) error {
	err := queue.redisClient.XAdd(ctx, &redis.XAddArgs{Stream: queue.stream, ID: "*", Values: map[string]interface{}{"taskID": value}}).Err()
	if err != nil {
		return err
	}
	return nil
}

func (queue *Queue) Consume(ctx context.Context, consumer string) (taskID string, messageID string, err error) {
	result, err := queue.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{Group: queue.group, Consumer: consumer, Streams: []string{queue.stream}}).Result()
	if err != nil {
		return "", "", err
	}
	message := result[0].Messages[0]
	return message.Values["taskID"].(string), message.ID, nil
}

func (queue *Queue) Ack() {

}

func (queue *Queue) initConsumeGroup(ctx context.Context) error {
	err := queue.redisClient.XGroupCreateMkStream(ctx, queue.stream, queue.group, "0-0").Err()
	if err != nil {
		// 在重复创建GROUP时, 会报错(error) BUSYGROUP Consumer Group name already exists
		// 无视错误
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			return err
		}
	}
	return nil
}

func New(ctx context.Context, cfg config.RedisConfig) (*Queue, error) {
	redisClient, err := newRedisClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	stream := "asyn-rev:stream:tasks"
	group := "asyn-rev:consume_group:workers"

	queue := Queue{
		redisClient: redisClient,
		stream:      stream,
		group:       group,
	}

	err = queue.initConsumeGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &queue, nil
}
