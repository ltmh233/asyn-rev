package queue

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	redisClient *redis.Client
	stream      string
	group       string
}

func (queue *Queue) Produce(ctx context.Context, taskID int64) error {
	err := queue.redisClient.XAdd(ctx, &redis.XAddArgs{Stream: queue.stream, ID: "*", Values: map[string]interface{}{"taskID": taskID}}).Err()
	if err != nil {
		return err
	}
	return nil
}

func (queue *Queue) Consume(ctx context.Context, consumer string) (taskID int64, messageID string, err error) {
	result, err := queue.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{Group: queue.group, Consumer: consumer, Streams: []string{queue.stream, ">"}, Count: 1, Block: 0}).Result()
	if err != nil {
		return 0, "", err
	}
	if len(result) == 0 || len(result[0].Messages) == 0 {
		return 0, "", redis.Nil
	}
	message := result[0].Messages[0]
	taskID, ok := message.Values["taskID"].(int64)
	if !ok {
		return 0, "", fmt.Errorf("Invalid TaskID")
	}
	return taskID, message.ID, nil
}

func (queue *Queue) Ack(ctx context.Context, messageID string) error {
	err := queue.redisClient.XAck(ctx, queue.stream, queue.group, messageID).Err()
	if err != nil {
		return err
	}
	return nil
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

func New(ctx context.Context, redisClient *redis.Client) (*Queue, error) {
	stream := "asyn-rev:stream:tasks"
	group := "asyn-rev:consume_group:workers"

	queue := Queue{
		redisClient: redisClient,
		stream:      stream,
		group:       group,
	}

	err := queue.initConsumeGroup(ctx)
	if err != nil {
		return nil, err
	}
	return &queue, nil
}
