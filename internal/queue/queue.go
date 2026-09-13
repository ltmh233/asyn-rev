package queue

import (
	"asyn-rev/internal/worker"
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	redisClient *redis.Client
	stream      string
	group       string
}

func (q *Queue) Produce(ctx context.Context, taskID int64) error {
	err := q.redisClient.XAdd(ctx, &redis.XAddArgs{Stream: q.stream, ID: "*", Values: map[string]interface{}{"task_id": taskID}}).Err()
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue) Consume(ctx context.Context, consumer string) (int64, string, bool, error) {
	result, err := q.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumer,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    2 * time.Second,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, "", false, err
		}
	}
	if len(result) == 0 || len(result[0].Messages) == 0 {
		return 0, "", false, nil
	}
	message := result[0].Messages[0]
	value, ok := message.Values["task_id"].(string)
	if !ok {
		return 0, "", false, fmt.Errorf("invalid taskID")
	}
	taskID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, "", false, fmt.Errorf("invalid taskID")
	}
	return taskID, message.ID, true, nil
}

func (q *Queue) Ack(ctx context.Context, messageID string) error {
	err := q.redisClient.XAck(ctx, q.stream, q.group, messageID).Err()
	if err != nil {
		return err
	}
	return nil
}

func (q *Queue) AutoClaim(ctx context.Context, consumer string, claimStart string) (int64, string, bool, error) {
	result, _, err := q.redisClient.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		MinIdle:  10 * time.Second,
		Start:    "0-0",
		Count:    1,
		Consumer: consumer,
	}).Result()
	if err != nil {
		if err != redis.Nil {
			return 0, "", false, err
		}
	}
	if len(result) == 0 {
		return 0, "", false, nil
	}
	message := result[0]
	value, ok := message.Values["task_id"].(string)
	if !ok {
		return 0, "", false, fmt.Errorf("invalid taskID")
	}
	taskID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, "", false, err
	}
	return taskID, message.ID, true, nil
}

func (q *Queue) Pending(ctx context.Context, claimStart string) ([]worker.PendingMessage, error) {
	result, err := q.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.stream,
		Group:  q.group,
		Idle:   0,
		Start:  claimStart,
		End:    "+",
		Count:  100,
	}).Result()
	if err != nil {
		return nil, err
	}
	pending := []worker.PendingMessage{}
	for _, p := range result {
		pending = append(pending, worker.PendingMessage{
			MessageID:     p.ID,
			Consumer:      p.Consumer,
			Idle:          p.Idle,
			DeliveryCount: p.RetryCount,
		})
	}
	return pending, nil
}

func (q *Queue) Claim(ctx context.Context, worker string, messageID string) (int64, bool, error) {
	result, err := q.redisClient.XClaim(ctx, &redis.XClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		Consumer: worker,
		MinIdle:  1 * time.Second,
		Messages: []string{messageID},
	}).Result()
	if err != nil {
		return 0, false, err
	}
	if len(result) == 0 {
		return 0, false, nil
	}
	message := result[0]
	value := message.Values["task_id"].(string)
	taskID, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return taskID, true, nil
}

func (q *Queue) initConsumeGroup(ctx context.Context) error {
	err := q.redisClient.XGroupCreateMkStream(ctx, q.stream, q.group, "0-0").Err()
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
