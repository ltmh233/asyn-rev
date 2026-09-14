package retryqueue

import (
	"asyn-rev/internal/task"
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type taskStore interface {
	Save(ctx context.Context, task *task.Task) error
	Get(ctx context.Context, taskID int64) (*task.Task, error)
	Update(ctx context.Context, task *task.Task) error
}

type queue interface {
	Produce(ctx context.Context, taskID int64) error
}

type RetryQueue struct {
	redisClient *redis.Client
	retryKey    string
	taskStore   taskStore
	queue       queue
}

func (rq *RetryQueue) Push(ctx context.Context, t *task.Task) error {
	if t.RetryCount < t.MaxRetry {
		now := time.Now()
		err := rq.redisClient.ZAdd(ctx, rq.retryKey, redis.Z{
			Score:  float64(now.Unix() + (1 << t.RetryCount)),
			Member: fmt.Sprint(t.ID),
		}).Err()
		if err != nil {
			return fmt.Errorf("error occur while retryqueue push rq.redisClient.ZAdd: %w", err)
		}
		t.RetryCount += 1
		t.Status = task.TaskStatusWatingRetry
		err = rq.taskStore.Update(ctx, t)
		if err != nil {
			return fmt.Errorf("error occur while retryqueue push rq.taskStore.Update: %w", err)
		}
		return nil
	} else {
		t.Status = task.TaskStatusRetryCountExceed
		err := rq.taskStore.Update(ctx, t)
		if err != nil {
			return fmt.Errorf("error occur while retryqueue push rq.taskStore.Update: %w", err)
		}
		return nil
	}
}

func (rq *RetryQueue) Pop(ctx context.Context) ([]int64, error) {
	now := time.Now()
	result, err := rq.redisClient.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     rq.retryKey,
		Start:   0,
		Stop:    now.Unix(),
		ByScore: true,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("error occur while retryqueue pop rq.redisClient.ZRange")
	}
	taskIDs := []int64{}
	for _, r := range result {
		taskID, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error occur while retryqueue pop strconv.ParseInt: %w", err)
		}
		taskIDs = append(taskIDs, taskID)
	}
	return taskIDs, nil
}

func (rq *RetryQueue) Scan(ctx context.Context) error {
	taskIDs, err := rq.Pop(ctx)
	if err != nil {
		return (fmt.Errorf("error occur while retryqueue scan rq.Pop: %w", err))
	}
	for _, taskID := range taskIDs {
		t, err := rq.taskStore.Get(ctx, taskID)
		if err != nil {
			log.Print(fmt.Errorf("error occur while retryqueue scan rq.taskStore.Get: %w", err))
			continue
		}
		t.Status = task.TaskStatusPending
		err = rq.taskStore.Update(ctx, t)
		if err != nil {
			log.Print(fmt.Errorf("error occur while retryqueue scan rq.taskStore.Update: %w", err))
			continue
		}
		err = rq.queue.Produce(ctx, taskID)
		if err != nil {
			log.Print(fmt.Errorf("error occur while retryqueue scan rq.queue.Produce: %w", err))
			continue
		}
		err = rq.Remove(ctx, taskID)
		if err != nil {
			log.Print("error occur while retryqueue scan rq.Remove: %w", err)
		}
	}
	return nil
}

func (rq *RetryQueue) Remove(ctx context.Context, taskID int64) error {
	err := rq.redisClient.ZRem(ctx, rq.retryKey, fmt.Sprint(taskID)).Err()
	if err != nil {
		return fmt.Errorf("error occur while retryqueue remove: %w", err)
	}
	return nil
}

func New(r *redis.Client, ts taskStore, q queue) *RetryQueue {
	retryKey := "asyn-rev:retry:tasks"
	return &RetryQueue{
		redisClient: r,
		retryKey:    retryKey,
		taskStore:   ts,
		queue:       q,
	}
}
