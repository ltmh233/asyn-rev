package taskstore

import (
	"asyn-rev/internal/task"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var ErrTaskNotFound = errors.New("error not found")

type TaskStore struct {
	redisClient *redis.Client
	storeKey    string
	idKey       string
}

func (ts *TaskStore) Save(ctx context.Context, task *task.Task) error {
	err := ts.redisClient.HSet(ctx, ts.key(task.ID), map[string]interface{}{
		"ID":         fmt.Sprint(task.ID),
		"Type":       fmt.Sprint(task.Type),
		"Payload":    fmt.Sprint(task.Payload),
		"Status":     fmt.Sprint(task.Status),
		"RetryCount": fmt.Sprint(task.RetryCount),
		"MaxRetry":   fmt.Sprint(task.MaxRetry),
	}).Err()
	if err != nil {
		return fmt.Errorf("error occur while taskStore save task ts.redisClinet.Hset: %w", err)
	}
	return nil
}

func (ts *TaskStore) Get(ctx context.Context, taskID int64) (*task.Task, error) {
	result, err := ts.redisClient.HGetAll(ctx, ts.key(taskID)).Result()
	if err != nil {
		return nil, fmt.Errorf("error occur while tastStore get task ts.redisClient.HGetAll: %w", err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("error occur while tastStore get task len(result)==0: %w", ErrTaskNotFound)
	}
	ID, err := strconv.ParseInt(result["ID"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error occur while taskStore get task strconv.ParseInt: %w", err)
	}
	Type := task.Type(result["Type"])
	Payload := []byte(result["Payload"])
	Status := task.Status(result["Status"])
	RetryCount, err := strconv.ParseInt(result["RetryCount"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error occur while taskstore get strconv.ParseInt: %w", err)
	}
	MaxRetry, err := strconv.ParseInt(result["MaxRetry"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error occur while taskstore get strconv.ParseInt: %w", err)
	}
	return &task.Task{
		ID:         ID,
		Type:       Type,
		Payload:    Payload,
		Status:     Status,
		RetryCount: RetryCount,
		MaxRetry:   MaxRetry,
	}, nil
}

func (ts *TaskStore) NextID(ctx context.Context) (int64, error) {
	result, err := ts.redisClient.Incr(ctx, ts.idKey).Result()
	if err != nil {
		return 0, fmt.Errorf("error occur while taskStore get nextid ts.redisClient.Incr: %w", err)
	}
	return result, nil
}

func (ts *TaskStore) Update(ctx context.Context, task *task.Task) error {
	return ts.Save(ctx, task)
}

func (ts *TaskStore) DeleteTask(ctx context.Context, taskID int64) error {
	err := ts.redisClient.Del(ctx, ts.key(taskID)).Err()
	if err != nil {
		return fmt.Errorf("error occur while taskstore delete task ts.redisClient.Del: %w", err)
	}
	return nil
}

func (ts *TaskStore) key(taskID int64) string {
	return ts.storeKey + fmt.Sprintf(":task:%d", taskID)
}

func New(ctx context.Context, r *redis.Client) *TaskStore {
	storeKey := "asyn-rev:task_store"
	idKey := "asyn-rev:task_id"
	return &TaskStore{redisClient: r, storeKey: storeKey, idKey: idKey}
}
