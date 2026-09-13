package taskstore

import (
	"asyn-rev/internal/task"
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type TaskStore struct {
	redisClient *redis.Client
	storeKey    string
	idKey       string
}

func (ts *TaskStore) Save(ctx context.Context, task *task.Task) error {
	err := ts.redisClient.HSet(ctx, ts.key(task.ID), map[string]interface{}{
		"ID":      fmt.Sprint(task.ID),
		"Type":    fmt.Sprint(task.Type),
		"Payload": fmt.Sprint(task.Payload),
		"Status":  fmt.Sprint(task.Status),
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

func (ts *TaskStore) Get(ctx context.Context, taskID int64) (*task.Task, error) {
	result, err := ts.redisClient.HGetAll(ctx, ts.key(taskID)).Result()
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("Task %d Not Found", taskID)
	}
	ID, err := strconv.ParseInt(result["ID"], 10, 64)
	if err != nil {
		return nil, err
	}
	Type := task.Type(result["Type"])
	Payload := []byte(result["Payload"])
	Status := task.Status(result["Status"])
	return &task.Task{ID: ID, Type: Type, Payload: Payload, Status: Status}, nil
}

func (ts *TaskStore) NextID(ctx context.Context) (int64, error) {
	result, err := ts.redisClient.Incr(ctx, ts.idKey).Result()
	if err != nil {
		return 0, err
	}
	return result, nil
}

func (ts *TaskStore) Update(ctx context.Context, task *task.Task) error {
	return ts.Save(ctx, task)
}

func (ts *TaskStore) DeleteTask(ctx context.Context, taskID int64) error {
	err := ts.redisClient.Del(ctx, ts.key(taskID)).Err()
	return err
}

func (ts *TaskStore) key(taskID int64) string {
	return ts.storeKey + fmt.Sprintf(":task:%d", taskID)
}

func New(ctx context.Context, redisClient *redis.Client) *TaskStore {
	storeKey := "asyn-rev:task_store"
	idKey := "asyn-rev:task_id"
	return &TaskStore{redisClient: redisClient, storeKey: storeKey, idKey: idKey}
}
