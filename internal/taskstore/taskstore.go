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
}

func (ts *TaskStore) Save(ctx context.Context, task *task.Task) error {
	err := ts.redisClient.HSet(ctx, ts.key(task.ID), map[string]interface{}{
		"ID":      task.ID,
		"Type":    task.Type,
		"Payload": task.Payload,
		"Status":  task.Status,
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

func (ts *TaskStore) Update(ctx context.Context, task *task.Task) error {
	return ts.Save(ctx, task)
}

func (ts *TaskStore) key(taskID int64) string {
	return ts.storeKey + fmt.Sprintf(":task:%d", taskID)
}

func New(ctx context.Context, redisClient *redis.Client) *TaskStore {
	storeKey := "asyn-rev:task_store"
	return &TaskStore{redisClient: redisClient, storeKey: storeKey}
}
