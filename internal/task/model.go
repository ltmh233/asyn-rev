package task

import (
	"context"
	"encoding/json"
)

type CreateTaskRequest struct {
	Type    Type            `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type Status string

const (
	TaskStatusPending  Status = "PENDING"
	TaskStatusRunning  Status = "RUNNING"
	TaskStatusFinished Status = "FINISHED"
	TaskStatusFailed   Status = "FAILED"
)

type Type string

const (
	TaskTypeSleep Type = "sleep"
	TaskTypeEmail Type = "email"
	TaskTypeImage Type = "image"
)

type Task struct {
	ID      int64
	Type    Type
	Payload []byte
	Status  Status
}

func HandlerTaskTypeSleep(ctx context.Context, payload []byte) error {
	return nil
}

func HandlerTaskTypeEmail(ctx context.Context, payload []byte) error {
	return nil
}

func HandlerTaskTypeImage(ctx context.Context, payload []byte) error {
	return nil
}
