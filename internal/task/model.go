package task

import (
	"context"
	"encoding/json"
	"time"
)

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
	ID         int64           `json:"id"`
	Type       Type            `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Status     Status          `json:"status"`
	RetryCount int64           `json:"retry_count"`
	MaxRetry   int64           `json:"max_retry"`
}

type SleepPayload struct {
	Seconds int `json:"seconds"`
}

func HandlerTaskTypeSleep(ctx context.Context, payload json.RawMessage) error {
	p := SleepPayload{}
	err := json.Unmarshal(payload, &p)
	if err != nil {
		return err
	}
	time.Sleep(time.Duration(p.Seconds) * time.Second)
	return nil
}

func HandlerTaskTypeEmail(ctx context.Context, payload json.RawMessage) error {
	return nil
}

func HandlerTaskTypeImage(ctx context.Context, payload json.RawMessage) error {
	return nil
}

func main() {
	HandlerTaskTypeSleep(context.Background(), Task{}.Payload)
}
