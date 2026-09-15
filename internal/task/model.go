package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Status string

const (
	TaskStatusPending          Status = "PENDING"
	TaskStatusRunning          Status = "RUNNING"
	TaskStatusFinished         Status = "FINISHED"
	TaskStatusFailed           Status = "FAILED"
	TaskStatusWaitingRetry     Status = "WAITINGRETRY"
	TaskStatusRetryCountExceed Status = "RETRYCOUNTEXCEED"
	TaskStatusUnknownType      Status = "UNKNWONTYPE"
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
	Seconds  int `json:"seconds"`
	IfFailed int `json:"if_failed"`
}

type TaskHandler func(ctx context.Context, payload json.RawMessage, taskID int64) error

func tsk TaskHandler{
	
}

func HandlerTaskTypeSleep(ctx context.Context, payload json.RawMessage) error {
	tsk := TaskHandler{

	}
	p := SleepPayload{}
	err := json.Unmarshal(payload, &p)
	if err != nil {
		return fmt.Errorf("error occur while hanlder task sleep json.Unmarshal:%w", err)
	}
	if p.IfFailed != 0 {
		return fmt.Errorf("err")
	} else {
		time.Sleep(time.Duration(p.Seconds) * time.Second)
		return nil
	}
}

func HandlerTaskTypeEmail(ctx context.Context, payload json.RawMessage) error {
	return nil
}

func HandlerTaskTypeImage(ctx context.Context, payload json.RawMessage) error {
	return nil
}
