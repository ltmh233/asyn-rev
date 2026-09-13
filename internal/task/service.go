package task

import (
	"context"
	"fmt"
)

type queue interface {
	Produce(ctx context.Context, taskID int64) error
}

type taskStore interface {
	Get(ctx context.Context, taskID int64) (*Task, error)
	Save(ctx context.Context, task *Task) error
	Update(ctx context.Context, task *Task) error
	NextID(ctx context.Context) (int64, error)
	DeleteTask(ctx context.Context, taskID int64) error
}

type Service struct {
	queue     queue
	taskStore taskStore
}

func (s *Service) CreateTask(ctx context.Context, taskType Type, payload []byte) (int64, error) {
	taskID, err := s.taskStore.NextID(ctx)
	if err != nil {
		return 0, err
	}
	err = s.taskStore.Save(ctx, &Task{
		ID:         taskID,
		Type:       taskType,
		Payload:    payload,
		Status:     TaskStatusPending,
		RetryCount: 0,
		MaxRetry:   3,
	})
	if err != nil {
		return 0, err
	}
	err = s.queue.Produce(ctx, taskID)
	if err != nil {
		errDelete := s.taskStore.DeleteTask(ctx, taskID)
		if errDelete != nil {
			return 0, fmt.Errorf("%w;%w", errDelete, err)
		}
		return 0, err
	}
	return taskID, nil
}

func (s *Service) GetTask(ctx context.Context, taskID int64) (*Task, error) {
	return s.taskStore.Get(ctx, taskID)
}

func NewService(q queue, ts taskStore) *Service {
	return &Service{queue: q, taskStore: ts}
}
