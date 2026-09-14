package sheduler

import (
	"asyn-rev/internal/task"
	"context"
	"fmt"
	"log"
	"time"
)

type retryQueue interface {
	Scan(ctx context.Context) error
}

type taskStore interface {
	Get(ctx context.Context, taskID int64) (*task.Task, error)
	Update(ctx context.Context, task *task.Task) error
}

type queue interface {
	Produce(ctx context.Context, taskID int64) error
}

type Scheduler struct {
	retryQueue retryQueue
	taskStore  taskStore
	queue      queue
}

func (s *Scheduler) schedule(ctx context.Context) error {
	err := s.retryQueue.Scan(ctx)
	if err != nil {
		return fmt.Errorf("error occur while retryschedule shecule s.retryQueue.Scan: %w", err)
	}
	return nil
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			err := s.schedule(ctx)
			if err != nil {
				log.Print(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func New(rq retryQueue, ts taskStore, q queue) *Scheduler {
	return &Scheduler{retryQueue: rq, taskStore: ts, queue: q}
}
