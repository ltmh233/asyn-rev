package worker

import (
	"asyn-rev/internal/task"
	"context"
	"errors"
	"fmt"
	"log"
)

type queue interface {
	Produce(ctx context.Context, taskID int64) error
	Consume(ctx context.Context, consumer string) (int64, string, bool, error)
	AutoClaim(ctx context.Context, consumer string) (int64, string, bool, error)
	Ack(ctx context.Context, messageID string) error
}

type status string

const (
	workerStatusBusy status = "Busy"
	workerStatusFree status = "Free"
)

type Worker struct {
	id        int64
	taskID    int64
	messageID string
	status    status
	executor  *executor
	queue     queue
	taskStore taskStore
}

func (w *Worker) String() string {
	return fmt.Sprintf("worker:%d", w.id)
}

func New(id int64, q queue, ts taskStore) *Worker {
	return &Worker{id: id, status: workerStatusFree, executor: newExecutor(ts), queue: q, taskStore: ts}
}

func (w *Worker) Run(ctx context.Context) error {
	for {

		if w.taskID == 0 && w.messageID == "" && w.status == workerStatusFree {
			taskID, messageID, claimed, err := w.queue.AutoClaim(ctx, w.String())
			if err != nil {
				return err
			}
			if claimed {
				w.set(taskID, messageID)

			} else {

				taskID, messageID, consumed, err := w.queue.Consume(ctx, w.String())
				if err != nil {
					return err
				}
				if !consumed {
					continue
				}
				w.set(taskID, messageID)
			}
		}

		err := w.executor.execute(ctx, w.taskID)
		if err != nil {
			log.Print(err)
			if !errors.Is(err, ErrTaskExecution) {
				return err
			}
			t, err := w.taskStore.Get(ctx, w.taskID)
			if err != nil {
				return err
			}

			if t.RetryCount < t.MaxRetry {
				t.RetryCount += 1
				t.Status = task.TaskStatusPending
				err = w.taskStore.Update(ctx, t)
				if err != nil {
					return err
				}

				err = w.queue.Produce(ctx, t.ID)
				if err != nil {
					return err
				}
			}

			err = w.queue.Ack(ctx, w.messageID)
			if err != nil {
				return err
			}

			w.reset()

			continue
		}

		err = w.queue.Ack(ctx, w.messageID)
		if err != nil {
			return err
		}

		w.reset()
	}
}

func (w *Worker) reset() {
	w.taskID = 0
	w.messageID = ""
	w.status = workerStatusFree
}

func (w *Worker) set(taskID int64, messageID string) {
	w.taskID = taskID
	w.messageID = messageID
	w.status = workerStatusBusy
}
