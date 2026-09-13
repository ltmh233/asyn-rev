package worker

import (
	"asyn-rev/internal/task"
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type PendingMessage struct {
	MessageID     string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int64
}

type queue interface {
	Produce(ctx context.Context, taskID int64) error
	Consume(ctx context.Context, consumer string) (int64, string, bool, error)
	AutoClaim(ctx context.Context, consumer string, claimStart string) (int64, string, bool, error)
	Pending(ctx context.Context, claimStart string) ([]PendingMessage, error)
	Claim(ctx context.Context, consumer string, messageID string) (int64, bool, error)
	Ack(ctx context.Context, messageID string) error
}

type taskStore interface {
	Get(ctx context.Context, taskID int64) (*task.Task, error)
	Update(ctx context.Context, task *task.Task) error
}

type registry interface {
	Refresh(ctx context.Context, worker string) error
	Alive(ctx context.Context, worker string) (bool, error)
}

type status string

const (
	workerStatusBusy status = "Busy"
	workerStatusFree status = "Free"
)

type Worker struct {
	id         int64
	taskID     int64
	messageID  string
	status     status
	executor   *executor
	queue      queue
	taskStore  taskStore
	registry   registry
	claimStart string
}

func (w *Worker) String() string {
	return fmt.Sprintf("worker:%d", w.id)
}

func New(id int64, q queue, ts taskStore, r registry) *Worker {
	return &Worker{id: id, status: workerStatusFree, executor: newExecutor(ts), queue: q, taskStore: ts, registry: r, claimStart: "0-0"}
}

func (w *Worker) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	err := w.registry.Refresh(ctx, w.String())
	if err != nil {
		log.Print(err)
	}

	for {
		select {
		case <-ticker.C:
			err = w.registry.Refresh(ctx, w.String())
			if err != nil {
				log.Print(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	go w.heartbeat(ctx)

	for {

		for {
			pending, err := w.queue.Pending(ctx, w.claimStart)
			if err != nil {
				return err
			}
			if len(pending) == 0 {
				w.claimStart = "-"
				break
			}
			for _, p := range pending {
				w.claimStart = "(" + p.MessageID
				alive, err := w.registry.Alive(ctx, p.Consumer)
				if err != nil {
					log.Print(err)
					continue
				}
				if !alive {
					taskID, claimed, err := w.queue.Claim(ctx, w.String(), p.MessageID)
					if err != nil {
						return err
					}
					if !claimed {
						continue
					}
					w.set(taskID, p.MessageID, "-")
					break
				}
			}
			if w.status == workerStatusBusy {
				break
			}
		}
		if w.taskID == 0 && w.messageID == "" && w.status == workerStatusFree {
			// taskID, messageID, claimed, err := w.queue.AutoClaim(ctx, w.String(), w.claimStart)
			// if err != nil {
			// 	return err
			// }
			// if claimed {
			// 	w.set(taskID, messageID, "-")

			// } else {

			taskID, messageID, consumed, err := w.queue.Consume(ctx, w.String())
			if err != nil {
				return err
			}
			if !consumed {
				continue
			}
			w.set(taskID, messageID, "-")

			//}
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

func (w *Worker) set(taskID int64, messageID string, claimStart string) {
	w.taskID = taskID
	w.messageID = messageID
	w.claimStart = claimStart
	w.status = workerStatusBusy
}
