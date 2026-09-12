package worker

import (
	"asyn-rev/internal/task"
	"context"
	"fmt"
)

type queue interface {
	Consume(ctx context.Context, consumer string) (int64, string, error)
	Ack(ctx context.Context, messageID string) error
}

type status string

const (
	workerStatusBusy status = "Busy"
	workerStatusFree status = "Free"
)

type Worker struct {
	id       int64
	taskID   int64
	status   status
	executor *executor
	queue    queue
}

func (w *Worker) String() string {
	return fmt.Sprintf("worker:%d", w.id)
}

func New(id int64, queue queue, taskStore taskStore) *Worker {
	return &Worker{id: id, status: workerStatusFree, executor: newExecutor(taskStore), queue: queue}
}

func (w *Worker) Run(ctx context.Context, taskType task.Type, payload []byte) error {
	for {
		taskID, messageID, err := w.queue.Consume(ctx, w.String())
		if err != nil {
			return err
		}
		w.status = workerStatusBusy
		w.taskID = taskID

		err = w.executor.execute(ctx, taskID)
		if err != nil {

			return err
		}

		err = w.queue.Ack(ctx, messageID)
		if err != nil {
			return err
		}

		w.taskID = 0
		w.status = workerStatusFree
	}
}
