package worker

import (
	"asyn-rev/internal/task"
	"context"
	"fmt"
)

type taskStore interface {
	Get(ctx context.Context, taskID int64) (*task.Task, error)
	Update(ctx context.Context, task *task.Task) error
}

type taskHandler func(ctx context.Context, payload []byte) error

type executor struct {
	handlers  map[task.Type]taskHandler
	taskStore taskStore
}

func (e *executor) register(taskType task.Type, handler taskHandler) {
	e.handlers[taskType] = handler
}

func (e *executor) execute(ctx context.Context, taskID int64) error {
	task, err := e.taskStore.Get(ctx, taskID)
	if err != nil {
		return err
	}
	handler, ok := e.handlers[task.Type]
	if !ok {
		return fmt.Errorf("unknown type of task: %v", task.Type)
	}
	err = handler(ctx, task.Payload)
	return err
}

func newExecutor(taskStore taskStore) *executor {
	executor := executor{handlers: map[task.Type]taskHandler{}, taskStore: taskStore}
	executor.register(task.TaskTypeEmail, task.HandlerTaskTypeEmail)
	executor.register(task.TaskTypeImage, task.HandlerTaskTypeImage)
	executor.register(task.TaskTypeSleep, task.HandlerTaskTypeSleep)
	return &executor
}
