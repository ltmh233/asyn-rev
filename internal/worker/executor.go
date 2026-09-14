package worker

import (
	"asyn-rev/internal/task"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrTaskExecution = errors.New("task execution failed")

type taskHandler func(ctx context.Context, payload json.RawMessage) error

type executor struct {
	handlers  map[task.Type]taskHandler
	taskStore taskStore
}

func (e *executor) register(taskType task.Type, handler taskHandler) {
	e.handlers[taskType] = handler
}

func (e *executor) execute(ctx context.Context, taskID int64) error {
	t, err := e.taskStore.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("error occur while executor execute e.taskStore.Get: %w", err)
	}
	handler, ok := e.handlers[t.Type]
	if !ok {
		t.Status = task.TaskStatusFailed
		err = e.taskStore.Update(ctx, t)
		if err != nil {
			return fmt.Errorf("error occur while executor execute e.taskStore.Update: %w", err)
		}
		return fmt.Errorf("error occur while executor execute e.handler[]: %w : unknwon type of task: %v", ErrTaskExecution, t.Type)
	}
	t.Status = task.TaskStatusRunning
	err = e.taskStore.Update(ctx, t)
	if err != nil {
		return fmt.Errorf("error occur while executor execute e.taskStore.Update: %w", err)

	}
	err = handler(ctx, t.Payload)
	if err != nil {
		t.Status = task.TaskStatusFailed
		errUpdate := e.taskStore.Update(ctx, t)
		if errUpdate != nil {
			return fmt.Errorf("error occur while executor execute e.taskStore.Update: %w", errUpdate)

		}
		return fmt.Errorf("error occur while executor execute handler: %w : unknwon type of task: %v", ErrTaskExecution, t.Type)
	}
	t.Status = task.TaskStatusFinished
	err = e.taskStore.Update(ctx, t)
	return fmt.Errorf("error occur while executor execute e.taskStore.Update: %w", err)

}

func newExecutor(ts taskStore) *executor {
	executor := executor{handlers: map[task.Type]taskHandler{}, taskStore: ts}
	executor.register(task.TaskTypeEmail, task.HandlerTaskTypeEmail)
	executor.register(task.TaskTypeImage, task.HandlerTaskTypeImage)
	executor.register(task.TaskTypeSleep, task.HandlerTaskTypeSleep)
	return &executor
}
