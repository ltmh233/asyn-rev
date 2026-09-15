package worker

import (
	"asyn-rev/internal/task"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrTaskExecution = errors.New("task execution failed")
var ErrUnknwonTaskType = errors.New("unknown task type")

type taskHandler func(ctx context.Context, payload json.RawMessage) error

type executor struct {
	handlers   map[task.Type]taskHandler
	taskStore  taskStore
	transition transition
}

func (e *executor) register(taskType task.Type, handler taskHandler) {
	e.handlers[taskType] = handler
}

func (e *executor) execute(ctx context.Context, taskID int64, messageID string) error {
	t, err := e.taskStore.Get(ctx, taskID)
	if err != nil {
		return fmt.Errorf("error occur while executor execute e.taskStore.Get: %w", err)
	}
	handler, ok := e.handlers[t.Type]
	if !ok {
		t.Status = task.TaskStatusUnknownType
		err = e.taskStore.Update(ctx, t)
		if err != nil {
			return fmt.Errorf("error occur while executor execute e.taskStore.Update: %w", err)
		}
		return fmt.Errorf("error occur while executor execute e.handler[]: %w :%v", ErrUnknwonTaskType, t.Type)
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
		return fmt.Errorf("error occur while executor execute handler: %w; %w", ErrTaskExecution, err)
	}
	err = e.transition.TaskFinish(ctx, taskID, messageID)
	if err != nil {
		return fmt.Errorf("error occur while executor execute e.transition.TaskFinish: %w", err)
	}
	return nil

}

func newExecutor(ts taskStore, t transition) *executor {
	executor := executor{
		handlers:   map[task.Type]taskHandler{},
		taskStore:  ts,
		transition: t,
	}
	executor.register(task.TaskTypeEmail, task.HandlerTaskTypeEmail)
	executor.register(task.TaskTypeImage, task.HandlerTaskTypeImage)
	executor.register(task.TaskTypeSleep, task.HandlerTaskTypeSleep)
	return &executor
}
