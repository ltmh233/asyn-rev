package transition

import (
	"asyn-rev/internal/rediskey"
	"asyn-rev/internal/task"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Transition struct {
	redisClient *redis.Client
}

func New(r *redis.Client) *Transition {
	return &Transition{
		redisClient: r,
	}
}

func (t *Transition) getTaskStoreKey(taskID int64) string {
	return rediskey.TaskStoreStoreKey + fmt.Sprintf(":task:%d", taskID)
}

func (t *Transition) ScheduleRetry(ctx context.Context, taskID int64, messageID string) error {
	script := redis.NewScript(`
		local status = redis.call("HGET",KEYS[1],"Status")
		if not status then
			return -1
		end
		if status == ARGV[1] then
			redis.call("XACK",KEYS[2],
			ARGV[2],ARGV[3]
			)
			return 3
		end
		if status ~= ARGV[4] then
			return 0
		end
		local retryCount = redis.call("HGET",KEYS[1],"RetryCount")
		local maxRetry = redis.call("HGET",KEYS[1],"MaxRetry")
		if not retryCount or not maxRetry then
			return -1
		end
		if tonumber(retryCount) >= tonumber(maxRetry) then
			redis.call("XACK",KEYS[2],
			ARGV[2],ARGV[3]
			)
			return 2
		end	

		local retryAt = tonumber(ARGV[5]) + 2^tonumber(retryCount)
		redis.call("ZADD",KEYS[3],
		retryAt,ARGV[6]
		)
		redis.call("HSET",KEYS[1],
		"Status",ARGV[7]
		)
		redis.call("HINCRBY",KEYS[1],"RetryCount",1)
		redis.call("XACK",KEYS[2],
		ARGV[2],ARGV[3]
		)
		return 1
		`)
	_, err := script.Run(ctx, t.redisClient, []string{t.getTaskStoreKey(taskID), rediskey.QueueStreamKey, rediskey.RetryQueueRetryKey}, string(task.TaskStatusFinished), rediskey.QueueGroupKey, messageID, string(task.TaskStatusFailed), strconv.FormatInt(time.Now().Unix(), 10), strconv.FormatInt(taskID, 10), string(task.TaskStatusWaitingRetry)).Result()
	if err != nil {
		return fmt.Errorf("error occur while transition ScheduleRetry: %w", err)
	}
	return nil
}

func (t *Transition) RetryTaskProduce(ctx context.Context, taskID int64) error {
	script := redis.NewScript(`
		local score = redis.call("ZSCORE",KEYS[1],ARGV[1])
		if not score then
			return -1
		end
		if tonumber(score) > tonumber(ARGV[2]) then
			return 0
		end

		local status = redis.call("HGET",KEYS[2],"Status")
		if not status then
			return -1
		end
		if status ~= ARGV[3] then
			return 0
		end

		redis.call("HSET",KEYS[2],
		"Status",ARGV[4]
		)
		redis.call("XADD",KEYS[3],"*","task_id",ARGV[1])
		redis.call("ZREM",KEYS[1],ARGV[1])
		return 1
		`)
	_, err := script.Run(ctx, t.redisClient, []string{rediskey.RetryQueueRetryKey, t.getTaskStoreKey(taskID), rediskey.QueueStreamKey}, strconv.FormatInt(taskID, 10), strconv.FormatInt(time.Now().Unix(), 10), string(task.TaskStatusWaitingRetry), string(task.TaskStatusPending)).Result()
	if err != nil {
		return fmt.Errorf("error occur while retryqueue scan: %w", err)
	}
	return nil
}

func (t *Transition) TaskFinish(ctx context.Context, taskID int64, messageID string) error {
	script := redis.NewScript(`
	local status = redis.call("HGET",KEYS[1],"Status")
	if not status then
		return -1
	end
	if status == ARGV[1] then
		redis.call("XACK",KEYS[2],ARGV[2],ARGV[3])	
		return 2
	end
	if status ~= ARGV[4] then
		return 0
	end

	redis.call("HSET",KEYS[1],"Status",ARGV[1])
	redis.call("XACK",KEYS[2],ARGV[2],ARGV[3])	
	return 1
	`)
	_, err := script.Run(ctx, t.redisClient, []string{t.getTaskStoreKey(taskID), rediskey.QueueStreamKey}, string(task.TaskStatusFinished), rediskey.QueueGroupKey, messageID, string(task.TaskStatusRunning)).Result()
	if err != nil {
		return fmt.Errorf("error occur while transition taskfinish: %w", err)
	}
	return nil
}
