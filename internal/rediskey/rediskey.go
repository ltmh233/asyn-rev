package rediskey

const (
	RetryQueueRetryKey = "asyn-rev:retry:tasks"
	TaskStoreStoreKey  = "asyn-rev:task_store"
	TaskStoreIDKey     = "asyn-rev:task_id"
	RegistryAliveKey   = "asyn-rev:alive:"
	QueueStreamKey     = "asyn-rev:stream:tasks"
	QueueGroupKey      = "asyn-rev:consume_group:workers"
)
