package main

import (
	"asyn-rev/internal/config"
	"asyn-rev/internal/queue"
	"asyn-rev/internal/redisclient"
	"asyn-rev/internal/retryqueue"
	"asyn-rev/internal/sheduler"
	"asyn-rev/internal/taskstore"
	"context"
	"log"
)

func main() {

	ctx := context.Background()
	config, err := config.LoadConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	redisClient, err := redisclient.Init(ctx, config.Redis)
	if err != nil {
		log.Fatal(err)
	}
	queue, err := queue.New(ctx, redisClient)
	if err != nil {
		log.Fatal(err)
	}
	taskStore := taskstore.New(ctx, redisClient)
	retryQueue := retryqueue.New(redisClient, taskStore, queue)

	scheduler := sheduler.New(retryQueue, taskStore, queue)
	scheduler.Run(ctx)
}
