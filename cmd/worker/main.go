package main

import (
	"asyn-rev/internal/config"
	"asyn-rev/internal/queue"
	"asyn-rev/internal/redisclient"
	"asyn-rev/internal/registry"
	"asyn-rev/internal/retryqueue"
	"asyn-rev/internal/taskstore"
	"asyn-rev/internal/transition"
	"asyn-rev/internal/worker"
	"context"
	"flag"
	"log"
)

func main() {
	id := flag.Int64("id", 1, "worker id")
	flag.Parse()

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
	registry := registry.New(redisClient)
	transition := transition.New(redisClient)
	retryQueue := retryqueue.New(redisClient, taskStore, queue, transition)

	w := worker.New(*id, queue, taskStore, registry, retryQueue, transition)
	err = w.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
}
