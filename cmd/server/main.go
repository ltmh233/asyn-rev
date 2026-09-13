package main

import (
	"asyn-rev/internal/config"
	"asyn-rev/internal/queue"
	"asyn-rev/internal/redisclient"
	"asyn-rev/internal/server"
	"asyn-rev/internal/task"
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
	service := task.NewService(queue, taskStore)
	server := server.New(service)

	server.Router().Run(config.Server.Host + ":" + config.Server.Port)
}
