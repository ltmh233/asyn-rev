package main

import (
	"asyn-rev/internal/config"
	"asyn-rev/internal/queue"
	"context"
	"fmt"
	"log"
)

func main() {
	ctx := context.Background()
	config, err := config.LoadConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}
	queue, err := queue.New(ctx, config.Redis)
	if err != nil {
		log.Fatal(err)
	}
	err = queue.Produce(ctx, "task:123")
	if err != nil {
		log.Fatal(err)
	}
	taskID, messageID, err := queue.Consume(ctx, "worker:1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v %v", taskID, messageID)
}
