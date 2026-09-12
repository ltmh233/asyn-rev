package main

import (
	"asyn-rev/internal/config"
	"asyn-rev/internal/queue"
	"asyn-rev/internal/redisclient"
	"asyn-rev/internal/taskstore"
	"context"
	"log"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
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

	router := gin.Default()

	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:8100"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	router.POST()

	router.Run()
}
