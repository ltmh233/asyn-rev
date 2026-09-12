package server

import (
	"asyn-rev/internal/queue"
	taskstore "asyn-rev/internal/taskStore"

	"github.com/gin-gonic/gin"
)

type Server struct {
	queue     *queue.Queue
	taskStore *taskstore.TaskStore
}

func (s *Server) Router() *gin.Engine {
	r := gin.Default()

	r.POST("/createtask", s.createTask)
	r.GET("/gettask/:id", s.getTask)

	return r
}

func (s *Server) createTask(c *gin.Context) {
	ctx := c.Request.Context()
}

func (s *Server) getTask(c *gin.Context) {

}

func New(queue *queue.Queue, taskStore *taskstore.TaskStore) *Server {
	return &Server{queue: queue, taskStore: taskStore}
}
