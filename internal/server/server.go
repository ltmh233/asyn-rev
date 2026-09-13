package server

import (
	"asyn-rev/internal/task"
	"encoding/json"

	"github.com/gin-gonic/gin"
)

type CreateTaskRequest struct {
	Type    task.Type       `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type GetTaskRequest struct {
	ID int64 `form:"id"`
}

type Server struct {
	service *task.Service
}

func (s *Server) Router() *gin.Engine {
	r := gin.Default()

	r.POST("/createtask", s.createTask)
	r.GET("/gettask", s.getTask)

	return r
}

func (s *Server) createTask(c *gin.Context) {
	req := CreateTaskRequest{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"message": err.Error()})
		return
	}
	ctx := c.Request.Context()

	taskID, err := s.service.CreateTask(ctx, req.Type, req.Payload)
	if err != nil {
		c.JSON(500, gin.H{"message": err.Error()})
		return
	}

	c.JSON(200, gin.H{"task_id": taskID})

}

func (s *Server) getTask(c *gin.Context) {
	req := GetTaskRequest{}
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(400, gin.H{"message": err.Error()})
		return
	}
	ctx := c.Request.Context()

	task, err := s.service.GetTask(ctx, req.ID)
	if err != nil {
		c.JSON(500, gin.H{"message": err.Error()})
		return
	}

	c.JSON(200, gin.H{"task": task})
}

func New(s *task.Service) *Server {
	return &Server{service: s}
}
