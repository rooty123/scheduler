package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

var (
	redisClient *redis.Client
)

func init() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func sendMessageToSubscribers() {
	ctx := context.Background()

	// Example payload - you can modify this as needed
	payload := map[string]interface{}{
		"type": "send_message",
		"data": map[string]interface{}{
			"message": "Daily scheduled message",
			"time":    time.Now().Format(time.RFC3339),
		},
	}

	// Publish to Redis channel
	err := redisClient.Publish(ctx, "scheduler_events", payload).Err()
	if err != nil {
		log.Printf("Error publishing message: %v", err)
		return
	}

	log.Printf("Successfully sent scheduled message to subscribers")
}

func main() {
	// Create a new cron scheduler
	c := cron.New()

	// Add a job that runs at 10:00 every day
	_, err := c.AddFunc("0 10 * * *", sendMessageToSubscribers)
	if err != nil {
		log.Fatalf("Error scheduling job: %v", err)
	}

	// Start the scheduler
	c.Start()
	log.Println("Scheduler started")

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Stop the scheduler
	ctx := c.Stop()
	<-ctx.Done()
	log.Println("Scheduler stopped")
}
