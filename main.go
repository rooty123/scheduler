package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

var (
	redisClient *redis.Client
	log         *logrus.Entry
)

func initLogger() {
	l := logrus.New()
	l.SetFormatter(&logrus.JSONFormatter{})
	if lvl, err := logrus.ParseLevel(os.Getenv("LOG_LEVEL")); err == nil {
		l.SetLevel(lvl)
	}
	name := os.Getenv("SERVICE_NAME")
	if name == "" {
		name = "scheduler"
	}
	log = l.WithField("service_name", name)
}

func init() {
	initLogger()

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})
}

func sendMessageToSubscribers() {
	ctx := context.Background()
	payload := "Daily scheduled message"

	if err := redisClient.Publish(ctx, "send_message", payload).Err(); err != nil {
		log.WithError(err).Error("Error publishing message")
		return
	}
	log.WithField("channel", "send_message").Info("Scheduled message published")
}

func main() {
	c := cron.New()
	if _, err := c.AddFunc("0 10 * * *", sendMessageToSubscribers); err != nil {
		log.WithError(err).Fatal("Error scheduling job")
	}

	c.Start()
	log.Info("Scheduler started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	ctx := c.Stop()
	<-ctx.Done()
	log.Info("Gracefully shutting down")
	os.Exit(0)
}
