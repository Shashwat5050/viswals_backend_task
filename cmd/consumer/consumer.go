package main

import (
	"fmt"
	"os"
	"strings"
	"sync"

	encryptions "github.com/viswals_backend_task/core/encryptions"
	"github.com/viswals_backend_task/core/logger"
	database "github.com/viswals_backend_task/core/postgres"
	"github.com/viswals_backend_task/core/rabbitmq"
	"github.com/viswals_backend_task/core/redis"
	"github.com/viswals_backend_task/internal"
	"go.uber.org/zap"
)

const (
	DevEnvironment  = "development"
	ProdEnvironment = "production"
	bufferSize      = 100
)

func main() {
	logger, err := logger.NewLogger(os.Stdout, strings.ToLower(os.Getenv("ENVIRONMENT")) == DevEnvironment)
	if err != nil {
		fmt.Println("Error initializing logger:", err)
		return
	}
	logger.Info("Logger initialized successfully")

	repository, err := database.New(os.Getenv("POSTGRES_URI"))
	if err != nil {
		logger.Error("Error in initializing repository layer", zap.Error(err))
		return
	}

	doMigration := strings.ToLower(os.Getenv("MIGRATION")) == "true"

	if doMigration {
		dbName := os.Getenv("DATABASE_NAME")
		if dbName == "" {
			logger.Error("database name is not set can't process migration")
			return
		}
		err := repository.Migrate(dbName)
		if err != nil {
			logger.Error("can't migrate database throws error", zap.Error(err))
			return
		}
	}

	redisClient, err := redis.New(os.Getenv("REDIS_URI"), redis.WithTTL(os.Getenv("REDIS_TTL")))
	if err != nil {
		logger.Error("can't initialize redis client", zap.Error(err))
		return
	}

	rabbitMQClient, err := rabbitmq.New(rabbitmq.WithConnectionString(os.Getenv("RABBITMQ_CONNECTION_STRING")), rabbitmq.WithQueueName(os.Getenv("RABBITMQ_QUEUE_NAME")))
	if err != nil {
		logger.Error("Error initializing rabbitmq", zap.Error(err))
		return
	}

	encry, err := encryptions.New([]byte(os.Getenv("ENCRYPTION_KEY")))
	if err != nil {
		logger.Error("encryption key not provided or invalid ", zap.Error(err))
		return
	}

	consumer, err := internal.NewConsumer(rabbitMQClient, repository, redisClient, encry, logger)
	if err != nil {
		logger.Error("can't initialize consumer", zap.Error(err))
		return
	}

	// ctrl:=controller.New(consumer, logger)
	// create a separate go routine to handle upcoming data.
	wg := &sync.WaitGroup{}

	wg.Add(1)

	go consumer.StartConsumption(wg, bufferSize)

	wg.Wait()

	// go func ()  {
	// 	defer wg.Done()
	// }()

}
