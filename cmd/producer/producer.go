package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/viswals_backend_task/core/logger"
	"github.com/viswals_backend_task/core/rabbitmq"
	"github.com/viswals_backend_task/internal"
	"go.uber.org/zap"
)

const (
	DevEnvironment  = "development"
	ProdEnvironment = "production"
)

func main() {
	logger, err := logger.NewLogger(os.Stdout, strings.ToLower(os.Getenv("ENVIRONMENT")) == DevEnvironment)
	if err != nil {
		fmt.Println("Error initializing logger:", err)
		return
	}
	logger.Info("Logger initialized successfully")

	// filepath flag
	csvFilePath := flag.String("csv", "", "Path to CSV file")
	flag.Parse()

	if csvFilePath == nil || *csvFilePath == "" {
		fmt.Println("CSV file path is not found in -csv flag looking for env var")
		path, ok := os.LookupEnv("CSV_FILE_PATH")
		if !ok {
			fmt.Println("csv file path is not found in flag and env var,")
			fmt.Println("you can specify csv file with either -csv flag or providing CSV_FILE_PATH environment variable")
			return
		}
		csvFilePath = &path
	}

	// csvReader,err:=csvhelper.OpenFile(*csvFilePath)
	// if err!=nil{
	// 	logger.Error("Error opening csv file",zap.Error(err),zap.String("file path",*csvFilePath))
	// 	return
	// }

	rabbitMQClient, err := rabbitmq.New(rabbitmq.WithConnectionString(os.Getenv("RABBITMQ_CONNECTION_STRING")), rabbitmq.WithQueueName(os.Getenv("RABBITMQ_QUEUE_NAME")))
	if err != nil {
		logger.Error("Error initializing rabbitmq", zap.Error(err))
		return
	}

	producer := internal.NewProducer(logger, rabbitMQClient)

	err = producer.PublishCSVData(context.Background(), os.Getenv("CSV_FILE_PATH"))
	if err != nil {
		logger.Error("Error publishing csv data", zap.Error(err))
		return
	}

}
