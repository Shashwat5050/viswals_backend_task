package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	csvhelper "github.com/viswals_backend_task/core/csv-helper"
	"github.com/viswals_backend_task/core/models"
	"go.uber.org/zap"
)

type Producer struct {
	logger        *zap.Logger
	messageBroker MessageBroker
}

func NewProducer(logger *zap.Logger, rabbitMQClient MessageBroker) *Producer {
	return &Producer{
		logger:        logger,
		messageBroker: rabbitMQClient,
	}
}

func (p *Producer) PublishCSVData(ctx context.Context, filePath string) error {

	csvReader, err := csvhelper.OpenFile(filePath)
	if err != nil {
		return err
	}

	userDataCh := make(chan models.UserDetails, 100)

	totalWorkers := 5

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(userDataCh)
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				p.logger.Error("error in reading csv file", zap.Error(err))
				continue
			}
			user, err := p.ConvertCSVRecordToUserData(ctx, record)
			if err != nil {
				p.logger.Error("failed to parse CSV record", zap.Error(err))
				continue
			}
			userDataCh <- user
		}

	}()
	// worker goroutines
	for i := 0; i < totalWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for userData := range userDataCh {
				err := p.PublishUserDataToQueue(ctx, userData)
				if err != nil {
					p.logger.Error("failed to publish user data to queue", zap.Error(err))
				} else {
					p.logger.Info("successfully published user data to queue")
				}
			}
		}(i)
	}

	wg.Wait() // wait for all goroutines to finish
	p.logger.Info("All CSV data processed and published to queue")
	return nil

}

func (p *Producer) PublishUserDataToQueue(ctx context.Context, userdata models.UserDetails) error {

	message, err := json.Marshal(userdata)
	if err != nil {
		p.logger.Error("failed to marshal user data", zap.Error(err))
		return err
	}

	// publish message to RabbitMQ queue
	err = p.messageBroker.Publish(ctx, message)
	if err != nil {
		p.logger.Error("failed to publish message to queue", zap.Error(err))
		return err
	}

	p.logger.Info("message published to queue", zap.String("user", string(message)))

	return nil
}

func (p *Producer) ConvertCSVRecordToUserData(ctx context.Context, record []string) (models.UserDetails, error) {
	userData := models.UserDetails{}
	var err error

	// Parses id
	if len(record) >= 1 {
		userData.ID, err = strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			p.logger.Error("invalid id in record", zap.Error(err))
			return userData, fmt.Errorf("invalid id in record: %w", err)
		}
	}

	// Parse first_name
	if len(record) >= 2 {
		userData.FirstName = record[1]
	}

	// Parse first_name
	if len(record) >= 3 {
		userData.LastName = record[2]
	}

	// Parse email
	if len(record) >= 4 {
		userData.EmailAddress = record[3]
	}

	// Parse created_at
	if len(record) >= 5 {
		userData.CreatedAt, err = parseTimestamp(record[4])
		if err != nil {
			return userData, fmt.Errorf("invalid created_at in record: %w", err)
		}
	}

	// Parse deleted_at
	if len(record) >= 6 {
		userData.DeletedAt, err = parseTimestamp(record[5])
		if err != nil {
			return userData, fmt.Errorf("invalid deleted_at in record: %w", err)
		}
	}

	// Parse merged_at
	if len(record) >= 7 {
		userData.MergedAt, err = parseTimestamp(record[6])
		if err != nil {
			return userData, fmt.Errorf("invalid deleted_at in record: %w", err)
		}
	}

	// Parse parent_user_id
	if len(record) >= 8 {
		if record[7] != "" {
			parentUserId, err := strconv.Atoi(record[7])
			if err != nil {
				return userData, fmt.Errorf("invalid parent_user_id in record: %w", err)
			}

			userData.ParentUserId = int64(parentUserId)
		}
	}

	return userData, nil
}

func parseTimestamp(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil // Return 0 if the timestamp is empty
	}

	timestamp, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil, err
	}

	// Convert int64 to time.Time
	t := time.Unix(timestamp, 0).UTC()
	return &t, nil
}
