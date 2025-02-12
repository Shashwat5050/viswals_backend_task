package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/viswals_backend_task/core/dto"
	encryptionutils "github.com/viswals_backend_task/core/encryptions"
	"github.com/viswals_backend_task/core/models"
	database "github.com/viswals_backend_task/core/postgres"
	"go.uber.org/zap"
	utils"github.com/viswals_backend_task/core/utils"
)

var (
	defaultTimeout = time.Second * 15
)

// Consumer handles the consumption of messages from the queue
type Consumer struct {
	queueManager MessageBroker
	messageChan  <-chan amqp.Delivery
	logger       *zap.Logger
	userDatabase userStoreProvider
	cacheStorage cacheStoreProvider
	encryption   *encryptionutils.Encryption
}

// NewConsumer initializes a new Consumer instance
func NewConsumer(queueManager MessageBroker, userDatabase userStoreProvider, cacheStorage cacheStoreProvider, encryption *encryptionutils.Encryption, logger *zap.Logger) (*Consumer, error) {
	messageChan, err := queueManager.Consume()
	if err != nil {
		return nil, err
	}

	return &Consumer{
		queueManager: queueManager,
		messageChan:  messageChan,
		logger:       logger,
		userDatabase: userDatabase,
		cacheStorage: cacheStorage,
		encryption:   encryption,
	}, nil
}

// StartConsumption starts the message consumption process
func (c *Consumer) StartConsumption(wg *sync.WaitGroup, bufferSize int) {
	defer wg.Done()

	inputChan := make(chan []byte, bufferSize)
	outputChan := make(chan []*models.UserDetails, bufferSize)
	errorChan := make(chan error, 10)

	workerGroup := new(sync.WaitGroup)

	workerGroup.Add(1)
	go c.transformMessages(workerGroup, inputChan, outputChan, errorChan)

	workerGroup.Add(1)
	go c.storeUserDetails(workerGroup, outputChan, errorChan)

	workerGroup.Add(1)
	go c.logErrors(workerGroup, errorChan)

	for message := range c.messageChan {
		body := message.Body
		if body == nil {
			continue
		}
		inputChan <- body
	}

	c.logger.Info("Message consumption stopped")
	close(inputChan)
	workerGroup.Wait()
}

// logErrors listens for errors and logs them
func (c *Consumer) logErrors(wg *sync.WaitGroup, errorChan chan error) {
	defer wg.Done()
	for err := range errorChan {
		if err != nil {
			c.logger.Error("Error occurred during consumption", zap.Error(err))
		}
	}
}

// transformMessages unmarshals messages from JSON into user details
func (c *Consumer) transformMessages(wg *sync.WaitGroup, inputChan chan []byte, outputChan chan []*models.UserDetails, errorChan chan error) {
	defer wg.Done()
	defer close(outputChan)
	for message := range inputChan {
		// c.logger.Debug("Received message", zap.ByteString("message", message))
		var rawData []*dto.RawUserData
		err := json.Unmarshal(message, &rawData)
		if err != nil {
			// errorChan <- fmt.Errorf("failed to unmarshal message: %w", err)
			// continue
			// If unmarshaling as an array fails, try unmarshaling as a single object
			var singleRawData dto.RawUserData
			if err := json.Unmarshal(message, &singleRawData); err != nil {
				errorChan <- fmt.Errorf("failed to unmarshal message: %w", err)
				continue
			}

			// If successful, wrap the single object into a slice for uniform processing
			rawData = append(rawData, &singleRawData)
		}
		// Transform RawUserData to UserDetails
		var userDetails []*models.UserDetails
		for _, raw := range rawData {
			userDetails = append(userDetails, &models.UserDetails{
				ID:           raw.Id,
				FirstName:    raw.FirstName,
				LastName:     raw.LastName,
				EmailAddress: raw.Email,
				CreatedAt:    utils.ConvertUnixToTime(raw.CreatedAt),
				DeletedAt:    utils.ConvertUnixToTime(raw.DeletedAt),
				MergedAt:     utils.ConvertUnixToTime(raw.MergedAt),
				ParentUserId: raw.ParentUserId,
			})
		}

		c.logger.Debug("Transformed data", zap.Int("user_count", len(userDetails)))
		outputChan <- userDetails
	}

	c.logger.Info("Message transformation stopped")
}

// storeUserDetails processes and saves user details to the database and cache
func (c *Consumer) storeUserDetails(wg *sync.WaitGroup, outputChan chan []*models.UserDetails, errorChan chan error) {
	defer wg.Done()
	defer close(errorChan)

	for userDetailsBatch := range outputChan {
		for _, userDetails := range userDetailsBatch {
			encryptedEmail, err := c.encryption.Encrypt(userDetails.EmailAddress)
			if err != nil {
				c.logger.Error("Error encrypting email", zap.Error(err))
				errorChan <- err
				continue
			}
			userDetails.EmailAddress = encryptedEmail

			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)

			// Save user details in the database
			err = c.userDatabase.CreateUser(ctx, userDetails)
			if err != nil {
				if errors.Is(err, database.ErrDuplicate) {
					c.logger.Warn("User already exists", zap.Error(err), zap.Any("user", userDetails))
				}
				errorChan <- err
				cancel()
				continue
			}

			// Save user details in the cache
			err = c.cacheStorage.Set(ctx, fmt.Sprint(userDetails.ID), userDetails)
			if err != nil {
				c.logger.Warn("Failed to cache user data", zap.Error(err), zap.Any("user", userDetails))
			}
			cancel()
		}
	}

	c.logger.Info("User detail storage stopped")
}

// Close gracefully shuts down the Consumer
func (c *Consumer) Close() error {
	return c.queueManager.Close()
}
