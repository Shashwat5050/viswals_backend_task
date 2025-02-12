package rabbitmq

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ represents a RabbitMQ RabbitMQ with connection, channel, and queue.
type RabbitMQ struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
}

type options func(*RabbitMQ) error

func WithQueueName(queueName string) options {
	return func(r *RabbitMQ) error {
		if queueName == "" {
			return errors.New("queue name cannot be empty")
		}
		r.queue.Name = queueName
		return nil
	}
}

func WithConnectionString(connectionString string) options {
	return func(r *RabbitMQ) error {
		if connectionString == "" {
			return errors.New("connection string cannot be empty")
		}
		conn, err := amqp.Dial(connectionString)
		if err != nil {
			return err
		}
		r.conn = conn
		return nil
	}
}

// NewRabbitMQ initializes a new RabbitMQ RabbitMQ with the given URI and queue name.
func New(opts ...options) (*RabbitMQ, error) {
	r := &RabbitMQ{}

	// Apply options
	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	// Ensure the channel is initialized
	if r.conn == nil {
		return nil, errors.New("RabbitMQ connection is not initialized")
	}

	ch, err := r.conn.Channel()
	if err != nil {
		r.conn.Close()
		return nil, err
	}
	r.ch = ch

	// Ensure the queue is initialized
	if r.queue.Name == "" {
		return nil, errors.New("RabbitMQ queue name is not set")
	}

	queue, err := r.ch.QueueDeclare(
		r.queue.Name,
		true,  // Durable
		false, // Auto-delete
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		r.ch.Close()
		r.conn.Close()
		return nil, err
	}

	r.queue = queue
	return r, nil
}

func (c *RabbitMQ) Close() error {
	if c.ch != nil {
		if err := c.ch.Close(); err != nil {
			return err
		}
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Publish sends a message to the RabbitMQ queue.
func (c *RabbitMQ) Publish(ctx context.Context, message []byte) error {
	// Use the default exchange with the queue as the routing key.
	err := c.ch.PublishWithContext(
		ctx,
		"",           // Exchange
		c.queue.Name, // Routing key (queue name)
		true,         // Mandatory
		false,        // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	return err
}

// Consume starts consuming messages from the RabbitMQ queue.
func (c *RabbitMQ) Consume() (<-chan amqp.Delivery, error) {
	deliveries, err := c.ch.Consume(
		c.queue.Name, // Queue name
		"",           // Consumer tag
		true,         // Auto-acknowledge
		false,        // Exclusive
		false,        // No-local
		false,        // No-wait
		nil,          // Arguments
	)
	if err != nil {
		return nil, err
	}
	return deliveries, nil
}
