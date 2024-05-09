package consumer

import (
	"context"
	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type consumer struct {
	config          Config
	messagesChannel <-chan amqp.Delivery
	out             chan amqp.Delivery
}

type Config struct {
	CreateConsumer func(ctx context.Context) (<-chan amqp.Delivery, error) `validate:"required"`
	LogError       func(ctx context.Context, err error)                    `validate:"required"`
}

func New(ctx context.Context, config Config) (<-chan amqp.Delivery, error) {
	if err := validator.New().Struct(config); err != nil {
		return nil, err
	}
	messagesChannel, err := config.CreateConsumer(ctx)
	if err != nil {
		return nil, err
	}
	c := &consumer{
		messagesChannel: messagesChannel,
		config:          config,
		out:             make(chan amqp.Delivery),
	}
	go c.monitorConsumer(ctx)
	return c.out, nil
}

func (c *consumer) monitorConsumer(ctx context.Context) {
	for {
		select {
		case msg, ok := <-c.messagesChannel:
			if !ok {
				c.renewConsumerWithBackoff(ctx)
				break
			}
			c.out <- msg
		case <-ctx.Done():
			return
		}
	}
}

func (c *consumer) renewConsumerWithBackoff(ctx context.Context) {
	backoffTime := time.Second
	timer := time.NewTimer(backoffTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := c.renewConsumer(ctx); err != nil {
				backoffTime = min(backoffTime*2, time.Minute*2)
				timer.Reset(backoffTime)
				break
			}
			return
		case <-ctx.Done():
			return
		}
	}
}

func (c *consumer) renewConsumer(ctx context.Context) error {
	newMessagesChannel, err := c.config.CreateConsumer(ctx)
	if err != nil {
		c.config.LogError(ctx, err)
		return err
	}
	c.messagesChannel = newMessagesChannel
	return nil
}
