package monitor

import (
	"context"
	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type consumerMonitor struct {
	config          ConsumerConfig
	messagesChannel <-chan amqp.Delivery
	out             chan amqp.Delivery
}

type ConsumerConfig struct {
	CreateConsumer func(ctx context.Context) (<-chan amqp.Delivery, error) `validate:"required"`
	LogError       func(ctx context.Context, err error)                    `validate:"required"`
}

func Consumer(ctx context.Context, config ConsumerConfig) (<-chan amqp.Delivery, error) {
	if err := validator.New().Struct(config); err != nil {
		return nil, err
	}
	messagesChannel, err := config.CreateConsumer(ctx)
	if err != nil {
		return nil, err
	}
	m := &consumerMonitor{
		config:          config,
		messagesChannel: messagesChannel,
		out:             make(chan amqp.Delivery),
	}
	go m.monitor(ctx)
	return m.out, nil
}

func (m *consumerMonitor) monitor(ctx context.Context) {
	defer close(m.out)
	for {
		select {
		case msg, ok := <-m.messagesChannel:
			if !ok {
				m.renewConsumerWithBackoff(ctx)
				break
			}
			m.out <- msg
		case <-ctx.Done():
			return
		}
	}
}

func (m *consumerMonitor) renewConsumerWithBackoff(ctx context.Context) {
	backoffTime := time.Second
	timer := time.NewTimer(backoffTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := m.renewConsumer(ctx); err != nil {
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

func (m *consumerMonitor) renewConsumer(ctx context.Context) error {
	newMessagesChannel, err := m.config.CreateConsumer(ctx)
	if err != nil {
		m.config.LogError(ctx, err)
		return err
	}
	m.messagesChannel = newMessagesChannel
	return nil
}
