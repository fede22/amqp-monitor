package channel

import (
	"context"
	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"rabbitmq-wrapper/pkg/connection"
	"sync"
	"time"
)

type Channel struct {
	config       Config
	channel      *amqp.Channel
	closeChannel chan *amqp.Error
	mutex        *sync.Mutex
}

type Config struct {
	Connection *connection.Connection               `validate:"required"`
	LogError   func(ctx context.Context, err error) `validate:"required"`
}

func New(ctx context.Context, config Config) (*Channel, error) {
	if err := validator.New().Struct(config); err != nil {
		return nil, err
	}
	channel, err := config.Connection.GetConnection().Channel()
	if err != nil {
		return nil, err
	}
	ch := &Channel{
		channel:      channel,
		closeChannel: make(chan *amqp.Error),
		config:       config,
		mutex:        &sync.Mutex{},
	}
	ch.channel.NotifyClose(ch.closeChannel)
	go ch.monitorChannel(ctx)
	return ch, nil
}

func (ch *Channel) GetChannel() *amqp.Channel {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	return ch.channel
}

func (ch *Channel) setNewChannel(newChan *amqp.Channel) {
	ch.mutex.Lock()
	defer ch.mutex.Unlock()
	ch.channel = newChan
	ch.closeChannel = make(chan *amqp.Error)
	ch.channel.NotifyClose(ch.closeChannel)
}

func (ch *Channel) monitorChannel(ctx context.Context) {
	for {
		select {
		case _, ok := <-ch.closeChannel:
			if !ok {
				return
			}
			ch.renewChannelWithBackoff(ctx)
		case <-ctx.Done():
			if err := ch.GetChannel().Close(); err != nil {
				ch.config.LogError(ctx, err)
			}
			return
		}
	}
}

func (ch *Channel) renewChannelWithBackoff(ctx context.Context) {
	backoffTime := time.Second
	timer := time.NewTimer(backoffTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := ch.renewChannel(ctx); err != nil {
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

func (ch *Channel) renewChannel(ctx context.Context) error {
	newChan, err := ch.config.Connection.GetConnection().Channel()
	if err != nil {
		ch.config.LogError(ctx, err)
		return err
	}
	ch.setNewChannel(newChan)
	return nil
}
