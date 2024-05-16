package monitor

import (
	"context"
	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type channelMonitor struct {
	config       ChannelConfig
	channel      *amqp.Channel
	closeChannel chan *amqp.Error
	mutex        *sync.Mutex
}

type ChannelConfig struct {
	ConnectionMonitor *connectionMonitor                   `validate:"required"`
	LogError          func(ctx context.Context, err error) `validate:"required"`
}

func Channel(ctx context.Context, config ChannelConfig) (*channelMonitor, error) {
	if err := validator.New().Struct(config); err != nil {
		return nil, err
	}
	channel, err := config.ConnectionMonitor.GetConnection().Channel()
	if err != nil {
		return nil, err
	}
	m := &channelMonitor{
		channel:      channel,
		closeChannel: make(chan *amqp.Error),
		config:       config,
		mutex:        &sync.Mutex{},
	}
	m.channel.NotifyClose(m.closeChannel)
	go m.monitor(ctx)
	return m, nil
}

func (m *channelMonitor) GetChannel() *amqp.Channel {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.channel
}

func (m *channelMonitor) setNewChannel(newChan *amqp.Channel) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.channel = newChan
	m.closeChannel = make(chan *amqp.Error)
	m.channel.NotifyClose(m.closeChannel)
}

func (m *channelMonitor) monitor(ctx context.Context) {
	for {
		select {
		case _, ok := <-m.closeChannel:
			if !ok {
				return
			}
			m.renewChannelWithBackoff(ctx)
		case <-ctx.Done():
			if err := m.GetChannel().Close(); err != nil {
				m.config.LogError(ctx, err)
			}
			return
		}
	}
}

func (m *channelMonitor) renewChannelWithBackoff(ctx context.Context) {
	backoffTime := time.Second
	timer := time.NewTimer(backoffTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := m.renewChannel(ctx); err != nil {
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

func (m *channelMonitor) renewChannel(ctx context.Context) error {
	newChan, err := m.config.ConnectionMonitor.GetConnection().Channel()
	if err != nil {
		m.config.LogError(ctx, err)
		return err
	}
	m.setNewChannel(newChan)
	return nil
}
