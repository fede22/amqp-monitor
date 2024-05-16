package monitor

import (
	"context"
	"github.com/go-playground/validator/v10"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
	"time"
)

type connectionMonitor struct {
	config       ConnectionConfig
	connection   *amqp.Connection
	closeChannel chan *amqp.Error
	mutex        *sync.Mutex
}

type ConnectionConfig struct {
	CreateConnection func(ctx context.Context) (*amqp.Connection, error) `validate:"required"`
	LogError         func(ctx context.Context, err error)                `validate:"required"`
}

func Connection(ctx context.Context, config ConnectionConfig) (*connectionMonitor, error) {
	if err := validator.New().Struct(config); err != nil {
		return nil, err
	}
	conn, err := config.CreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	m := &connectionMonitor{
		config:       config,
		connection:   conn,
		closeChannel: conn.NotifyClose(make(chan *amqp.Error)),
		mutex:        &sync.Mutex{},
	}
	go m.monitor(ctx)
	return m, nil
}

func (m *connectionMonitor) GetConnection() *amqp.Connection {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.connection
}

func (m *connectionMonitor) setNewConnection(newConn *amqp.Connection) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.connection = newConn
	m.closeChannel = make(chan *amqp.Error)
	m.connection.NotifyClose(m.closeChannel)
}

func (m *connectionMonitor) monitor(ctx context.Context) {
	for {
		select {
		case _, ok := <-m.closeChannel:
			if !ok {
				return
			}
			m.renewConnectionWithBackoff(ctx)
		case <-ctx.Done():
			if err := m.GetConnection().Close(); err != nil {
				m.config.LogError(ctx, err)
			}
			return
		}
	}
}

func (m *connectionMonitor) renewConnectionWithBackoff(ctx context.Context) {
	backoffTime := time.Second
	timer := time.NewTimer(backoffTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := m.renewConnection(ctx); err != nil {
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

func (m *connectionMonitor) renewConnection(ctx context.Context) error {
	newConn, err := m.config.CreateConnection(ctx)
	if err != nil {
		m.config.LogError(ctx, err)
		return err
	}
	m.setNewConnection(newConn)
	return nil
}
