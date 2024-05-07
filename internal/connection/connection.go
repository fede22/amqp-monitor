package rabbitmq_wrapper

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type connection struct {
	config       Config
	connection   *amqp.Connection
	closeChannel chan *amqp.Error
}

type Config struct {
	createConnection func(ctx context.Context) (*amqp.Connection, error)
	logError         func(ctx context.Context, err error)
}

func New(ctx context.Context, config Config) (*connection, error) {
	conn, err := config.createConnection(ctx)
	if err != nil {
		return nil, err
	}
	c := &connection{
		connection:   conn,
		closeChannel: make(chan *amqp.Error),
		config:       config,
	}
	c.connection.NotifyClose(c.closeChannel)
	go c.monitorConnection(ctx)
	return c, nil
}

func (c *connection) monitorConnection(ctx context.Context) {
	for {
		select {
		case _, ok := <-c.closeChannel:
			if !ok {
				break
			}
			c.renewConnectionWithBackoff(ctx)
		case <-ctx.Done():
			if err := c.connection.Close(); err != nil {
				c.config.logError(ctx, err)
			}
			return
		}
	}
}

func (c *connection) renewConnectionWithBackoff(ctx context.Context) {
	backoffTime := time.Second
	timer := time.NewTimer(backoffTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err := c.renewConnection(ctx); err != nil {
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

func (c *connection) renewConnection(ctx context.Context) error {
	newConn, err := c.config.createConnection(ctx)
	if err != nil {
		c.config.logError(ctx, err)
		return err
	}
	c.connection = newConn
	c.closeChannel = make(chan *amqp.Error)
	c.connection.NotifyClose(c.closeChannel)
	return nil
}
