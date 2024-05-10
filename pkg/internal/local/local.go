package local

import (
	"context"
	"fmt"
	"github.com/ory/dockertest/v3"

	"github.com/ory/dockertest/v3/docker"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	container *dockertest.Resource
)

func CreateConnection() (func(ctx context.Context) (*amqp.Connection, error), error) {
	return func(ctx context.Context) (*amqp.Connection, error) {
		return amqp.Dial("amqp://guest:guest@localhost:5672")
	}, nil
}

func InitializeContainer() error {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return fmt.Errorf("could not construct pool: %s", err)
	}

	err = pool.Client.Ping()
	if err != nil {
		return fmt.Errorf("could not connect to Docker: %s", err)
	}

	// Start a RabbitMQ container
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rabbitmq",
		Tag:        "management-alpine",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5672/tcp":  {{HostIP: "127.0.0.1", HostPort: "5672"}},
			"15672/tcp": {{HostIP: "127.0.0.1", HostPort: "15672"}},
		},
	})
	if err != nil {
		return fmt.Errorf("could not start resource: %s", err)
	}

	rabbitMQURL := "amqp://guest:guest@localhost:5672"

	// exponential backoff-retry, because the RabbitMQ container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		_, err := amqp.Dial(rabbitMQURL)
		if err != nil {
			return err
		}
		return nil

	}); err != nil {
		return fmt.Errorf("could not connect to RabbitMQ: %s", err)
	}
	container = resource
	return nil
}

func ShutdownContainer() error {
	if container == nil {
		return nil
	}
	return container.Close()
}
