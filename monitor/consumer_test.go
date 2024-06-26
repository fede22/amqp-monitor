package monitor_test

import (
	"amqp-monitor/monitor"
	"amqp-monitor/monitor/internal/local"
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
	"time"
)

func TestConsumer_Reconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	createConnection, err := local.CreateConnection()
	require.NoError(t, err)
	conn, err := monitor.Connection(ctx, monitor.ConnectionConfig{
		CreateConnection: createConnection,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)
	// Validate that the Connection is working
	require.False(t, conn.GetConnection().IsClosed())
	ch, err := monitor.Channel(ctx, monitor.ChannelConfig{
		ConnectionMonitor: conn,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)
	// Validate that the Channel is working
	require.False(t, ch.GetChannel().IsClosed())
	q, err := ch.GetChannel().QueueDeclare(
		"test-queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	require.NoError(t, err)

	// Create a consumer function for testing
	createConsumer := func(ctx context.Context) (<-chan amqp.Delivery, error) {
		return ch.GetChannel().Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
	}

	// Initialize the consumer
	msgs, err := monitor.Consumer(ctx, monitor.ConsumerConfig{
		CreateConsumer: createConsumer,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)

	// Send a message to the queue
	require.NoError(t, ch.GetChannel().PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("Hello World"),
	}))

	//Consume the message
	require.Equal(t, "Hello World", string((<-msgs).Body))

	// Shutdown the local rabbit instance
	require.NoError(t, local.ShutdownContainer())
	// Validate that the Connection is not working
	require.True(t, conn.GetConnection().IsClosed())
	// Validate that the Channel is not working
	require.True(t, ch.GetChannel().IsClosed())
	//Restart the local rabbit instance.
	require.NoError(t, local.InitializeContainer())
	// Retry until the Channel is re-established. If the Channel is not re-established in 50 seconds, the test will fail.
	require.NoError(t, retry(func() bool {
		return ch.GetChannel().IsClosed()
	}, false, 10, 5*time.Second))
	//Validate that the Connection is working too
	require.False(t, conn.GetConnection().IsClosed())
	//Redeclare the queue because the container is no durable.
	q, err = ch.GetChannel().QueueDeclare(
		"test-queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	require.NoError(t, err)
	// Send a new message to the queue
	require.NoError(t, ch.GetChannel().PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte("Hello World, again!"),
	}))
	//Consume the message
	require.Equal(t, "Hello World, again!", string((<-msgs).Body))
}
