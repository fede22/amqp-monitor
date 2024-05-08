package channel_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"log"
	"rabbitmq-wrapper/pkg/channel"
	"rabbitmq-wrapper/pkg/connection"
	"rabbitmq-wrapper/pkg/internal/local"
	"testing"
	"time"
)

func TestChannel_Reconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	createConnection, err := local.CreateConnection()
	require.NoError(t, err)
	conn, err := connection.New(ctx, connection.Config{
		CreateConnection: createConnection,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)
	// Validate that the Connection is working
	require.False(t, conn.GetConnection().IsClosed())
	ch, err := channel.New(ctx, channel.Config{
		Connection: conn,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)
	// Validate that the Channel is working
	require.False(t, ch.GetChannel().IsClosed())
	// ShutdownContainer the local rabbit instance
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
	// Validate that the Connection is working too
	require.False(t, conn.GetConnection().IsClosed())
}

func retry(fn func() bool, expected bool, attempts int, interval time.Duration) error {
	for i := 0; i < attempts; i++ {
		if ok := fn(); ok == expected {
			return nil
		}
		time.Sleep(interval)
	}
	return errors.New("didn't get expected result in time")
}
