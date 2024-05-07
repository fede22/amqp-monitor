package connection_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"log"
	"rabbitmq-wrapper/internal/connection"
	"rabbitmq-wrapper/internal/local"
	"testing"
	"time"
)

func TestConnection_Reconnect(t *testing.T) {
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
	// Validate that the connection is working
	require.False(t, conn.IsClosed(ctx))
	// ShutdownContainer the local rabbit instance
	require.NoError(t, local.ShutdownContainer())
	// Validate that the connection is not working
	require.True(t, conn.IsClosed(ctx))
	//Restart the local rabbit instance.
	require.NoError(t, local.InitializeContainer())
	// Retry until the connection is re-established. If the connection is not re-established in 50 seconds, the test will fail.
	require.NoError(t, retry(func() bool {
		return conn.IsClosed(ctx)
	}, false, 10, 5*time.Second))
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
