package monitor_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"log"
	"rabbit-monitor/monitor"
	"rabbit-monitor/monitor/internal/local"
	"testing"
	"time"
)

func TestConnection_Reconnect(t *testing.T) {
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
	// ShutdownContainer the local rabbit instance
	require.NoError(t, local.ShutdownContainer())
	// Validate that the Connection is not working
	require.True(t, conn.GetConnection().IsClosed())
	//Restart the local rabbit instance.
	require.NoError(t, local.InitializeContainer())
	// Retry until the Connection is re-established. If the Connection is not re-established in 50 seconds, the test will fail.
	require.NoError(t, retry(func() bool {
		return conn.GetConnection().IsClosed()
	}, false, 10, 5*time.Second))
}

func TestConnection_Close(t *testing.T) {
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
	// Close the Connection
	require.NoError(t, conn.GetConnection().Close())
	// Validate that the Connection is not working
	require.True(t, conn.GetConnection().IsClosed())
	// Validate that the Connection is still not working after one minute because the monitor goroutine has been
	// shut down correctly after the connection was closed.
	time.Sleep(1 * time.Minute)
	require.True(t, conn.GetConnection().IsClosed())
}

func TestConnection_ContextCancellation(t *testing.T) {
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
	// Cancel the context
	cancel()
	time.Sleep(time.Second * 10)
	// Validate that the Connection is not working because the context was cancelled and the monitor goroutine shutoff the connection before returning
	require.True(t, conn.GetConnection().IsClosed())
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
