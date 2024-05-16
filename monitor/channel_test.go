package monitor_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"log"
	"rabbitmq-wrapper/monitor"
	"rabbitmq-wrapper/monitor/internal/local"
	"testing"
	"time"
)

func TestChannel_Reconnect(t *testing.T) {
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
	// Validate that the Connection is working too
	require.False(t, conn.GetConnection().IsClosed())
}

func TestChannel_Close(t *testing.T) {
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
	// Close the Channel
	require.NoError(t, ch.GetChannel().Close())
	// Validate that the Channel is not working
	require.True(t, ch.GetChannel().IsClosed())
	// Validate that the Channel is still closed after one minute because the monitor goroutine has been shut down
	// correctly after the channel was closed.
	time.Sleep(1 * time.Minute)
	require.True(t, ch.GetChannel().IsClosed())
	// Validate that the Connection is still working
	require.False(t, conn.GetConnection().IsClosed())
}

func TestChannel_ContextCancellation_I(t *testing.T) {
	createConnection, err := local.CreateConnection()
	require.NoError(t, err)
	connectionCtx, connectionCtxCancel := context.WithCancel(context.Background())
	defer connectionCtxCancel()
	conn, err := monitor.Connection(connectionCtx, monitor.ConnectionConfig{
		CreateConnection: createConnection,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)
	// Validate that the Connection is working
	require.False(t, conn.GetConnection().IsClosed())
	channelCtx, channelCtxCancel := context.WithCancel(context.Background())
	ch, err := monitor.Channel(channelCtx, monitor.ChannelConfig{
		ConnectionMonitor: conn,
		LogError: func(ctx context.Context, err error) {
			log.Printf("Error: %s", err.Error())
		},
	})
	require.NoError(t, err)
	// Validate that the Channel is working
	require.False(t, ch.GetChannel().IsClosed())
	// Cancel the channel context
	channelCtxCancel()
	time.Sleep(time.Second * 10)
	// Validate that the Channel is closed because the context was cancelled and the monitor goroutine shutoff the channel before returning
	require.True(t, ch.GetChannel().IsClosed())
	// Validate that the Connection is still working
	require.False(t, conn.GetConnection().IsClosed())
}

func TestChannel_ContextCancellation_II(t *testing.T) {
	createConnection, err := local.CreateConnection()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
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
	// Validate that the Connection is working
	require.False(t, conn.GetConnection().IsClosed())
	// Validate that the Channel is working
	require.False(t, ch.GetChannel().IsClosed())
	// Cancel the channel context
	cancel()
	time.Sleep(time.Second * 10)
	// Validate that the Channel is closed because the context was cancelled and the monitor goroutine shutoff the channel before returning
	require.True(t, ch.GetChannel().IsClosed())
	// Validate that the Connection is closed because the context was cancelled and the monitor goroutine shutoff the connection before returning
	require.True(t, conn.GetConnection().IsClosed())
}
