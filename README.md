# AMQP Monitor

This project provides a set of utilities for monitoring and managing AMQP connections, channels, and consumers in Go. The package ensures robust handling of AMQP resources, including automatic reconnection with exponential backoff in case of closures or failures.

## Features

- **Connection Monitor**: Manages the AMQP connection lifecycle with automatic reconnection.
- **Channel Monitor**: Manages the AMQP channel lifecycle with automatic reconnection.
- **Consumer Monitor**: Manages the AMQP consumer lifecycle, ensuring message consumption continues seamlessly.

## Installation

To install the package, run:

```sh
go get github.com/fede22/amqp-monitor
```

## Usage

### Connection Monitor

The `connectionMonitor` manages the AMQP connection lifecycle, automatically reconnecting in case of connection failure. The monitor shuts down when the context it is passed is cancelled.

```go
package main

import (
    "amqp-monitor/monitor"
    "context"
    "log"

    amqp "github.com/rabbitmq/amqp091-go"
)

func createConnection(ctx context.Context) (*amqp.Connection, error) {
    return amqp.Dial("amqp://guest:guest@localhost:5672/")
}

func logError(ctx context.Context, err error) {
    log.Println("Error:", err)
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    config := monitor.ConnectionConfig{
        CreateConnection: createConnection,
        LogError:         logError,
    }

    connMonitor, err := monitor.Connection(ctx, config)
    if err != nil {
        log.Fatalf("Failed to create connection monitor: %v", err)
    }

    // Use connMonitor.GetConnection() to get the current connection
}
```

### Channel Monitor

Set up the connection like in the previous example and then:

```go
package main

import (
    "amqp-monitor/monitor"
    "context"
    "log"

    amqp "github.com/rabbitmq/amqp091-go"
)

func createConnection(ctx context.Context) (*amqp.Connection, error) {
    return amqp.Dial("amqp://guest:guest@localhost:5672/")
}

func logError(ctx context.Context, err error) {
    log.Println("Error:", err)
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    connConfig := monitor.ConnectionConfig{
        CreateConnection: createConnection,
        LogError:         logError,
    }

    connMonitor, err := monitor.Connection(ctx, connConfig)
    if err != nil {
        log.Fatalf("Failed to create connection monitor: %v", err)
    }

    chanConfig := monitor.ChannelConfig{
        ConnectionMonitor: connMonitor,
        LogError:          logError,
    }

    chanMonitor, err := monitor.Channel(ctx, chanConfig)
    if err != nil {
        log.Fatalf("Failed to create channel monitor: %v", err)
    }

    // Use chanMonitor.GetChannel() to get the current channel
}
```

### Consumer Monitor

Set up the connection and channel like in the previous examples and then:

```go
package main

import (
    "amqp-monitor/monitor"
    "context"
    "log"

    amqp "github.com/rabbitmq/amqp091-go"
)

func createConnection(ctx context.Context) (*amqp.Connection, error) {
    return amqp.Dial("amqp://guest:guest@localhost:5672/")
}

func logError(ctx context.Context, err error) {
    log.Println("Error:", err)
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    connConfig := monitor.ConnectionConfig{
        CreateConnection: createConnection,
        LogError:         logError,
    }

    connMonitor, err := monitor.Connection(ctx, connConfig)
    if err != nil {
        log.Fatalf("Failed to create connection monitor: %v", err)
    }

    chanConfig := monitor.ChannelConfig{
        ConnectionMonitor: connMonitor,
        LogError:          logError,
    }

    chanMonitor, err := monitor.Channel(ctx, chanConfig)
    if err != nil {
        log.Fatalf("Failed to create channel monitor: %v", err)
    }

    consumerConfig := monitor.ConsumerConfig{
        CreateConsumer: func(ctx context.Context) (<-chan amqp.Delivery, error) {
            return chanMonitor.GetChannel().Consume(
                "queue_name",
                "",
                true,
                false,
                false,
                false,
                nil,
            )
        },
        LogError: logError,
    }

    messages, err := monitor.Consumer(ctx, consumerConfig)
    if err != nil {
        log.Fatalf("Failed to create consumer monitor: %v", err)
    }

    go func() {
        for msg := range messages {
            log.Printf("Received message: %s", msg.Body)
        }
    }()

    // Block main goroutine to keep the application running
    select {}
}
```

## Configuration

### ConnectionConfig

- `CreateConnection func(ctx context.Context) (*amqp.Connection, error)`: A function to create a new AMQP connection.
- `LogError func(ctx context.Context, err error)`: A function to log errors.

### ChannelConfig

- `ConnectionMonitor *connectionMonitor`: A reference to an existing connection monitor.
- `LogError func(ctx context.Context, err error)`: A function to log errors.

### ConsumerConfig

- `CreateConsumer func(ctx context.Context) (<-chan amqp.Delivery, error)`: A function to create a new AMQP consumer.
- `LogError func(ctx context.Context, err error)`: A function to log errors.

## Testing

To test the package, you will need Docker installed. The tests use the `dockertest` package to handle starting and shutting down the RabbitMQ container automatically.

Run the tests with:

```sh
go test ./...
```

## Contributing

Contributions are welcome! Please submit pull requests and issues on the [GitHub repository](https://github.com/fede22/amqp-monitor).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.