package monitor_test

import (
	"amqp-monitor/monitor/internal/local"
	"log"
	"testing"
)

func TestMain(m *testing.M) {
	if err := local.InitializeContainer(); err != nil {
		log.Fatalf("could not initialize local rabbit: %s", err)
	}
	m.Run()
	if err := local.ShutdownContainer(); err != nil {
		log.Fatalf("could not shutdown local rabbit: %s", err)
	}
}
