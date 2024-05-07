package connection_test

import (
	"log"
	"rabbitmq-wrapper/internal/local"
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
