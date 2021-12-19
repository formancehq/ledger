package cmd

import (
	"context"
	"go.uber.org/fx"
	"testing"
)

func TestContainer(t *testing.T) {
	run := make(chan struct{}, 1)
	app := NewContainer(
		WithOption(fx.Invoke(func() {
			run <- struct{}{}
		})),
	)
	err := app.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer app.Stop(context.Background())

	select {
	case <-run:
	default:
		t.Fatal("application not started correctly")
	}
}
