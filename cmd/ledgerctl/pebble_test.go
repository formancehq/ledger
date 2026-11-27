package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestPebbleCommandsRegistered(t *testing.T) {
	t.Parallel()

	root := newRootCommand()
	for _, name := range []string{"db", "find", "lsm", "manifest", "remotecat", "sstable", "wal", "blob"} {
		cmd, remaining, err := root.Find([]string{"pebble", name})
		require.NoError(t, err)
		require.Empty(t, remaining)
		require.Equal(t, "ledgerctl pebble "+name, cmd.CommandPath())
	}
}

func TestPebbleCommandIgnoresConnectionProfile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := newRootCommand()
	root.SetArgs([]string{"pebble", "db", "check", filepath.Join(t.TempDir(), "missing"), "--profile", "missing"})
	err := root.Execute()
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestPebbleCommandNamesRootSpan(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	ctx, span := provider.Tracer("test").Start(context.Background(), "ledgerctl")
	root := newRootCommand()
	root.SetArgs([]string{"pebble", "db", "check", filepath.Join(t.TempDir(), "missing"), "--profile", "missing"})
	err := root.ExecuteContext(ctx)
	span.End()
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")

	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "ledgerctl pebble db check", spans[0].Name)
	require.Contains(t, spans[0].Attributes, attribute.String("ledgerctl.command", "ledgerctl pebble db check"))
}
