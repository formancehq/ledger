package cmd

import (
	"context"
	"errors"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/numary/go-libs/sharedotlp/sharedotlptraces"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestContainers(t *testing.T) {

	pgServer, err := pgtesting.PostgresServer()
	if !assert.NoError(t, err) {
		return
	}
	defer pgServer.Close()

	type testCase struct {
		name    string
		options []fx.Option
		init    func(*viper.Viper)
	}

	for _, tc := range []testCase{
		{
			name: "default",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-stdout",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
				v.Set(otelTracesFlag, true)
				v.Set(otelTracesExporterFlag, "stdout")
			},
			options: []fx.Option{
				fx.Invoke(fx.Annotate(func(lc fx.Lifecycle, t *testing.T, exp trace.SpanExporter, options ...trace.TracerProviderOption) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							assert.Len(t, options, 2)
							if os.Getenv("CI") == "true" { // runtime.FuncForPC does not return same results locally or in the CI pipeline (probably related to inlining)
								return nil
							}
							var (
								foundWithResource bool
								foundWithSyncer   bool
							)
							for _, opt := range options {
								if strings.Contains(runtime.FuncForPC(reflect.ValueOf(opt).Pointer()).Name(), "trace.WithSyncer") {
									foundWithSyncer = true
								}
								if strings.Contains(runtime.FuncForPC(reflect.ValueOf(opt).Pointer()).Name(), "trace.WithResource") {
									foundWithResource = true
								}
							}
							assert.True(t, foundWithResource)
							assert.True(t, foundWithSyncer)
							return nil
						},
					})
				}, fx.ParamTags(``, ``, ``, sharedotlptraces.TracerProviderOptionKey))),
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-stdout-and-batch",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
				v.Set(otelTracesFlag, true)
				v.Set(otelTracesExporterFlag, "stdout")
				v.Set(otelTracesBatchFlag, true)
			},
			options: []fx.Option{
				fx.Invoke(fx.Annotate(func(lc fx.Lifecycle, t *testing.T, exp trace.SpanExporter, options ...trace.TracerProviderOption) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							if !assert.Len(t, options, 2) {
								return nil
							}
							if os.Getenv("CI") == "true" { // runtime.FuncForPC does not returns same results locally or in the CI pipeline (probably related to inlining)
								return nil
							}
							var (
								foundWithResource bool
								foundWithBatcher  bool
							)
							for _, opt := range options {
								if strings.Contains(runtime.FuncForPC(reflect.ValueOf(opt).Pointer()).Name(), "trace.WithBatch") {
									foundWithBatcher = true
								}
								if strings.Contains(runtime.FuncForPC(reflect.ValueOf(opt).Pointer()).Name(), "trace.WithResource") {
									foundWithResource = true
								}
							}
							assert.True(t, foundWithResource)
							assert.True(t, foundWithBatcher)
							return nil
						},
					})
				}, fx.ParamTags(``, ``, ``, sharedotlptraces.TracerProviderOptionKey))),
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-otlp",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
				v.Set(otelTracesFlag, true)
				v.Set(otelTracesExporterFlag, "otlp")
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-jaeger",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
				v.Set(otelTracesFlag, true)
				v.Set(otelTracesExporterFlag, "jaeger")
			},
		},
		{
			name: "default-with-opentelemetry-metrics-on-noop",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
				v.Set(otelMetricsFlag, true)
				v.Set(otelMetricsExporterFlag, "noop")
			},
		},
		{
			name: "default-with-opentelemetry-metrics-on-otlp",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.SQLite.String())
				v.Set(otelMetricsFlag, true)
				v.Set(otelMetricsExporterFlag, "otlp")
			},
		},
		{
			name: "pg",
			init: func(v *viper.Viper) {
				v.Set(storageDriverFlag, sqlstorage.PostgreSQL.String())
				v.Set(storagePostgresConnectionStringFlag, pgServer.ConnString())
			},
			options: []fx.Option{
				fx.Invoke(func(lc fx.Lifecycle, t *testing.T, driver storage.Driver, storageFactory storage.Driver) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							store, _, err := storageFactory.GetStore(ctx, "testing", true)
							if err != nil {
								return err
							}
							err = store.Close(ctx)
							if err != nil {
								return err
							}
							return nil
						},
					})
				}),
			},
		},
		{
			name: "default-with-lock-strategy-memory",
			init: func(v *viper.Viper) {
				v.Set(lockStrategyFlag, "redis")
			},
		},
		{
			name: "default-with-lock-strategy-none",
			init: func(v *viper.Viper) {
				v.Set(lockStrategyFlag, "none")
			},
		},
		{
			name: "default-with-lock-strategy-redis",
			init: func(v *viper.Viper) {
				v.Set(lockStrategyFlag, "redis")
				v.Set(lockStrategyRedisUrlFlag, "redis://redis:6789")
			},
			options: []fx.Option{
				fx.Invoke(func(lc fx.Lifecycle, resolver *ledger.Resolver) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							l, err := resolver.GetLedger(ctx, uuid.New())
							if err != nil {
								return err
							}
							_, _, err = l.Commit(ctx, nil)
							if !ledger.IsLockError(err) { // No redis in test, it should trigger a lock error
								return err
							}
							return nil
						},
					})
				}),
			},
		},
		{
			name: "event-bus",
			init: func(v *viper.Viper) {},
			options: []fx.Option{
				fx.Invoke(func(lc fx.Lifecycle, ch *gochannel.GoChannel, resolver *ledger.Resolver) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							messages, err := ch.Subscribe(ctx, bus.FallbackTopic)
							if err != nil {
								return err
							}
							name := uuid.New()
							l, err := resolver.GetLedger(ctx, name)
							if err != nil {
								return err
							}
							errCh := make(chan error, 1)
							go func() {
								err := l.SaveMeta(ctx, core.MetaTargetTypeAccount, "world", core.Metadata{"foo": []byte(`"bar"`)})
								if err != nil {
									errCh <- err
								}
							}()
							select {
							case <-ctx.Done():
								return ctx.Err()
							case err := <-errCh:
								return err
							case <-messages:
							case <-time.After(time.Second):
								return errors.New("timeout")
							}
							return nil
						},
					})
				}),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := make(chan struct{}, 1)
			options := append(tc.options,
				fx.Invoke(func() {
					run <- struct{}{}
				}),
				fx.Provide(func() *testing.T {
					return t
				}),
			)
			v := viper.New()
			// Default options
			v.Set(storageDriverFlag, sqlstorage.SQLite.String())
			v.Set(storageDirFlag, "/tmp")
			//v.Set(storageSQLiteDBNameFlag, uuid.New())
			tc.init(v)
			app := NewContainer(v, options...)

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
		})
	}

}
