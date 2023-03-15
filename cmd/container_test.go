package cmd

import (
	"context"
	"errors"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/formancehq/ledger/pkg/bus"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func TestContainers(t *testing.T) {
	db := pgtesting.NewPostgresDatabase(t)

	type testCase struct {
		name    string
		options []fx.Option
		init    func(*viper.Viper)
	}

	for _, tc := range []testCase{
		{
			name: "default-with-opentelemetry-traces-on-stdout",
			init: func(v *viper.Viper) {
				v.Set(otlptraces.OtelTracesFlag, true)
				v.Set(otlptraces.OtelTracesExporterFlag, "stdout")
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
				}, fx.ParamTags(``, ``, ``, otlptraces.TracerProviderOptionKey))),
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-stdout-and-batch",
			init: func(v *viper.Viper) {
				v.Set(otlptraces.OtelTracesFlag, true)
				v.Set(otlptraces.OtelTracesExporterFlag, "stdout")
				v.Set(otlptraces.OtelTracesBatchFlag, true)
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
				}, fx.ParamTags(``, ``, ``, otlptraces.TracerProviderOptionKey))),
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-otlp",
			init: func(v *viper.Viper) {
				v.Set(otlptraces.OtelTracesFlag, true)
				v.Set(otlptraces.OtelTracesExporterFlag, "otlp")
			},
		},
		{
			name: "default-with-opentelemetry-traces-on-jaeger",
			init: func(v *viper.Viper) {
				v.Set(otlptraces.OtelTracesFlag, true)
				v.Set(otlptraces.OtelTracesExporterFlag, "jaeger")
			},
		},
		{
			name: "pg",
			init: func(v *viper.Viper) {
				v.Set(storagePostgresConnectionStringFlag, db.ConnString())
			},
			options: []fx.Option{
				fx.Invoke(func(lc fx.Lifecycle, t *testing.T, driver storage.Driver) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							store, _, err := driver.GetLedgerStore(ctx, "testing", true)
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
			name: "event-bus",
			init: func(v *viper.Viper) {},
			options: []fx.Option{
				fx.Invoke(func(lc fx.Lifecycle, ch *gochannel.GoChannel, resolver *ledger.Resolver) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							messages, err := ch.Subscribe(ctx, bus.EventTypeSavedMetadata)
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
								logs, err := l.SaveMeta(ctx, core.MetaTargetTypeAccount, "world", core.Metadata{"foo": []byte(`"bar"`)})
								if err == nil {
									err = logs.Wait(ctx)
								}
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
				fx.Provide(func() logging.Logger {
					return logging.FromContext(context.Background())
				}),
			)
			v := viper.New()
			// Default options
			v.Set(storagePostgresConnectionStringFlag, db.ConnString())
			v.Set(cacheCapacityBytes, 100000000)
			v.Set(cacheMaxNumKeys, 100)
			tc.init(v)
			app := NewContainer(v, options...)

			require.NoError(t, app.Start(context.Background()))
			defer func(app *fx.App, ctx context.Context) {
				require.NoError(t, app.Stop(ctx))
			}(app, context.Background())

			select {
			case <-run:
			default:
				t.Fatal("application not started correctly")
			}
		})
	}

}
