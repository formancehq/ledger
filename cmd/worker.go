package cmd

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/worker"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"reflect"
	"strconv"
)

func addWorkerFlags(cmd *cobra.Command) {
	for _, runnerFactory := range worker.AllRunnerFactories() {
		typeOfRunnerFactory := reflect.TypeOf(runnerFactory)
		method, _ := typeOfRunnerFactory.MethodByName("CreateRunner")
		configType := method.Type.In(1)

		for i := 0; i < configType.NumField(); i++ {
			field := configType.Field(i)
			fieldTag := field.Tag
			flag := field.Tag.Get("mapstructure")
			description := fieldTag.Get("description")
			defaultValue := fieldTag.Get("default")

			switch field.Type.Kind() {
			case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
				defaultValue, err := strconv.ParseInt(defaultValue, 10, 64)
				if err != nil {
					panic(err)
				}
				cmd.Flags().Int(flag, int(defaultValue), description)
			case reflect.String:
				cmd.Flags().String(flag, defaultValue, description)
			default:
				panic(fmt.Sprintf("cannot config flag %s as type %T is not handled", flag, field.Type))
			}
		}
	}
}

func NewWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "worker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			workerModule, err := worker.NewFXModule(func(v any) error {
				return MapConfig(cmd, v)
			})
			if err != nil {
				return fmt.Errorf("creating worker module: %w", err)
			}

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{}),
				workerModule,
			).Run(cmd)
		},
	}

	addWorkerFlags(cmd)
	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())

	return cmd
}
