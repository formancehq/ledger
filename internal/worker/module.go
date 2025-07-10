package worker

import (
	"context"
	"go.uber.org/fx"
	"reflect"
)

func NewFXModule(configLoader func(v any) error) (fx.Option, error) {

	options := make([]fx.Option, 0)
	for _, factory := range runnerFactories {
		vFactory := reflect.TypeOf(factory)
		method, _ := vFactory.MethodByName("CreateRunner")
		configType := reflect.New(method.Type.In(1)).Interface()
		if err := configLoader(configType); err != nil {
			return nil, err
		}

		ret := reflect.ValueOf(factory).
			MethodByName("CreateRunner").
			Call([]reflect.Value{
				reflect.ValueOf(configType).Elem(),
			})
		if ret[1].Interface() != nil {
			return nil, ret[1].Interface().(error)
		}

		runnerConstructor := ret[0].Interface()

		options = append(options, fx.Provide(
			fx.Annotate(runnerConstructor, fx.ResultTags(`group:"runners"`)),
		))
	}

	options = append(options,
		fx.Invoke(fx.Annotate(func(runners []Runner, lc fx.Lifecycle) {
			for _, runner := range runners {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						go func() {
							if err := runner.Run(context.WithoutCancel(ctx)); err != nil {
								panic(err)
							}
						}()
						return nil
					},
					OnStop: runner.Stop,
				})
			}
		}, fx.ParamTags(`group:"runners"`))),
	)

	return fx.Options(options...), nil
}

type Runner interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

type RunnerFactory[Config any] interface {
	// CreateRunner returns a constructor for the runner
	// It should be passable to fx
	CreateRunner(config Config) (any, error)
}

var runnerFactories = make([]any, 0)

func AllRunnerFactories() []any {
	return runnerFactories
}

func RegisterRunnerFactory[Config any](factory RunnerFactory[Config]) {
	runnerFactories = append(runnerFactories, factory)
}
