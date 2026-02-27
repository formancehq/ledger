package drivers

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/platform/postgres"

	"github.com/formancehq/ledger/internal/replication/config"
)

// Registry holds all available drivers
// It implements Factory
type Registry struct {
	constructors map[string]any
	logger       logging.Logger
	store        Store
}

func (c *Registry) RegisterDriver(name string, constructor any) {
	if err := c.registerDriver(name, constructor); err != nil {
		panic(err)
	}
}

func (c *Registry) registerDriver(name string, constructor any) error {
	typeOfConstructor := reflect.TypeOf(constructor)
	if typeOfConstructor.Kind() != reflect.Func {
		return errors.New("constructor must be a func")
	}

	if typeOfConstructor.NumIn() != 2 {
		return errors.New("constructor must take two parameters")
	}

	if typeOfConstructor.NumOut() != 2 {
		return errors.New("constructor must return two values")
	}

	if !typeOfConstructor.In(1).AssignableTo(reflect.TypeOf(new(logging.Logger)).Elem()) {
		return fmt.Errorf("constructor arg 2 must be of kind %s", reflect.TypeOf(new(logging.Logger)).Elem().String())
	}

	errorType := reflect.TypeOf(new(error)).Elem()
	if !typeOfConstructor.Out(1).AssignableTo(errorType) {
		return fmt.Errorf("return 1 must be of kind %s", errorType.String())
	}

	driverType := reflect.TypeOf(new(Driver)).Elem()
	if !typeOfConstructor.Out(0).AssignableTo(driverType) {
		return fmt.Errorf("return 0 must be of kind %s", driverType.String())
	}

	c.constructors[name] = constructor

	return nil
}

func (c *Registry) extractConfigType(constructor any) any {
	return reflect.New(reflect.TypeOf(constructor).In(0)).Interface()
}

func (c *Registry) Create(ctx context.Context, id string) (Driver, json.RawMessage, error) {
	exporter, err := c.store.GetExporter(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, postgres.ErrNotFound):
			return nil, nil, NewErrExporterNotFound(id)
		default:
			return nil, nil, err
		}
	}

	driverConstructor, ok := c.constructors[exporter.Driver]
	if !ok {
		return nil, nil, fmt.Errorf("cannot build exporter '%s', not exists", id)
	}
	driverConfig := c.extractConfigType(driverConstructor)

	if err := json.Unmarshal(exporter.Config, driverConfig); err != nil {
		return nil, nil, err
	}

	if v, ok := driverConfig.(config.Defaulter); ok {
		v.SetDefaults()
	}

	ret := reflect.ValueOf(driverConstructor).Call([]reflect.Value{
		reflect.ValueOf(driverConfig).Elem(),
		reflect.ValueOf(c.logger),
	})
	if !ret[1].IsZero() {
		return nil, nil, ret[1].Interface().(error)
	}

	return ret[0].Interface().(Driver), exporter.Config, nil
}

func (c *Registry) CreateFromConfig(driverName string, rawConfig json.RawMessage) (Driver, error) {
	if err := c.ValidateConfig(driverName, rawConfig); err != nil {
		return nil, err
	}
	driverConstructor, ok := c.constructors[driverName]
	if !ok {
		return nil, NewErrDriverNotFound(driverName)
	}
	driverConfig := c.extractConfigType(driverConstructor)

	if err := json.Unmarshal(rawConfig, driverConfig); err != nil {
		return nil, err
	}

	if v, ok := driverConfig.(config.Defaulter); ok {
		v.SetDefaults()
	}

	ret := reflect.ValueOf(driverConstructor).Call([]reflect.Value{
		reflect.ValueOf(driverConfig).Elem(),
		reflect.ValueOf(c.logger),
	})
	if !ret[1].IsZero() {
		return nil, ret[1].Interface().(error)
	}

	return ret[0].Interface().(Driver), nil
}

func (c *Registry) GetConfigType(driverName string) (any, error) {
	driverConstructor, ok := c.constructors[driverName]
	if !ok {
		return nil, NewErrDriverNotFound(driverName)
	}
	return c.extractConfigType(driverConstructor), nil
}

func (c *Registry) ValidateConfig(driverName string, rawDriverConfig json.RawMessage) error {
	driverConfig, err := c.GetConfigType(driverName)
	if err != nil {
		return errors.Wrapf(err, "validating config for exporter '%s'", driverName)
	}

	if err := json.Unmarshal(rawDriverConfig, driverConfig); err != nil {
		return NewErrMalformedConfiguration(driverName, err)
	}
	if v, ok := driverConfig.(config.Defaulter); ok {
		v.SetDefaults()
	}
	if v, ok := driverConfig.(config.Validator); ok {
		if err := v.Validate(); err != nil {
			return NewErrInvalidConfiguration(driverName, err)
		}
	}

	type batchingHolder struct {
		Batching Batching `json:"batching"`
	}

	bh := batchingHolder{}
	if err := json.Unmarshal(rawDriverConfig, &bh); err != nil {
		return NewErrMalformedConfiguration(driverName, err)
	}

	bh.Batching.SetDefaults()

	if err := bh.Batching.Validate(); err != nil {
		return NewErrInvalidConfiguration(driverName, err)
	}

	return nil
}

func NewRegistry(logger logging.Logger, store Store) *Registry {
	return &Registry{
		constructors: map[string]any{},
		logger:       logger,
		store:        store,
	}
}

var _ Factory = (*Registry)(nil)
