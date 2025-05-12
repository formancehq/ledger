package drivers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/internal/replication/config"
	"github.com/formancehq/ledger/internal/storage/common"
	"reflect"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/pkg/errors"
)

// Registry holds all available drivers
// It implements Factory
type Registry struct {
	constructors map[string]any
	logger       logging.Logger
	store        Store
}

func (c *Registry) RegisterConnector(name string, constructor any) {
	if err := c.registerConnector(name, constructor); err != nil {
		panic(err)
	}
}

func (c *Registry) registerConnector(name string, constructor any) error {
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

	connectorType := reflect.TypeOf(new(Driver)).Elem()
	if !typeOfConstructor.Out(0).AssignableTo(connectorType) {
		return fmt.Errorf("return 0 must be of kind %s", connectorType.String())
	}

	c.constructors[name] = constructor

	return nil
}

func (c *Registry) extractConfigType(constructor any) any {
	return reflect.New(reflect.TypeOf(constructor).In(0)).Interface()
}

func (c *Registry) Create(ctx context.Context, id string) (Driver, json.RawMessage, error) {
	connector, err := c.store.GetConnector(ctx, id)
	if err != nil {
		switch {
		case errors.Is(err, common.ErrNotFound):
			return nil, nil, NewErrConnectorNotFound(id)
		default:
			return nil, nil, err
		}
	}

	driverConstructor, ok := c.constructors[connector.Driver]
	if !ok {
		return nil, nil, fmt.Errorf("cannot build connector '%s', not exists", id)
	}
	driverConfig := c.extractConfigType(driverConstructor)

	if err := json.Unmarshal(connector.Config, driverConfig); err != nil {
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

	return ret[0].Interface().(Driver), connector.Config, nil
}

func (c *Registry) GetConfigType(connectorName string) (any, error) {
	connectorConstructor, ok := c.constructors[connectorName]
	if !ok {
		return nil, NewErrDriverNotFound(connectorName)
	}
	return c.extractConfigType(connectorConstructor), nil
}

func (c *Registry) ValidateConfig(connectorName string, rawConnectorConfig json.RawMessage) error {

	connectorConfig, err := c.GetConfigType(connectorName)
	if err != nil {
		return errors.Wrapf(err, "validating config for connector '%s'", connectorName)
	}

	if err := json.Unmarshal(rawConnectorConfig, connectorConfig); err != nil {
		return NewErrMalformedConfiguration(connectorName, err)
	}
	if v, ok := connectorConfig.(config.Defaulter); ok {
		v.SetDefaults()
	}
	if v, ok := connectorConfig.(config.Validator); ok {
		if err := v.Validate(); err != nil {
			return NewErrInvalidConfiguration(connectorName, err)
		}
	}

	type batchingHolder struct {
		Batching Batching `json:"batching"`
	}

	bh := batchingHolder{}
	if err := json.Unmarshal(rawConnectorConfig, &bh); err != nil {
		return NewErrMalformedConfiguration(connectorName, err)
	}

	bh.Batching.SetDefaults()

	if err := bh.Batching.Validate(); err != nil {
		return NewErrInvalidConfiguration(connectorName, err)
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
