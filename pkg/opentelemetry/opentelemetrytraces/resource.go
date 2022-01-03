package opentelemetrytraces

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.uber.org/fx"
)

type resourceFactory struct {
	attributes []attribute.KeyValue
}

func (f *resourceFactory) Make() (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, f.attributes...),
		//resource.NewWithAttributes(
		//	semconv.SchemaURL,
		//	semconv.ServiceNameKey.String(serviceName),
		//	semconv.ServiceVersionKey.String(version),
		//),
	)
}

func NewResourceFactory(attributes ...attribute.KeyValue) *resourceFactory {
	return &resourceFactory{attributes: attributes}
}

const oltpAttribute = `group:"_otlpAttributes"`

func ProvideOTLPAttribute(attr attribute.KeyValue) fx.Option {
	return fx.Provide(fx.Annotate(func() attribute.KeyValue {
		return attr
	}, fx.ResultTags(oltpAttribute)))
}

func ResourceFactoryModule() fx.Option {
	return fx.Options(fx.Provide(
		fx.Annotate(NewResourceFactory, fx.ParamTags(oltpAttribute))),
	)
}
