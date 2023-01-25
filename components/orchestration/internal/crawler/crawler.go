package crawler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/formancehq/orchestration/internal/spec"
)

type Crawler struct {
	schema  spec.Schema
	input   any
	context Context
}

func (c Crawler) interpolate(v string) string {
	r := regexp.MustCompile(`\$\{.+}`)
	return r.ReplaceAllStringFunc(v, func(key string) string {
		key = strings.TrimPrefix(key, "${")
		key = strings.TrimSuffix(key, "}")
		return c.context.Variables[key]
	})
}

func (c Crawler) GetProperty(key string) Crawler {
	dataAsMap, ok := c.input.(map[string]any)
	if !ok {
		panic(fmt.Errorf("error converting key %s to map", key))
	}

	schemaAsObjectSchema, ok := c.schema.(spec.ObjectSchema)
	if !ok {
		panic(fmt.Errorf("schema is not of type object"))
	}

	return New(schemaAsObjectSchema.Properties[key], dataAsMap[key], c.context)
}

func (c Crawler) AsDiscriminated() DiscriminatedCrawler {
	dataAsMap, ok := c.input.(map[string]any)
	if !ok {
		panic("data is not a map")
	}

	objectSchema, ok := c.schema.(spec.PolymorphicObjectSchema)
	if !ok {
		panic(fmt.Errorf("schema is not of type polymorphic object"))
	}

	discriminator := dataAsMap[objectSchema.Discriminator].(string)
	schema := objectSchema.Options[discriminator]

	newMap := make(map[string]any)
	for key, value := range dataAsMap {
		newMap[key] = value
	}

	return newDiscriminatedCrawler(New(schema, newMap, c.context), discriminator)
}

func (c Crawler) AsString() string {
	if c.input == nil {
		return *c.schema.(spec.ScalarSchema[string]).DefaultValue
	}
	return c.interpolate(c.input.(string))
}

func (c Crawler) AsUInt64() uint64 {
	switch v := c.input.(type) {
	case float64:
		return uint64(c.input.(float64))
	case string:
		interpolated := c.interpolate(v)
		asUInt64, err := strconv.ParseUint(interpolated, 10, 64)
		if err != nil {
			panic(err)
		}

		return asUInt64
	default:
		panic("unsupported type")
	}
}

func New(schema spec.Schema, input any, ctx Context) Crawler {
	return Crawler{
		schema:  schema,
		input:   input,
		context: ctx,
	}
}

type DiscriminatedCrawler struct {
	Crawler
	cType string
}

func (d DiscriminatedCrawler) GetType() string {
	return d.cType
}

func newDiscriminatedCrawler(c Crawler, cType string) DiscriminatedCrawler {
	return DiscriminatedCrawler{
		Crawler: c,
		cType:   cType,
	}
}
