package spec

var (
	AmountSchema = NewObjectSchema().
		WithProperty("amount", NewUInt64Schema().WithRequired()).
		WithProperty("asset", NewStringSchema().WithRequired())
)

type Schema interface {
	validate(value any) error
	getType() string
}

var baseSchemas = map[string]Schema{}

func registerBaseSchema(schema Schema) {
	baseSchemas[schema.getType()] = schema
}
