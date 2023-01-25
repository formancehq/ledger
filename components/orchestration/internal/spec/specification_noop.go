package spec

const (
	NoopSpecificationLabel = "noop"
)

var (
	NoopSpecificationSchema = NewObjectSchema()
	NoopSpecification       = NewSpecification(NoopSpecificationSchema)
)

func init() {
	RegisterSpecification(NoopSpecificationLabel, NoopSpecification)
}
