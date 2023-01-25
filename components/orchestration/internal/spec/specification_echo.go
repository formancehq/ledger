package spec

const (
	EchoLabel = "echo"
)

var (
	EchoSpecificationSchema = NewObjectSchema().WithProperty("message", NewStringSchema().WithRequired())
	Echo                    = NewSpecification(EchoSpecificationSchema)
)

func init() {
	RegisterSpecification(EchoLabel, Echo)
}
