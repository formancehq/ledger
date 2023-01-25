package spec

const (
	PayinLabel = "Payin"
)

var (
	PayinSpecificationSchema = NewObjectSchema().
					WithProperty("source", NewStringSchema().WithRequired()).
					WithProperty("destination", NewPolymorphicObjectSchema("kind").
						WithOption("wallet", NewObjectSchema().
							WithProperty("wallet", NewStringSchema().WithRequired()).
							WithProperty("balance", NewStringSchema().WithRequired().WithDefault("main")),
			),
		)
	Payin = NewSpecification(PayinSpecificationSchema)
)

func init() {
	RegisterSpecification(PayinLabel, Payin)
}
