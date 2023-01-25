package spec

const (
	PayoutLabel = "payout"
)

var (
	PayoutSpecificationSchema = NewObjectSchema().
					WithProperty("source", NewPolymorphicObjectSchema("kind").
						WithOption("wallet", NewObjectSchema().
							WithProperty("wallet", NewStringSchema().WithRequired()).
							WithProperty("balance", NewStringSchema().WithRequired().WithDefault("main")),
			),
		).
		WithProperty("amount", AmountSchema)
	Payout = NewSpecification(PayoutSpecificationSchema)
)

func init() {
	RegisterSpecification(PayoutLabel, Payout)
}
