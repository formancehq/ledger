package clickhouse

import (
	"fmt"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type externalComponent struct {
	pulumi.ResourceState

	DSN pulumix.Output[string]
}

func (e *externalComponent) GetDSN() pulumix.Output[string] {
	return e.DSN
}

type externalComponentArgs struct {
	DSN pulumi.String
}

func newExternalComponent(ctx *pulumi.Context, name string, args externalComponentArgs, opts ...pulumi.ResourceOption) (*externalComponent, error) {
	cmp := &externalComponent{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Clickhouse:External", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	cmp.DSN = args.DSN.ToOutput(ctx.Context())

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}

var _ dsnProvider = (*externalComponent)(nil)
