package pulumi_ledger

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func image(tag pulumix.Input[string]) pulumi.StringOutput {
	return pulumi.Sprintf("ghcr.io/formancehq/ledger:%s", tag)
}
