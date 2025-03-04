package utils

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func GetImage(tag pulumix.Input[string]) pulumi.StringOutput {
	return pulumi.Sprintf("ghcr.io/formancehq/ledger:%s", tag)
}

func BoolToString(output pulumix.Input[bool]) pulumix.Output[string] {
	return pulumix.Apply(output, func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	})
}
