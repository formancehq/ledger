package utils

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func GetMainImage(tag pulumix.Input[string]) pulumi.StringOutput {
	return GetImage(pulumi.String("ledger"), tag)
}

func GetImage(component, tag pulumix.Input[string]) pulumi.StringOutput {
	return pulumi.Sprintf("ghcr.io/formancehq/%s:%s", component, pulumix.Apply(tag, func(version string) string {
		if version == "" {
			return "latest"
		}
		return version
	}))
}

func BoolToString(output pulumix.Input[bool]) pulumix.Output[string] {
	return pulumix.Apply(output, func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	})
}
