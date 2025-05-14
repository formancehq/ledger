package utils

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func GetMainImage(registry, tag pulumix.Input[string]) pulumi.StringOutput {
	return GetImage(registry, pulumi.String("ledger"), tag)
}

func GetImage(registry, component, tag pulumix.Input[string]) pulumi.StringOutput {
	return pulumi.Sprintf(
		"%s/formancehq/%s:%s",
		pulumix.Apply(registry, func(r string) string {
			if r == "" {
				return "ghcr.io"
			}
			return r
		}),
		component,
		pulumix.Apply(tag, func(version string) string {
			if version == "" {
				return "latest"
			}
			return version
		}),
	)
}

func BoolToString(output pulumix.Input[bool]) pulumix.Output[string] {
	return pulumix.Apply(output, func(v bool) string {
		if v {
			return "true"
		}
		return "false"
	})
}
