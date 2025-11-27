package utils

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func GetMainImage(imageConfiguration ImageConfiguration) pulumi.StringOutput {
	return GetImage(imageConfiguration, pulumi.String("ledger"))
}

func GetImage(imageConfiguration ImageConfiguration, component pulumix.Input[string]) pulumi.StringOutput {
	return pulumi.Sprintf(
		"%s/%s/%s:%s",
		pulumix.Apply(imageConfiguration.Registry, func(registry string) string {
			if registry == "" {
				return "ghcr.io"
			}
			return registry
		}),
		pulumix.Apply(imageConfiguration.Repository, func(repository string) string {
			if repository == "" {
				return "formancehq"
			}
			return repository
		}),
		component,
		pulumix.Apply(imageConfiguration.Tag, func(version string) string {
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

type ImageConfiguration struct {
	Registry   pulumix.Input[string]
	Repository pulumix.Input[string]
	Tag        pulumix.Input[string]
}

func (args ImageConfiguration) WithFallbackTag(input pulumix.Input[string]) ImageConfiguration {
	args.Tag = pulumix.Apply2(args.Tag, input, func(providedVersion, fallbackVersion string) string {
		if providedVersion == "" {
			return fallbackVersion
		}
		return providedVersion
	})
	return args
}

func (args *ImageConfiguration) SetDefaults() {
	if args.Registry == nil {
		args.Registry = pulumi.String("ghcr.io")
	} else {
		args.Registry = pulumix.Apply(args.Registry, func(registry string) string {
			if registry == "" {
				return "ghcr.io"
			}
			return registry
		})
	}

	if args.Repository == nil {
		args.Repository = pulumi.String("formancehq")
	} else {
		args.Repository = pulumix.Apply(args.Repository, func(repository string) string {
			if repository == "" {
				return "formancehq"
			}
			return repository
		})
	}

	if args.Tag == nil {
		args.Tag = pulumi.String("latest")
	} else {
		args.Tag = pulumix.Apply(args.Tag, func(tag string) string {
			if tag == "" {
				return "latest"
			}
			return tag
		})
	}
}