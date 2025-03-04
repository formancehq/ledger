package utils

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type CommonArgs struct {
	Namespace       pulumix.Input[string]
	Otel            *OtelArgs
	Tag             pulumix.Input[string]
	ImagePullPolicy pulumix.Input[string]
	Debug           pulumix.Input[bool]
}

func (args *CommonArgs) SetDefaults() {
	if args.Namespace == nil {
		args.Namespace = pulumi.String("")
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
	if args.ImagePullPolicy == nil {
		args.ImagePullPolicy = pulumi.String("")
	}
	if args.Debug == nil {
		args.Debug = pulumi.Bool(false)
	}
}
