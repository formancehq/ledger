package cmd

import (
	"encoding/base64"
	"fmt"
	v1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	v2 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/rbac/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

func hackStack(ctx *pulumi.Context) error {

	stackReference, err := pulumi.NewStackReference(ctx, "ledger-stack", &pulumi.StackReferenceArgs{
		Name: pulumi.String(ctx.Organization() + "/ledger/" + ctx.Stack()),
	})
	if err != nil {
		return fmt.Errorf("failed to get stack reference: %w", err)
	}

	account, err := v1.NewServiceAccount(ctx, "external-access", &v1.ServiceAccountArgs{
		Metadata: metav1.ObjectMetaArgs{
			Name:      pulumi.String("external-access"),
			Namespace: stackReference.GetStringOutput(pulumi.String("namespace")),
		},
	})
	if err != nil {
		return err
	}

	_, err = v2.NewClusterRoleBinding(ctx, "external-access", &v2.ClusterRoleBindingArgs{
		ApiVersion: nil,
		Kind:       nil,
		Metadata: metav1.ObjectMetaArgs{
			Name:      pulumi.String("external-access"),
			Namespace: stackReference.GetStringOutput(pulumi.String("namespace")),
		},
		RoleRef: v2.RoleRefArgs{
			Kind: pulumi.String("ClusterRole"),
			Name: pulumi.String("cluster-admin"),
		},
		Subjects: v2.SubjectArray{
			v2.SubjectArgs{
				Kind:      pulumi.String("ServiceAccount"),
				Name:      account.Metadata.Name().Elem(),
				Namespace: account.Metadata.Namespace().Elem(),
			},
		},
	},
		pulumi.DependsOn([]pulumi.Resource{account}),
	)
	if err != nil {
		return err
	}

	secret, err := v1.NewSecret(ctx, "external-access", &v1.SecretArgs{
		Metadata: metav1.ObjectMetaArgs{
			Name:      pulumi.String("external-access"),
			Namespace: stackReference.GetStringOutput(pulumi.String("namespace")),
			Annotations: pulumi.StringMap{
				"kubernetes.io/service-account.name": account.Metadata.Name().Elem(),
			},
		},
		Type: pulumi.String("kubernetes.io/service-account-token"),
	},
		pulumi.DependsOn([]pulumi.Resource{account}),
	)
	if err != nil {
		return err
	}

	ctx.Export("token", pulumix.ApplyErr(secret.Data.MapIndex(pulumi.String("token")), func(token string) (string, error) {
		ret, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return "", fmt.Errorf("failed to decode token: %w", err)
		}

		return string(ret), nil
	}))

	return nil
}
