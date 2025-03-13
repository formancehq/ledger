package provision

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/api"
	"github.com/formancehq/ledger/deployments/pulumi/pkg/utils"
	batchv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/batch/v1"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	rbacv1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/rbac/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumix"
)

type Component struct {
	pulumi.ResourceState
}

type LedgerConfigArgs struct {
	Bucket   string            `json:"bucket"`
	Metadata map[string]string `json:"metadata"`
	Features map[string]string `json:"features"`
}

type ConfigArgs struct {
	Ledgers map[string]LedgerConfigArgs `json:"ledgers"`
}

type Args struct {
	ProvisionerVersion pulumi.String
	Config             ConfigArgs
}

type ComponentArgs struct {
	utils.CommonArgs
	API *api.Component
	Args
}

func NewComponent(ctx *pulumi.Context, name string, args ComponentArgs, opts ...pulumi.ResourceOption) (*Component, error) {
	cmp := &Component{}
	err := ctx.RegisterComponentResource("Formance:Ledger:Provisioner", name, cmp, opts...)
	if err != nil {
		return nil, err
	}

	marshalledConfig, err := json.Marshal(args.Config)
	if err != nil {
		return nil, err
	}

	digest := sha256.New()
	_, err = digest.Write(marshalledConfig)
	if err != nil {
		return nil, err
	}

	configMap, err := corev1.NewConfigMap(ctx, "provisioner", &corev1.ConfigMapArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		Data: pulumi.StringMap{
			"config.yaml": pulumi.String(marshalledConfig),
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	serviceAccount, err := corev1.NewServiceAccount(ctx, "provisioner", &corev1.ServiceAccountArgs{
		AutomountServiceAccountToken: pulumi.BoolPtr(true),
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	role, err := rbacv1.NewRole(ctx, "provisioner", &rbacv1.RoleArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		Rules: rbacv1.PolicyRuleArray{
			rbacv1.PolicyRuleArgs{
				ApiGroups: pulumi.StringArray{
					pulumi.String(""), // core
				},
				Resources: pulumi.StringArray{
					pulumi.String("configmaps"),
				},
				Verbs: pulumi.StringArray{
					pulumi.String("*"),
				},
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	_, err = rbacv1.NewRoleBinding(ctx, "provisioner", &rbacv1.RoleBindingArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
		},
		RoleRef: rbacv1.RoleRefArgs{
			Kind:     pulumi.String("Role"),
			Name:     role.Metadata.Name().Elem(),
			ApiGroup: pulumi.String("rbac.authorization.k8s.io"),
		},
		Subjects: rbacv1.SubjectArray{
			rbacv1.SubjectArgs{
				Kind:      pulumi.String("ServiceAccount"),
				Name:      serviceAccount.Metadata.Name().Elem(),
				Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			},
		},
	}, pulumi.Parent(cmp))
	if err != nil {
		return nil, err
	}

	_, err = batchv1.NewJob(ctx, "provisioner", &batchv1.JobArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Annotations: pulumi.StringMap{
				"config-hash": pulumi.String(fmt.Sprintf("%x", digest.Sum(nil))),
			},
		},
		Spec: batchv1.JobSpecArgs{
			Template: corev1.PodTemplateSpecArgs{
				Spec: corev1.PodSpecArgs{
					ServiceAccount: serviceAccount.Metadata.Name(),
					RestartPolicy:  pulumi.String("Never"),
					Containers: corev1.ContainerArray{
						corev1.ContainerArgs{
							Name: pulumi.String("provisioner"),
							Args: pulumi.StringArray{
								pulumi.String("--config"),
								pulumi.String("/config.yaml"),
								pulumi.String("--ledger-url"),
								pulumi.Sprintf(
									"http://%s:%d",
									args.API.Service.Metadata.Name().Elem(),
									args.API.Service.Spec.Ports().Index(pulumi.Int(0)).Port(),
								),
								pulumi.String("--state-store"),
								pulumi.Sprintf("k8s:///%s/provisioner", args.Namespace),
							},
							Image: utils.GetImage(pulumi.String("ledger-provisioner"), pulumix.Apply2(args.Tag, args.ProvisionerVersion, func(ledgerVersion, provisionerVersion string) string {
								if provisionerVersion != "" {
									return provisionerVersion
								}
								return ledgerVersion
							})),
							VolumeMounts: corev1.VolumeMountArray{
								corev1.VolumeMountArgs{
									Name:      pulumi.String("config"),
									MountPath: pulumi.String("/config.yaml"),
									SubPath:   pulumi.String("config.yaml"),
								},
							},
						},
					},
					Volumes: corev1.VolumeArray{
						corev1.VolumeArgs{
							Name: pulumi.String("config"),
							ConfigMap: &corev1.ConfigMapVolumeSourceArgs{
								Name: configMap.Metadata.Name(),
							},
						},
					},
				},
			},
		},
	}, pulumi.Parent(cmp), pulumi.DeleteBeforeReplace(true))
	if err != nil {
		return nil, err
	}

	if err := ctx.RegisterResourceOutputs(cmp, pulumi.Map{}); err != nil {
		return nil, fmt.Errorf("registering outputs: %w", err)
	}

	return cmp, nil
}
