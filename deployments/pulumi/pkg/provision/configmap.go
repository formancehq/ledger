package provision

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func createConfigMap(ctx *pulumi.Context, cmp *Component, args ComponentArgs) (*corev1.ConfigMap, error) {
	marshalledConfig, err := json.Marshal(struct {
		Ledgers    map[string]LedgerConfigArgs `json:"ledgers"`
	}{
		Ledgers:    args.Ledgers,
	})
	if err != nil {
		return nil, err
	}

	digest := sha256.New()
	_, err = digest.Write(marshalledConfig)
	if err != nil {
		return nil, err
	}

	return corev1.NewConfigMap(ctx, "provisioner", &corev1.ConfigMapArgs{
		Metadata: metav1.ObjectMetaArgs{
			Namespace: args.Namespace.ToOutput(ctx.Context()).Untyped().(pulumi.StringOutput),
			Annotations: pulumi.StringMap{
				"config-hash": pulumi.String(fmt.Sprintf("%x", digest.Sum(nil))),
			},
		},
		Data: pulumi.StringMap{
			"config.yaml": pulumi.String(marshalledConfig),
		},
	}, pulumi.Parent(cmp))
}
