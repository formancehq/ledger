package pulumiutil

import (
	"github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func newK8sProviderInternal(ctx *pulumi.Context, kubeContext string) (*kubernetes.Provider, error) {
	return kubernetes.NewProvider(ctx, "k8s", &kubernetes.ProviderArgs{
		Context: pulumi.StringPtr(kubeContext),
	})
}
