package v1beta2

import (
	"context"

	"github.com/formancehq/operator/apis/stack/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type VersionsInterface interface {
	List(ctx context.Context, opts metav1.ListOptions) (*v1beta2.VersionsList, error)
	Get(ctx context.Context, name string, options metav1.GetOptions) (*v1beta2.Versions, error)
	Create(ctx context.Context, versions *v1beta2.Versions) (*v1beta2.Versions, error)
	Update(ctx context.Context, versions *v1beta2.Versions) (*v1beta2.Versions, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Delete(ctx context.Context, name string) error
}

type versionClient struct {
	restClient rest.Interface
}

func (c *versionClient) List(ctx context.Context, opts metav1.ListOptions) (*v1beta2.VersionsList, error) {
	result := v1beta2.VersionsList{}
	err := c.restClient.
		Get().
		Resource("versions").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(ctx).
		Into(&result)

	return &result, err
}

func (c *versionClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1beta2.Versions, error) {
	result := v1beta2.Versions{}
	err := c.restClient.
		Get().
		Resource("versions").
		Name(name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(ctx).
		Into(&result)

	return &result, err
}

func (c *versionClient) Create(ctx context.Context, version *v1beta2.Versions) (*v1beta2.Versions, error) {
	result := v1beta2.Versions{}
	err := c.restClient.
		Post().
		Resource("versions").
		Body(version).
		Do(ctx).
		Into(&result)

	return &result, err
}

func (c *versionClient) Delete(ctx context.Context, name string) error {
	return c.restClient.
		Delete().
		Resource("versions").
		Name(name).
		Do(ctx).
		Error()
}

func (c *versionClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.restClient.
		Get().
		Resource("versions").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(ctx)
}

func (c *versionClient) Update(ctx context.Context, o *v1beta2.Versions) (*v1beta2.Versions, error) {
	result := v1beta2.Versions{}
	err := c.restClient.
		Put().
		Resource("versions").
		Name(o.Name).
		Body(o).
		Do(ctx).
		Into(&result)

	return &result, err
}
