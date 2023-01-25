package v1beta2

import (
	"context"

	"github.com/formancehq/operator/apis/stack/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type ConfigurationInterface interface {
	List(ctx context.Context, opts metav1.ListOptions) (*v1beta2.ConfigurationList, error)
	Get(ctx context.Context, name string, options metav1.GetOptions) (*v1beta2.Configuration, error)
	Create(ctx context.Context, configuration *v1beta2.Configuration) (*v1beta2.Configuration, error)
	Update(ctx context.Context, configuration *v1beta2.Configuration) (*v1beta2.Configuration, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Delete(ctx context.Context, name string) error
}

type configurationClient struct {
	restClient rest.Interface
}

func (c *configurationClient) List(ctx context.Context, opts metav1.ListOptions) (*v1beta2.ConfigurationList, error) {
	result := v1beta2.ConfigurationList{}
	err := c.restClient.
		Get().
		Resource("configurations").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(ctx).
		Into(&result)

	return &result, err
}

func (c *configurationClient) Get(ctx context.Context, name string, opts metav1.GetOptions) (*v1beta2.Configuration, error) {
	result := v1beta2.Configuration{}
	err := c.restClient.
		Get().
		Resource("configurations").
		Name(name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(ctx).
		Into(&result)

	return &result, err
}

func (c *configurationClient) Create(ctx context.Context, version *v1beta2.Configuration) (*v1beta2.Configuration, error) {
	result := v1beta2.Configuration{}
	err := c.restClient.
		Post().
		Resource("configurations").
		Body(version).
		Do(ctx).
		Into(&result)

	return &result, err
}

func (c *configurationClient) Delete(ctx context.Context, name string) error {
	return c.restClient.
		Delete().
		Resource("configurations").
		Name(name).
		Do(ctx).
		Error()
}

func (c *configurationClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.restClient.
		Get().
		Resource("configurations").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(ctx)
}

func (c *configurationClient) Update(ctx context.Context, o *v1beta2.Configuration) (*v1beta2.Configuration, error) {
	result := v1beta2.Configuration{}
	err := c.restClient.
		Put().
		Resource("configurations").
		Name(o.Name).
		Body(o).
		Do(ctx).
		Into(&result)

	return &result, err
}
