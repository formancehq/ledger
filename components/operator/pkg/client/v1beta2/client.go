package v1beta2

import (
	"github.com/formancehq/operator/apis/stack/v1beta2"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

func init() {
	if err := v1beta2.AddToScheme(scheme.Scheme); err != nil {
		panic(err)
	}
}

type Client struct {
	rest.Interface
}

func NewClient(restClient rest.Interface) *Client {
	return &Client{
		Interface: restClient,
	}
}

func (c *Client) Stacks() StackInterface {
	return &stackClient{
		restClient: c.Interface,
	}
}

func (c *Client) Versions() VersionsInterface {
	return &versionClient{
		restClient: c.Interface,
	}
}

func (c *Client) Configurations() ConfigurationInterface {
	return &configurationClient{
		restClient: c.Interface,
	}
}
