package publishhttp

import (
	"net/http"

	"github.com/ThreeDotsLabs/watermill"
	wHttp "github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.uber.org/fx"
)

func NewPublisher(logger watermill.LoggerAdapter, config wHttp.PublisherConfig) (*wHttp.Publisher, error) {
	return wHttp.NewPublisher(config, logger)
}

func NewPublisherConfig(httpClient *http.Client, m wHttp.MarshalMessageFunc) wHttp.PublisherConfig {
	return wHttp.PublisherConfig{
		MarshalMessageFunc: m,
		Client:             httpClient,
	}
}

func DefaultMarshalMessageFunc() wHttp.MarshalMessageFunc {
	return func(url string, msg *message.Message) (*http.Request, error) {
		req, err := wHttp.DefaultMarshalMessageFunc(url, msg)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}
}

func Module() fx.Option {
	return fx.Options(
		fx.Provide(NewPublisher),
		fx.Provide(NewPublisherConfig),
		fx.Provide(DefaultMarshalMessageFunc),
		fx.Supply(http.DefaultClient),
		fx.Decorate(
			func(p *wHttp.Publisher) message.Publisher {
				return p
			},
		),
	)
}
