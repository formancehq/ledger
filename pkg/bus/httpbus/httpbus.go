package httpbus

import (
	"github.com/ThreeDotsLabs/watermill"
	wHttp "github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.uber.org/fx"
	"net/http"
)

func NewPublisher(logger watermill.LoggerAdapter, httpClient *http.Client) (*wHttp.Publisher, error) {
	return wHttp.NewPublisher(wHttp.PublisherConfig{
		MarshalMessageFunc: func(url string, msg *message.Message) (*http.Request, error) {
			req, err := wHttp.DefaultMarshalMessageFunc(url, msg)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			return req, nil
		},
		Client: httpClient,
	}, logger)
}

func Module() fx.Option {
	return fx.Options(
		fx.Provide(NewPublisher),
		fx.Supply(http.DefaultClient),
		fx.Decorate(
			func(p *wHttp.Publisher) message.Publisher {
				return p
			},
		),
	)
}
