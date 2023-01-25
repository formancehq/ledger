package publishhttp

import (
	"net/http"
	"reflect"
	"testing"

	wHttp "github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/publish"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func MarshallerFunc(url string, msg *message.Message) (*http.Request, error) {
	return nil, nil
}

func CustomMarshaller() wHttp.MarshalMessageFunc {
	return MarshallerFunc
}

func TestModuleHTTP(t *testing.T) {

	app := fxtest.New(t,
		publish.Module(),
		Module(),
		fx.Decorate(CustomMarshaller),
		fx.Invoke(func(p message.Publisher, cfg wHttp.PublisherConfig) {
			if !assert.IsType(t, &wHttp.Publisher{}, p) {
				return
			}
			if !assert.Equal(t, reflect.ValueOf(MarshallerFunc).Pointer(), reflect.ValueOf(cfg.MarshalMessageFunc).Pointer()) {
				return
			}
		}),
	)
	app.
		RequireStart().
		RequireStop()
}
