package oauth2introspect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pkg/errors"
)

type IntrospecterOption interface {
	apply(introspecter *Introspecter)
}
type IntrospecterOptionFn func(introspecter *Introspecter)

func (fn IntrospecterOptionFn) apply(introspecter *Introspecter) {
	fn(introspecter)
}

func WithClient(client *http.Client) IntrospecterOptionFn {
	return func(introspecter *Introspecter) {
		introspecter.client = client
	}
}

func WithCache(cache *ristretto.Cache, cacheTTL time.Duration) IntrospecterOptionFn {
	return func(introspecter *Introspecter) {
		introspecter.cache = cache
		introspecter.cacheTTL = cacheTTL
	}
}

type Introspecter struct {
	introspectUrl string
	client        *http.Client
	cache         *ristretto.Cache
	cacheTTL      time.Duration
}

func (i *Introspecter) Introspect(ctx context.Context, bearer string) (bool, error) {

	if i.cache != nil {
		v, ok := i.cache.Get(bearer)
		if ok {
			return v.(bool), nil
		}
	}

	form := url.Values{}
	form.Set("token", bearer)

	checkAuthReq, err := http.NewRequest(http.MethodPost, i.introspectUrl, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return false, errors.Wrap(err, "creating introspection request")
	}
	checkAuthReq.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	checkAuthReq = checkAuthReq.WithContext(ctx)

	rsp, err := i.client.Do(checkAuthReq)
	if err != nil {
		return false, errors.Wrap(err, "making introspection request")
	}

	switch rsp.StatusCode {
	case http.StatusOK:
		type X struct {
			Active bool `json:"active"`
		}
		x := X{}
		err = json.NewDecoder(rsp.Body).Decode(&x)
		if err != nil {
			return false, errors.Wrap(err, "decoding introspection response")
		}

		if i.cache != nil {
			_ = i.cache.SetWithTTL(bearer, x.Active, 1, i.cacheTTL)
		}

		return x.Active, nil
	default:
		return false, fmt.Errorf("unexpected status code %d on introspection request", rsp.StatusCode)
	}
}

func NewIntrospecter(url string, options ...IntrospecterOption) *Introspecter {
	i := &Introspecter{
		introspectUrl: url,
		client:        http.DefaultClient,
	}
	for _, opt := range options {
		opt.apply(i)
	}
	return i
}
