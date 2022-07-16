package openapi3

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	. "github.com/onsi/gomega"
)

type transport struct {
	doc        *openapi3.T
	router     routers.Router
	underlying http.RoundTripper
}

type stringerError struct {
	error
}

func (e stringerError) String() string {
	return e.Error()
}

func newStringerError(err error) *stringerError {
	if err == nil {
		return nil
	}
	return &stringerError{err}
}

func (t transport) RoundTrip(request *http.Request) (*http.Response, error) {
	var (
		err         error
		requestData []byte
	)
	if request.Body != nil {
		requestData, err = ioutil.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = ioutil.NopCloser(bytes.NewBuffer(requestData))
	}

	response, err := t.underlying.RoundTrip(request)
	if err != nil {
		return nil, err
	}

	route, pathParams, err := t.router.FindRoute(request)
	if err != nil {
		return nil, err
	}

	if request.Body != nil {
		request.Body = ioutil.NopCloser(bytes.NewBuffer(requestData))
	}

	options := &openapi3filter.Options{
		IncludeResponseStatus: true,
		MultiError:            true,
		AuthenticationFunc:    openapi3filter.NoopAuthenticationFunc,
	}
	input := &openapi3filter.RequestValidationInput{
		Request:     request,
		PathParams:  pathParams,
		QueryParams: request.URL.Query(),
		Route:       route,
		Options:     options,
	}

	Expect(newStringerError(
		openapi3filter.ValidateRequest(context.Background(), input),
	)).WithOffset(8).To(BeNil())

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	response.Body = ioutil.NopCloser(bytes.NewBuffer(responseBody))

	err = openapi3filter.ValidateResponse(context.Background(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: input,
		Status:                 response.StatusCode,
		Header:                 response.Header,
		Body:                   ioutil.NopCloser(bytes.NewBuffer(responseBody)),
		Options:                options,
	})
	Expect(newStringerError(err)).WithOffset(8).To(BeNil())

	return response, nil
}

var _ http.RoundTripper = &transport{}

func NewTransport(ledgerUrl string) http.RoundTripper {
	data, err := os.ReadFile(filepath.Join("..", "..", "pkg", "api", "controllers", "swagger.yaml"))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	loader := &openapi3.Loader{Context: ctx, IsExternalRefsAllowed: true}
	doc, err := loader.LoadFromData(data)
	if err != nil {
		panic(err)
	}
	doc.Servers[0] = &openapi3.Server{
		URL: ledgerUrl,
	}

	err = doc.Validate(ctx)
	if err != nil {
		panic(err)
	}

	router, err := gorillamux.NewRouter(doc)
	if err != nil {
		panic(err)
	}

	return &transport{
		doc:        doc,
		router:     router,
		underlying: http.DefaultTransport,
	}
}
