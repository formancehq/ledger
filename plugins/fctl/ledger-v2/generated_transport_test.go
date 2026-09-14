package ledgerv2

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

type captureHTTPClient struct {
	request *http.Request
	body    []byte
}

func (c *captureHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.request = request
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		c.body = body
	}
	return &http.Response{StatusCode: 204, Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

func TestGeneratedTransportRemovesOnlyByteExactNullGETBody(t *testing.T) {
	capture := &captureHTTPClient{}
	transport := stripGeneratedNullGETBody{next: capture}
	request, err := http.NewRequest(http.MethodGet, "https://product.invalid/v2?cursor=opaque", bytes.NewBufferString("null"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if _, err := transport.Do(request); err != nil {
		t.Fatal(err)
	}
	if len(capture.body) != 0 || capture.request.Body != http.NoBody || capture.request.ContentLength != 0 || capture.request.Header.Get("Content-Type") != "" {
		t.Fatalf("forwarded request = body %q content-length %d content-type %q", capture.body, capture.request.ContentLength, capture.request.Header.Get("Content-Type"))
	}
}

func TestGeneratedTransportPreservesEveryOtherBody(t *testing.T) {
	for _, body := range []string{`{"filter":"value"}`, `null `, `NULL`} {
		t.Run(body, func(t *testing.T) {
			capture := &captureHTTPClient{}
			transport := stripGeneratedNullGETBody{next: capture}
			request, err := http.NewRequest(http.MethodGet, "https://product.invalid/v2", bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			if _, err := transport.Do(request); err != nil {
				t.Fatal(err)
			}
			if string(capture.body) != body || capture.request.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("forwarded body = %q content-type %q", capture.body, capture.request.Header.Get("Content-Type"))
			}
		})
	}
}

func TestGeneratedTransportMakesNonEmptyCursorQueriesExclusive(t *testing.T) {
	tests := []struct {
		name, rawURL, wantQuery string
	}{
		{name: "generated default removed", rawURL: "https://product.invalid/v2?cursor=opaque&includeDeleted=false&pageSize=15", wantQuery: "cursor=opaque"},
		{name: "cursor canonically encoded", rawURL: "https://product.invalid/v2?cursor=opaque%20%2F%3F%26&sort=id%3Adesc", wantQuery: "cursor=opaque+%2F%3F%26"},
		{name: "empty cursor untouched", rawURL: "https://product.invalid/v2?cursor=&includeDeleted=false", wantQuery: "cursor=&includeDeleted=false"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := &captureHTTPClient{}
			transport := stripGeneratedNullGETBody{next: capture}
			request, err := http.NewRequest(http.MethodGet, test.rawURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := transport.Do(request); err != nil {
				t.Fatal(err)
			}
			if got := capture.request.URL.RawQuery; got != test.wantQuery {
				t.Fatalf("query = %q, want %q", got, test.wantQuery)
			}
		})
	}
}
