package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/formancehq/ledger-v3-poc/pkg/client"
)

var (
	serverURL string
	debugMode bool
)

// debugHTTPClient wraps an HTTP client to log requests and responses when debug mode is enabled
type debugHTTPClient struct {
	client http.Client
	debug  bool
}

func (c *debugHTTPClient) Do(req *http.Request) (*http.Response, error) {
	if c.debug {
		// Log request
		fmt.Fprintf(os.Stderr, "\n=== HTTP Request ===\n")
		fmt.Fprintf(os.Stderr, "%s %s\n", req.Method, req.URL.String())

		// Log headers
		fmt.Fprintf(os.Stderr, "Headers:\n")
		for key, values := range req.Header {
			for _, value := range values {
				fmt.Fprintf(os.Stderr, "  %s: %s\n", key, value)
			}
		}

		// Log body if present
		if req.Body != nil {
			bodyBytes, err := io.ReadAll(req.Body)
			if err == nil {
				req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				if len(bodyBytes) > 0 {
					fmt.Fprintf(os.Stderr, "Body:\n")
					var prettyJSON bytes.Buffer
					if err := json.Indent(&prettyJSON, bodyBytes, "", "  "); err == nil {
						fmt.Fprintf(os.Stderr, "%s\n", prettyJSON.String())
					} else {
						fmt.Fprintf(os.Stderr, "%s\n", string(bodyBytes))
					}
				}
			}
		}
		fmt.Fprintf(os.Stderr, "===================\n\n")
	}

	// Execute request
	resp, err := c.client.Do(req)
	if err != nil {
		if c.debug {
			fmt.Fprintf(os.Stderr, "=== HTTP Error ===\n")
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			fmt.Fprintf(os.Stderr, "==================\n\n")
		}
		return nil, err
	}

	if c.debug {
		// Log response
		fmt.Fprintf(os.Stderr, "=== HTTP Response ===\n")
		fmt.Fprintf(os.Stderr, "Status: %s\n", resp.Status)

		// Log headers
		fmt.Fprintf(os.Stderr, "Headers:\n")
		for key, values := range resp.Header {
			for _, value := range values {
				fmt.Fprintf(os.Stderr, "  %s: %s\n", key, value)
			}
		}

		// Log body
		bodyBytes, err := io.ReadAll(resp.Body)
		if err == nil {
			resp.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			if len(bodyBytes) > 0 {
				fmt.Fprintf(os.Stderr, "Body:\n")
				var prettyJSON bytes.Buffer
				if err := json.Indent(&prettyJSON, bodyBytes, "", "  "); err == nil {
					fmt.Fprintf(os.Stderr, "%s\n", prettyJSON.String())
				} else {
					fmt.Fprintf(os.Stderr, "%s\n", string(bodyBytes))
				}
			}
		}
		fmt.Fprintf(os.Stderr, "====================\n\n")
	}

	return resp, nil
}

// newSDKClient creates a new SDK client with optional debug HTTP client
func newSDKClient() *client.Formance {
	opts := []client.SDKOption{
		client.WithServerURL(serverURL),
	}

	if debugMode {
		debugClient := &debugHTTPClient{
			client: http.Client{Timeout: 60 * time.Second},
			debug:  true,
		}
		opts = append(opts, client.WithClient(debugClient))
	}

	return client.New(opts...)
}

