package http

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
)

// ctxResponseStatusCodeKey is a context key for the http status code in the message context
type ctxResponseStatusCodeKey struct{}

// StatusCodeFromContext returns the status code from the context.
func StatusCodeFromContext(ctx context.Context, otherwise int) int {
	if v := ctx.Value(ctxResponseStatusCodeKey{}); v != nil {
		if code, ok := v.(int); ok {
			return code
		}
	}
	return otherwise
}

// WithResponseStatusCode returns a new context with the status code.
func WithResponseStatusCode(ctx context.Context, code int) context.Context {
	return context.WithValue(ctx, ctxResponseStatusCodeKey{}, code)
}

// SetResponseStatusCode sets a http status code to the given message.
func SetResponseStatusCode(m *message.Message, code int) *message.Message {
	m.SetContext(WithResponseStatusCode(m.Context(), code))
	return m
}
