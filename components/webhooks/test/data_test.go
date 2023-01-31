package test_test

import (
	"time"

	webhooks "github.com/formancehq/webhooks/pkg"
)

var (
	app1   = "app1"
	type1  = "type1"
	event1 = webhooks.EventMessage{
		Date:    time.Now().UTC(),
		App:     app1,
		Version: "v1.8.0",
		Type:    type1,
		Payload: map[string]any{
			"key1": "value1",
		},
	}

	app2   = "app2"
	type2  = "type2"
	event2 = webhooks.EventMessage{
		Date:    time.Now().UTC(),
		App:     app2,
		Version: "v0.3.1",
		Type:    type2,
		Payload: map[string]any{
			"key2": "value2",
		},
	}

	app3   = "app3"
	type3  = "type3"
	event3 = webhooks.EventMessage{
		Date:    time.Now().UTC(),
		App:     app3,
		Version: "v0.3.2",
		Type:    type3,
		Payload: map[string]any{
			"key3": "value3",
		},
	}
)
