package webhooks

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	cfg := ConfigUser{
		Endpoint:   "https://example.com",
		EventTypes: []string{"TYPE1", "TYPE2"},
	}
	assert.NoError(t, cfg.Validate())

	cfg = ConfigUser{
		Endpoint:   "https://example.com",
		Secret:     NewSecret(),
		EventTypes: []string{"TYPE1", "TYPE2"},
	}
	assert.NoError(t, cfg.Validate())

	cfg = ConfigUser{
		Endpoint:   " http://invalid",
		EventTypes: []string{"TYPE1", "TYPE2"},
	}
	assert.Error(t, cfg.Validate())

	cfg = ConfigUser{
		Endpoint:   "https://example.com",
		EventTypes: []string{"TYPE1", ""},
	}
	assert.Error(t, cfg.Validate())

	cfg = ConfigUser{
		Endpoint:   "https://example.com",
		Secret:     base64.StdEncoding.EncodeToString([]byte(`invalid`)),
		EventTypes: []string{"TYPE1", "TYPE2"},
	}
	assert.Error(t, cfg.Validate())
}
