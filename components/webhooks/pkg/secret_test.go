package webhooks

import (
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSecret_Validate(t *testing.T) {
	sec := Secret{Secret: NewSecret()}
	assert.NoError(t, sec.Validate())

	sec = Secret{}
	assert.NoError(t, sec.Validate())

	sec = Secret{Secret: "invalid"}
	assert.Error(t, sec.Validate())

	sec = Secret{Secret: base64.StdEncoding.EncodeToString([]byte(`invalid`))}
	assert.Error(t, sec.Validate())

	token := make([]byte, 23)
	_, err := rand.Read(token)
	require.NoError(t, err)
	tooShort := base64.StdEncoding.EncodeToString(token)
	sec = Secret{Secret: tooShort}
	assert.Error(t, sec.Validate())

	token = make([]byte, 25)
	_, err = rand.Read(token)
	require.NoError(t, err)
	tooLong := base64.StdEncoding.EncodeToString(token)
	sec = Secret{Secret: tooLong}
	assert.Error(t, sec.Validate())
}
