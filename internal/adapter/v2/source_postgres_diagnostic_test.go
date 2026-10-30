package v2

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestNewPostgresSource_ParseErrorDoesNotDiscloseCredentials(t *testing.T) {
	t.Parallel()
	const secret = "MIRROR_SECRET_a7b9:/+@"
	encoded := url.QueryEscape(secret)
	for name, dsn := range map[string]string{
		"query":         "postgres://user@localhost/ledger?password=" + secret + "&connect_timeout=invalid",
		"encoded query": "postgres://user@localhost/ledger?password=" + encoded + "&connect_timeout=invalid",
		"userinfo":      "postgres://user:" + url.PathEscape(secret) + "@localhost/ledger?connect_timeout=invalid",
		"keyword":       "host=localhost user=user password='" + secret + "' connect_timeout=invalid",
		"sslpassword":   "postgres://user@localhost/ledger?sslpassword=" + encoded + "&connect_timeout=invalid",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}
			source, err := NewPostgresSource(context.Background(), cfg, "source-ledger")
			require.Nil(t, source)
			require.ErrorContains(t, err, "invalid connect_timeout")
			for current := err; current != nil; current = errors.Unwrap(current) {
				for _, credential := range []string{secret, encoded, url.PathEscape(secret)} {
					require.NotContains(t, fmt.Sprintf("%+v", current), credential)
				}
			}
			require.Nil(t, errors.Unwrap(err), "parser errors must not retain credential-bearing causes")
			require.Equal(t, dsn, cfg.GetDsn())
		})
	}
}

func TestNewPostgresSource_ParseErrorDoesNotDiscloseInvalidValues(t *testing.T) {
	t.Parallel()
	const secret = "INVALID_OPTION_SECRET_71fe:/+@"
	for name, dsn := range map[string]string{
		"timeout value":     "postgres://user@localhost/ledger?connect_timeout=" + url.QueryEscape(secret),
		"pool value":        "postgres://user@localhost/ledger?pool_max_conns=" + url.QueryEscape(secret),
		"TLS value":         "postgres://user@localhost/ledger?sslmode=" + url.QueryEscape(secret),
		"malformed keyword": "host=localhost password='" + secret,
		"malformed URI":     "postgres://user:" + secret + "%zz@localhost/ledger",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}
			source, err := NewPostgresSource(context.Background(), cfg, "source-ledger")
			require.Nil(t, source)
			require.ErrorContains(t, err, "parsing mirror DSN: invalid")
			require.NotContains(t, err.Error(), secret)
			require.NotContains(t, err.Error(), url.QueryEscape(secret))
			require.Nil(t, errors.Unwrap(err))
			require.Equal(t, dsn, cfg.GetDsn())
		})
	}
}

func TestBuildPgxPoolConfig_QueryPasswordPreserved(t *testing.T) {
	t.Parallel()
	const secret = "VALID_PASSWORD_71fe:/+@"
	dsn := "postgres://user@localhost/ledger?password=" + url.QueryEscape(secret) + "&connect_timeout=1&sslmode=disable"
	input := &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}
	cfg, err := buildPgxPoolConfig(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, secret, cfg.ConnConfig.Password)
	require.Equal(t, dsn, input.GetDsn())
}
