package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func newTestLedgerReconciler(objects ...corev1.Secret) *LedgerReconciler {
	clientset := fake.NewClientset()
	for i := range objects {
		_, _ = clientset.CoreV1().Secrets(objects[i].Namespace).Create(
			context.Background(), &objects[i], metav1.CreateOptions{})
	}

	return &LedgerReconciler{
		Clientset: clientset,
	}
}

func newLedger(name, namespace, serviceRef, ledgerName string) *ledgerv1alpha1.Ledger {
	return &ledgerv1alpha1.Ledger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ledgerv1alpha1.LedgerCRDSpec{
			Name:       ledgerName,
			ServiceRef: serviceRef,
		},
	}
}

// ---------------------------------------------------------------------------
// Normal mode
// ---------------------------------------------------------------------------

func TestBuildCreateArgs_NormalMode(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test-ledger", "default", "my-service", "orders")

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{"ledgers", "create", "--name", "orders"}, args)
}

func TestBuildCreateArgs_NormalModeExplicit(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test-ledger", "default", "my-service", "orders")
	ledger.Spec.Mode = "normal"

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{"ledgers", "create", "--name", "orders"}, args)
}

// ---------------------------------------------------------------------------
// Mirror mode — validation errors
// ---------------------------------------------------------------------------

func TestBuildCreateArgs_MirrorMissingSource(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mirrorSource is required")
}

func TestBuildCreateArgs_MirrorNoHTTPOrPostgres(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{}

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must specify either http or postgres")
}

// ---------------------------------------------------------------------------
// Mirror mode — HTTP source
// ---------------------------------------------------------------------------

func TestBuildCreateArgs_MirrorHTTPBasic(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{
			BaseURL: "https://source.example.com",
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-source-type", "http",
		"--mirror-base-url", "https://source.example.com",
	}, args)
}

func TestBuildCreateArgs_MirrorHTTPWithOptions(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	batchSize := int32(500)
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		LedgerName: "source-ledger",
		BatchSize:  &batchSize,
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{
			BaseURL: "https://source.example.com",
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-ledger-name", "source-ledger",
		"--mirror-batch-size", "500",
		"--mirror-source-type", "http",
		"--mirror-base-url", "https://source.example.com",
	}, args)
}

func TestBuildCreateArgs_MirrorHTTPWithOAuth2(t *testing.T) {
	t.Parallel()

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "oauth-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"client-secret": []byte("super-secret-value"),
		},
	}

	r := newTestLedgerReconciler(secret)
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{
			BaseURL: "https://source.example.com",
			OAuth2: &ledgerv1alpha1.OAuth2ClientCredentials{
				ClientID: "my-client-id",
				ClientSecretFrom: ledgerv1alpha1.SecretKeyRef{
					Name: "oauth-secret",
					Key:  "client-secret",
				},
				TokenEndpoint: "https://auth.example.com/token",
				Scopes:        []string{"ledger:read", "ledger:write"},
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-source-type", "http",
		"--mirror-base-url", "https://source.example.com",
		"--mirror-oauth2-client-id", "my-client-id",
		"--mirror-oauth2-token-endpoint", "https://auth.example.com/token",
		"--mirror-oauth2-client-secret", "super-secret-value",
		"--mirror-oauth2-scopes", "ledger:read",
		"--mirror-oauth2-scopes", "ledger:write",
	}, args)
}

func TestBuildCreateArgs_MirrorHTTPOAuth2SecretMissing(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler() // no secrets
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{
			BaseURL: "https://source.example.com",
			OAuth2: &ledgerv1alpha1.OAuth2ClientCredentials{
				ClientID: "my-client-id",
				ClientSecretFrom: ledgerv1alpha1.SecretKeyRef{
					Name: "missing-secret",
					Key:  "key",
				},
				TokenEndpoint: "https://auth.example.com/token",
			},
		},
	}

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading OAuth2 client secret")
}

// ---------------------------------------------------------------------------
// Mirror mode — PostgreSQL source
// ---------------------------------------------------------------------------

func TestBuildCreateArgs_MirrorPostgres(t *testing.T) {
	t.Parallel()

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"dsn": []byte("postgres://user:pass@host:5432/db"),
		},
	}

	r := newTestLedgerReconciler(secret)
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			DSNFrom: ledgerv1alpha1.SecretKeyRef{
				Name: "pg-secret",
				Key:  "dsn",
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-source-type", "postgres",
		"--mirror-dsn", "postgres://user:pass@host:5432/db",
	}, args)
}

func TestBuildCreateArgs_MirrorPostgresSecretMissing(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			DSNFrom: ledgerv1alpha1.SecretKeyRef{
				Name: "missing",
				Key:  "dsn",
			},
		},
	}

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading Postgres DSN secret")
}

// ---------------------------------------------------------------------------
// computeLedgerSpecHash
// ---------------------------------------------------------------------------

func TestComputeLedgerSpecHash_Deterministic(t *testing.T) {
	t.Parallel()

	spec := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ServiceRef: "svc",
		Mode:       "normal",
	}
	h1 := computeLedgerSpecHash(spec)
	h2 := computeLedgerSpecHash(spec)
	assert.Equal(t, h1, h2)
}

func TestComputeLedgerSpecHash_DifferentMode(t *testing.T) {
	t.Parallel()

	spec1 := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ServiceRef: "svc",
		Mode:       "normal",
	}
	spec2 := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ServiceRef: "svc",
		Mode:       "mirror",
	}
	assert.NotEqual(t, computeLedgerSpecHash(spec1), computeLedgerSpecHash(spec2))
}
