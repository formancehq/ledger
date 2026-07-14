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

func newLedger(name, namespace, clusterRef, ledgerName string) *ledgerv1alpha1.Ledger {
	return &ledgerv1alpha1.Ledger{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: ledgerv1alpha1.LedgerCRDSpec{
			Name:       ledgerName,
			ClusterRef: clusterRef,
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

func TestBuildCreateArgs_MirrorHTTPWithRewriteRules(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		RewriteRules: []ledgerv1alpha1.MirrorRewriteRule{
			{AnyVariant: &ledgerv1alpha1.AnyVariantRule{
				Actions: []ledgerv1alpha1.AnyVariantAction{{
					RewriteAddress: &ledgerv1alpha1.RewriteAddressAction{Pattern: ":worker:\\d+", Replacement: ""},
				}},
			}},
			{CreatedTransaction: &ledgerv1alpha1.CreatedTransactionRule{
				Match: `log.metadata["type"].string_value == "payout"`,
				Actions: []ledgerv1alpha1.CreatedTransactionAction{{
					SetMetadata: &ledgerv1alpha1.SetMetadataAction{Key: "category", Value: "external"},
				}},
			}, Stop: true},
		},
		HTTP: &ledgerv1alpha1.HTTPMirrorSource{
			BaseURL: "https://source.example.com",
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-rewrite-rule", `{"anyVariant":{"actions":[{"rewriteAddress":{"pattern":":worker:\\d+","replacement":""}}]}}`,
		"--mirror-rewrite-rule", `{"createdTransaction":{"match":"log.metadata[\"type\"].string_value == \"payout\"","actions":[{"setMetadata":{"key":"category","value":"external"}}]},"stop":true}`,
		"--mirror-source-type", "http",
		"--mirror-base-url", "https://source.example.com",
	}, args)
}

func TestBuildCreateArgs_MirrorPostgresWithRewriteRules(t *testing.T) {
	t.Parallel()

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("s3cr3t"),
		},
	}

	r := newTestLedgerReconciler(secret)
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		RewriteRules: []ledgerv1alpha1.MirrorRewriteRule{
			{AnyVariant: &ledgerv1alpha1.AnyVariantRule{
				Actions: []ledgerv1alpha1.AnyVariantAction{{
					RewriteAddress: &ledgerv1alpha1.RewriteAddressAction{Pattern: ":worker:\\d+", Replacement: ""},
				}},
			}},
		},
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.example.com",
			Port:     5432,
			User:     "ledger",
			Database: "ledger",
			SSLMode:  "require",
			PasswordFrom: &ledgerv1alpha1.SecretKeyRef{
				Name: "pg-secret",
				Key:  "password",
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-rewrite-rule", `{"anyVariant":{"actions":[{"rewriteAddress":{"pattern":":worker:\\d+","replacement":""}}]}}`,
		"--mirror-source-type", "postgres",
		"--mirror-dsn", "postgres://ledger:s3cr3t@db.example.com:5432/ledger?sslmode=require",
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

func TestBuildCreateArgs_MirrorPostgresPassword(t *testing.T) {
	t.Parallel()

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("s3cr3t"),
		},
	}

	r := newTestLedgerReconciler(secret)
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.example.com",
			Port:     5432,
			User:     "ledger",
			Database: "ledger",
			SSLMode:  "require",
			PasswordFrom: &ledgerv1alpha1.SecretKeyRef{
				Name: "pg-secret",
				Key:  "password",
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-source-type", "postgres",
		"--mirror-dsn", "postgres://ledger:s3cr3t@db.example.com:5432/ledger?sslmode=require",
	}, args)
}

func TestBuildCreateArgs_MirrorPostgresPasswordSpecialChars(t *testing.T) {
	t.Parallel()

	// Passwords with URL-special characters must be percent-encoded so the
	// generated DSN parses cleanly on the ledger side.
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"password": []byte("p@ss|word?#"),
		},
	}

	r := newTestLedgerReconciler(secret)
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.example.com",
			User:     "ledger",
			Database: "ledger",
			PasswordFrom: &ledgerv1alpha1.SecretKeyRef{
				Name: "pg-secret",
				Key:  "password",
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	// Port and SSLMode fall back to their defaults (5432, require).
	assert.Contains(t, args, "--mirror-dsn")
	assert.Contains(t, args, "postgres://ledger:p%40ss%7Cword%3F%23@db.example.com:5432/ledger?sslmode=require")
}

func TestBuildCreateArgs_MirrorPostgresAWSIAMAuth(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.region.rds.amazonaws.com",
			User:     "iam-user",
			Database: "ledger",
			AWSIAMAuth: &ledgerv1alpha1.AWSIAMAuthSpec{
				Region: "eu-west-1",
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-source-type", "postgres",
		"--mirror-dsn", "postgres://iam-user@db.region.rds.amazonaws.com:5432/ledger?sslmode=require",
		"--mirror-aws-iam-region", "eu-west-1",
	}, args)
}

func TestBuildCreateArgs_MirrorPostgresAWSIAMAuthWithAssumeRole(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.region.rds.amazonaws.com",
			User:     "iam-user",
			Database: "ledger",
			AWSIAMAuth: &ledgerv1alpha1.AWSIAMAuthSpec{
				Region:        "eu-west-1",
				AssumeRoleArn: "arn:aws:iam::222222222222:role/cross-tenant-mirror",
			},
		},
	}

	args, err := r.buildCreateArgs(context.Background(), ledger)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"ledgers", "create", "--name", "ledger1",
		"--mode", "mirror",
		"--mirror-source-type", "postgres",
		"--mirror-dsn", "postgres://iam-user@db.region.rds.amazonaws.com:5432/ledger?sslmode=require",
		"--mirror-aws-iam-region", "eu-west-1",
		"--mirror-aws-iam-assume-role-arn", "arn:aws:iam::222222222222:role/cross-tenant-mirror",
	}, args)
}

func TestBuildCreateArgs_MirrorPostgresMissingAuth(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.example.com",
			User:     "ledger",
			Database: "ledger",
		},
	}

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "passwordFrom or awsIamAuth must be set")
}

func TestBuildCreateArgs_MirrorPostgresBothAuthRejected(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.example.com",
			User:     "ledger",
			Database: "ledger",
			PasswordFrom: &ledgerv1alpha1.SecretKeyRef{
				Name: "pg-secret",
				Key:  "password",
			},
			AWSIAMAuth: &ledgerv1alpha1.AWSIAMAuthSpec{
				Region: "eu-west-1",
			},
		},
	}

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestBuildCreateArgs_MirrorPostgresPasswordSecretMissing(t *testing.T) {
	t.Parallel()

	r := newTestLedgerReconciler()
	ledger := newLedger("test", "default", "svc", "ledger1")
	ledger.Spec.Mode = "mirror"
	ledger.Spec.MirrorSource = &ledgerv1alpha1.MirrorSourceSpec{
		Postgres: &ledgerv1alpha1.PostgresMirrorSource{
			Host:     "db.example.com",
			User:     "ledger",
			Database: "ledger",
			PasswordFrom: &ledgerv1alpha1.SecretKeyRef{
				Name: "missing",
				Key:  "password",
			},
		},
	}

	_, err := r.buildCreateArgs(context.Background(), ledger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading Postgres password secret")
}

// ---------------------------------------------------------------------------
// computeLedgerSpecHash
// ---------------------------------------------------------------------------

func TestComputeLedgerSpecHash_Deterministic(t *testing.T) {
	t.Parallel()

	spec := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ClusterRef: "svc",
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
		ClusterRef: "svc",
		Mode:       "normal",
	}
	spec2 := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ClusterRef: "svc",
		Mode:       "mirror",
	}
	assert.NotEqual(t, computeLedgerSpecHash(spec1), computeLedgerSpecHash(spec2))
}

// TestComputeLedgerSpecHash_IgnoresIndexes asserts that index edits do not
// change the spec hash — indexes are mutable and reconciled continuously, so
// editing them must not trip the SpecDrifted (immutability) condition.
func TestComputeLedgerSpecHash_IgnoresIndexes(t *testing.T) {
	t.Parallel()

	base := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ClusterRef: "svc",
		Mode:       "normal",
	}
	withIndexes := &ledgerv1alpha1.LedgerCRDSpec{
		Name:       "test",
		ClusterRef: "svc",
		Mode:       "normal",
		Indexes: &ledgerv1alpha1.LedgerIndexesSpec{
			Transaction: []string{"reference", "address"},
			Account:     []string{"asset"},
			Metadata:    []ledgerv1alpha1.MetadataIndexSpec{{Target: "account", Key: "k", Type: "string"}},
		},
	}

	assert.Equal(t, computeLedgerSpecHash(base), computeLedgerSpecHash(withIndexes))

	// And the input spec must not be mutated by hashing (shallow copy nils the
	// pointer on the copy, not the original).
	require.NotNil(t, withIndexes.Indexes)
}
