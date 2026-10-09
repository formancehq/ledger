//go:build e2e

package business

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/go-jose/go-jose/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("HTTP events sinks mutations", func() {
	const path = "/v3/_/events-sinks"
	client := &http.Client{Timeout: 10 * time.Second}
	request := func(method, suffix string, body []byte, key string) (int, []byte) {
		GinkgoHelper()
		req, err := http.NewRequestWithContext(sharedCtx, method,
			fmt.Sprintf("http://localhost:%d%s%s", sharedHTTPPort, path, suffix), bytes.NewReader(body))
		Expect(err).To(Succeed())
		req.Header.Set("Content-Type", "application/json")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		resp, err := client.Do(req)
		Expect(err).To(Succeed())
		defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
		raw, err := io.ReadAll(resp.Body)
		Expect(err).To(Succeed())
		return resp.StatusCode, raw
	}
	errorReason := func(raw []byte, reason string) {
		GinkgoHelper()
		var response struct {
			ErrorCode string `json:"errorCode"`
		}
		Expect(json.Unmarshal(raw, &response)).To(Succeed())
		Expect(response.ErrorCode).To(Equal(reason), string(raw))
	}
	encode := func(config *commonpb.SinkConfig) []byte {
		GinkgoHelper()
		raw, err := protojson.Marshal(config)
		Expect(err).To(Succeed())
		return raw
	}
	list := func() []*commonpb.SinkConfig {
		GinkgoHelper()
		code, raw := request(http.MethodGet, "", nil, "")
		Expect(code).To(Equal(http.StatusOK), string(raw))
		var envelope struct {
			Data json.RawMessage `json:"data"`
		}
		Expect(json.Unmarshal(raw, &envelope)).To(Succeed())
		data := &servicepb.GetEventsSinksResponse{}
		Expect(protojson.Unmarshal(envelope.Data, data)).To(Succeed())
		return data.GetSinks()
	}
	find := func(name string) *commonpb.SinkConfig {
		GinkgoHelper()
		for _, config := range list() {
			if config.GetName() == name {
				return config
			}
		}
		return nil
	}
	cleanup := func(config *commonpb.SinkConfig) {
		GinkgoHelper()
		DeferCleanup(func() {
			if find(config.GetName()) == nil {
				return
			}
			action := removeEventsSinkAction(config.GetName())
			action.GetRemoveEventsSink().ControllerId = config.GetControllerId()
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", action))
			Expect(err).To(Succeed())
		})
	}
	seed := func(config *commonpb.SinkConfig) {
		GinkgoHelper()
		cleanup(config)
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", addEventsSinkAction(config)))
		Expect(err).To(Succeed())
	}
	created := func(code int, raw []byte, name string) {
		GinkgoHelper()
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		Expect(string(raw)).To(MatchJSON(fmt.Sprintf(`{"data":{"name":%q}}`, name)))
	}
	removed := func(code int, raw []byte) {
		GinkgoHelper()
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		Expect(raw).To(BeEmpty())
	}

	It("creates, lists and deletes a direct protobuf JSON SinkConfig over HTTP", func() {
		config := newTestSinkConfig("http-sinks-cycle", "http.events.cycle")
		config.ControllerId = "controller-cycle"
		cleanup(config)
		code, raw := request(http.MethodPost, "", encode(config), "")
		created(code, raw, config.GetName())
		Expect(proto.Equal(find(config.GetName()), config)).To(BeTrue())
		persisted, err := sharedClient.GetEventsSinks(sharedCtx, &servicepb.GetEventsSinksRequest{})
		Expect(err).To(Succeed())
		Expect(persisted.GetSinks()).To(ContainElement(Satisfy(func(s *commonpb.SinkConfig) bool { return proto.Equal(s, config) })))
		code, raw = request(http.MethodDelete, "/"+config.GetName()+"?controllerId="+url.QueryEscape(config.GetControllerId()), nil, "")
		removed(code, raw)
		Expect(find(config.GetName())).To(BeNil())
	})

	It("rejects duplicate names without replacing the Apply-created configuration", func() {
		config := newTestSinkConfig("http-sinks-duplicate", "http.events.original")
		seed(config)
		duplicate := proto.Clone(config).(*commonpb.SinkConfig)
		duplicate.GetNats().Topic = "http.events.replacement"
		code, raw := request(http.MethodPost, "", encode(duplicate), "")
		Expect(code).To(Equal(http.StatusConflict), string(raw))
		errorReason(raw, domain.ErrReasonSinkAlreadyExists)
		Expect(proto.Equal(find(config.GetName()), config)).To(BeTrue())
	})

	DescribeTable("rejects invalid JSON or sink configuration without persisting it", func(name, body string) {
		code, raw := request(http.MethodPost, "", []byte(body), "")
		Expect(code).To(Equal(http.StatusBadRequest), string(raw))
		if name == "http-sinks-oversized" {
			errorReason(raw, domain.ErrReasonSinkBatchSizeTooLarge)
		}
		if name != "" {
			Expect(find(name)).To(BeNil())
		}
	},
		Entry("malformed JSON", "", `{"name":`),
		Entry("missing configuration", "", `{}`),
		Entry("missing name", "", `{"format":"json","nats":{"url":"nats://localhost:4222","topic":"http.events.no-name"}}`),
		Entry("unknown protobuf field", "http-sinks-unknown", `{"name":"http-sinks-unknown","unknown":true}`),
		Entry("missing sink type", "http-sinks-no-type", `{"name":"http-sinks-no-type","format":"json"}`),
		Entry("oversized batch", "http-sinks-oversized", fmt.Sprintf(`{"name":"http-sinks-oversized","format":"json","batchSize":%d,"nats":{"url":"nats://localhost:4222","topic":"http.events.invalid"}}`, domain.MaxSinkBatchSize+1)),
	)

	It("returns not found when deleting an absent sink", func() {
		code, raw := request(http.MethodDelete, "/http-sinks-missing?controllerId=owner", nil, "")
		Expect(code).To(Equal(http.StatusNotFound), string(raw))
		errorReason(raw, domain.ErrReasonSinkNotFound)
	})

	It("checks controller ownership and permits deletion with the matching identity", func() {
		config := newTestSinkConfig("http-sinks-owned", "http.events.owned")
		config.ControllerId = "owner/with spaces?&=+"
		seed(config)
		for _, query := range []string{"controllerId=%ZZ", "controllerId=owner;extra=x", "controllerId=owner&controllerId=another"} {
			code, raw := request(http.MethodDelete, "/"+config.GetName()+"?"+query, nil, "")
			Expect(code).To(Equal(http.StatusBadRequest), string(raw))
			Expect(string(raw)).To(ContainSubstring("INVALID_REQUEST"))
			Expect(proto.Equal(find(config.GetName()), config)).To(BeTrue())
		}
		for _, owner := range []string{"another-owner", "owner/with spaces?&= "} {
			code, raw := request(http.MethodDelete, "/"+config.GetName()+"?controllerId="+url.QueryEscape(owner), nil, "")
			Expect(code).To(Equal(http.StatusConflict), string(raw))
			errorReason(raw, domain.ErrReasonSinkControllerMismatch)
			Expect(proto.Equal(find(config.GetName()), config)).To(BeTrue())
		}
		code, raw := request(http.MethodDelete, "/"+config.GetName()+"?controllerId="+url.QueryEscape(config.GetControllerId()), nil, "")
		removed(code, raw)
		Expect(find(config.GetName())).To(BeNil())
	})

	It("does not grant a controller ownership of a manually created sink", func() {
		config := newTestSinkConfig("http-sinks-manual", "http.events.manual")
		seed(config)
		code, raw := request(http.MethodDelete, "/"+config.GetName()+"?controllerId=claimed-owner", nil, "")
		Expect(code).To(Equal(http.StatusConflict), string(raw))
		errorReason(raw, domain.ErrReasonSinkControllerMismatch)
		Expect(proto.Equal(find(config.GetName()), config)).To(BeTrue())
		code, raw = request(http.MethodDelete, "/"+config.GetName(), nil, "")
		removed(code, raw)
		Expect(find(config.GetName())).To(BeNil())
	})

	It("retains unconditional deletion when controllerId is omitted", func() {
		config := newTestSinkConfig("http-sinks-unconditional", "http.events.unconditional")
		config.ControllerId = "controller-unconditional"
		seed(config)
		code, raw := request(http.MethodDelete, "/"+config.GetName(), nil, "")
		removed(code, raw)
		Expect(find(config.GetName())).To(BeNil())
	})

	It("replays creation without reapplying it after deletion and rejects key reuse with a different config", func() {
		config := newTestSinkConfig("http-sinks-idempotent-create", "http.events.idempotent.create")
		cleanup(config)
		body := encode(config)
		key := "http-sinks-create-key"
		code, raw := request(http.MethodPost, "", body, key)
		created(code, raw, config.GetName())
		code, raw = request(http.MethodPost, "", body, key)
		created(code, raw, config.GetName())
		Expect(proto.Equal(find(config.GetName()), config)).To(BeTrue())
		code, raw = request(http.MethodDelete, "/"+config.GetName(), nil, "")
		removed(code, raw)
		code, raw = request(http.MethodPost, "", body, key)
		created(code, raw, config.GetName())
		Expect(find(config.GetName())).To(BeNil(), "replay must not resurrect the sink")
		changed := proto.Clone(config).(*commonpb.SinkConfig)
		changed.GetNats().Topic = "http.events.changed"
		code, raw = request(http.MethodPost, "", encode(changed), key)
		Expect(code).To(Equal(http.StatusConflict), string(raw))
		errorReason(raw, domain.ErrReasonIdempotencyKeyConflict)
		Expect(find(config.GetName())).To(BeNil())
	})

	It("replays deletion without removing a recreated sink and binds the key to controllerId", func() {
		config := newTestSinkConfig("http-sinks-idempotent-delete", "http.events.idempotent.delete")
		config.ControllerId = "original-owner"
		seed(config)
		suffix := "/" + config.GetName() + "?controllerId=" + config.GetControllerId()
		key := "http-sinks-delete-key"
		code, raw := request(http.MethodDelete, suffix, nil, key)
		removed(code, raw)
		code, raw = request(http.MethodDelete, suffix, nil, key)
		removed(code, raw)
		replacement := proto.Clone(config).(*commonpb.SinkConfig)
		replacement.ControllerId = "replacement-owner"
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", addEventsSinkAction(replacement)))
		Expect(err).To(Succeed())
		// Cleanup must use the replacement owner even if a later assertion fails.
		config.ControllerId = replacement.GetControllerId()
		code, raw = request(http.MethodDelete, suffix, nil, key)
		removed(code, raw)
		Expect(proto.Equal(find(config.GetName()), replacement)).To(BeTrue())
		code, raw = request(http.MethodDelete, "/"+config.GetName()+"?controllerId="+replacement.GetControllerId(), nil, key)
		Expect(code).To(Equal(http.StatusConflict), string(raw))
		errorReason(raw, domain.ErrReasonIdempotencyKeyConflict)
		Expect(proto.Equal(find(config.GetName()), replacement)).To(BeTrue())
	})
})

var _ = Describe("HTTP events sinks mutation scopes", func() {
	It("requires ledger:OpsWrite for both mutations on an authenticated node", func() {
		ctx := GinkgoT().Context()
		dir := GinkgoT().TempDir()
		public, private, err := ed25519.GenerateKey(rand.Reader)
		Expect(err).To(Succeed())
		publicPath := filepath.Join(dir, "public.hex")
		Expect(os.WriteFile(publicPath, fmt.Appendf(nil, "%x\n", public), 0600)).To(Succeed())
		config, err := json.Marshal(internalauth.Ed25519KeysConfig{Keys: []internalauth.Ed25519KeyEntry{{
			KeyID: "http-sinks-scope-key", PublicKeyFile: publicPath,
			Scopes: []string{"ledger:admin", "ledger:OpsRead", "ledger:OpsWrite", "ledger:TransactionWrite"},
		}}})
		Expect(err).To(Succeed())
		configPath := filepath.Join(dir, "auth.json")
		Expect(os.WriteFile(configPath, config, 0600)).To(Succeed())
		token := func(scope string) string {
			GinkgoHelper()
			claims := &oidc.AccessTokenClaims{}
			claims.Subject = "http-sinks-scope-user"
			claims.IssuedAt = oidc.FromTime(oidc.Time(time.Now().Unix()).AsTime())
			claims.Expiration = oidc.FromTime(oidc.Time(time.Now().Add(time.Hour).Unix()).AsTime())
			claims.Scopes = oidc.SpaceDelimitedArray{scope}
			payload, err := json.Marshal(claims)
			Expect(err).To(Succeed())
			signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA,
				Key: &jose.JSONWebKey{Key: private, KeyID: "http-sinks-scope-key"}}, nil)
			Expect(err).To(Succeed())
			signed, err := signer.Sign(payload)
			Expect(err).To(Succeed())
			raw, err := signed.CompactSerialize()
			Expect(err).To(Succeed())
			return raw
		}
		certs, err := testserver.GenerateTestCerts(dir)
		Expect(err).To(Succeed())
		lease := testserver.AllocateNodeLease()
		ports := lease.Ports()
		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID: 1, ClusterID: "http-sinks-scope-cluster", Ports: ports,
			WalDir: GinkgoT().TempDir(), DataDir: GinkgoT().TempDir(), Output: GinkgoWriter,
		})
		instruments = append(instruments, testserver.WithBootstrap(), testserver.WithAuthEnabled(),
			testserver.WithAuthEd25519Keys(configPath), testserver.WithAuthService("ledger"),
			testserver.WithTLSMode("required"), testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile))
		server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
		Expect(server.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})
		ca, err := os.ReadFile(certs.CACertFile)
		Expect(err).To(Succeed())
		roots := x509.NewCertPool()
		Expect(roots.AppendCertsFromPEM(ca)).To(BeTrue())
		tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", ports.GRPC()),
			grpcprotocol.ClientOption(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		Expect(err).To(Succeed())
		DeferCleanup(func() { Expect(conn.Close()).To(Succeed()) })
		cluster := clusterpb.NewClusterServiceClient(conn)
		adminCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token("ledger:admin"))
		Eventually(func(g Gomega) uint32 {
			state, err := cluster.GetClusterState(adminCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.GetLeader()
		}).Within(10 * time.Second).ShouldNot(BeZero())
		transport := &http.Transport{TLSClientConfig: tlsConfig}
		DeferCleanup(transport.CloseIdleConnections)
		client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
		request := func(method, suffix, scope string, body []byte) (int, []byte) {
			GinkgoHelper()
			req, err := http.NewRequestWithContext(ctx, method,
				fmt.Sprintf("http://localhost:%d/v3/_/events-sinks%s", ports.HTTP(), suffix), bytes.NewReader(body))
			Expect(err).To(Succeed())
			req.Header.Set("Content-Type", "application/json")
			if scope != "" {
				req.Header.Set("Authorization", "Bearer "+token(scope))
			}
			resp, err := client.Do(req)
			Expect(err).To(Succeed())
			defer func() { Expect(resp.Body.Close()).To(Succeed()) }()
			raw, err := io.ReadAll(resp.Body)
			Expect(err).To(Succeed())
			return resp.StatusCode, raw
		}
		sink := newTestSinkConfig("http-sinks-scopes", "http.events.scopes")
		body, err := protojson.Marshal(sink)
		Expect(err).To(Succeed())
		for _, scope := range []string{"", "ledger:OpsRead", "ledger:TransactionWrite"} {
			code, raw := request(http.MethodPost, "", scope, body)
			expected := http.StatusForbidden
			if scope == "" {
				expected = http.StatusUnauthorized
			}
			Expect(code).To(Equal(expected), "scope=%q body=%s", scope, raw)
		}
		code, raw := request(http.MethodPost, "", "ledger:OpsWrite", body)
		Expect(code).To(Equal(http.StatusCreated), string(raw))
		Expect(string(raw)).To(MatchJSON(`{"data":{"name":"http-sinks-scopes"}}`))
		for _, scope := range []string{"", "ledger:OpsRead", "ledger:TransactionWrite"} {
			code, raw := request(http.MethodDelete, "/"+sink.GetName(), scope, nil)
			expected := http.StatusForbidden
			if scope == "" {
				expected = http.StatusUnauthorized
			}
			Expect(code).To(Equal(expected), "scope=%q body=%s", scope, raw)
		}
		code, raw = request(http.MethodGet, "", "ledger:OpsRead", nil)
		Expect(code).To(Equal(http.StatusOK), string(raw))
		Expect(string(raw)).To(ContainSubstring(sink.GetName()))
		code, raw = request(http.MethodDelete, "/"+sink.GetName(), "ledger:OpsWrite", nil)
		Expect(code).To(Equal(http.StatusNoContent), string(raw))
		Expect(raw).To(BeEmpty())
		code, raw = request(http.MethodGet, "", "ledger:OpsRead", nil)
		Expect(code).To(Equal(http.StatusOK), string(raw))
		Expect(string(raw)).NotTo(ContainSubstring(sink.GetName()))
	})
})
