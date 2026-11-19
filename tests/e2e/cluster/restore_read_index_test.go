//go:build e2e && s3

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/metrics"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Wait for the local indexer to reach a nonempty primary log head.
func caughtUpReadIndexSequence(ctx context.Context, client servicepb.BucketServiceClient) uint64 {
	GinkgoHelper()
	var prefix uint64
	Eventually(func(g Gomega) {
		status, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(status.GetLastIndexedSequence()).To(BeNumerically(">", 0))
		g.Expect(status.GetLastIndexedSequence()).To(Equal(status.GetLastLogSequence()))
		prefix = status.GetLastIndexedSequence()
	}).Should(Succeed())
	return prefix
}

// Indexed results alone cannot distinguish a restored index from a fast full
// rebuild on a tiny fixture. Once caught up, subtract the logs actually folded
// by THIS process from its absolute cursor. A cold replay gives zero; a warm
// resume has skipped at least the prefix observed before taking the full.
func expectIndexResumedFromBackup(ctx context.Context, client servicepb.BucketServiceClient, httpPort int, prefix uint64) {
	GinkgoHelper()
	head := caughtUpReadIndexSequence(ctx, client)
	Expect(head).To(BeNumerically(">=", prefix))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", httpPort), nil)
	Expect(err).NotTo(HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	Expect(err).NotTo(HaveOccurred())
	defer func() { _ = resp.Body.Close() }()
	Expect(resp.StatusCode).To(Equal(http.StatusOK))

	var snapshot struct {
		ScopeMetrics []struct {
			Metrics []struct {
				Name string
				Data struct {
					DataPoints []struct{ Value json.Number }
				}
			}
		}
	}
	Expect(json.NewDecoder(resp.Body).Decode(&snapshot)).To(Succeed())
	for _, scope := range snapshot.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name == metrics.DefaultPrefix+".index.builder.logs_indexed" {
				Expect(instrument.Data.DataPoints).To(HaveLen(1))
				folded, err := instrument.Data.DataPoints[0].Value.Int64()
				Expect(err).NotTo(HaveOccurred())
				Expect(folded).To(BeNumerically("<=", head-prefix),
					"restored indexer folded %d logs from a history of %d; the full already covered at least %d", folded, head, prefix)
				return
			}
		}
	}
	Fail("missing index.builder.logs_indexed from restored node metrics")
}
