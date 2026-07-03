package ledger

import (
	"fmt"
	"regexp"

	"github.com/google/uuid"
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/types/time"
)

// AddressRewriteRule rewrites account addresses while a pipeline mirrors a
// ledger to an exporter. Pattern is a Go regular expression applied to the full
// account address; matches are substituted with Replacement (which may reference
// capture groups). An empty Replacement drops the matched part, allowing segments
// used as lock-avoidance tricks to be removed, e.g. Pattern "(:worker:\d+)" turns
// "payments:acme:worker:001:main" into "payments:acme:main".
type AddressRewriteRule struct {
	Pattern     string `json:"pattern" bun:"pattern"`
	Replacement string `json:"replacement" bun:"replacement"`
}

type PipelineConfiguration struct {
	Ledger     string `json:"ledger" bun:"ledger"`
	ExporterID string `json:"exporterID" bun:"exporter_id"`
	// AddressRewriteRules are applied, in order, to every account address in the
	// mirrored logs before they are pushed to the exporter. They never affect the
	// source ledger; the mirrored stream is a projection.
	AddressRewriteRules []AddressRewriteRule `json:"addressRewriteRules,omitempty" bun:"address_rewrite_rules,type:jsonb,nullzero"`
}

func (p PipelineConfiguration) String() string {
	return fmt.Sprintf("%s/%s", p.Ledger, p.ExporterID)
}

// Validate checks that every address rewrite rule holds a compilable regular
// expression. It is called on both the HTTP and gRPC pipeline-creation paths.
func (p PipelineConfiguration) Validate() error {
	for _, rule := range p.AddressRewriteRules {
		if _, err := regexp.Compile(rule.Pattern); err != nil {
			return fmt.Errorf("invalid address rewrite pattern %q: %w", rule.Pattern, err)
		}
	}
	return nil
}

func NewPipelineConfiguration(ledger, exporterID string) PipelineConfiguration {
	return PipelineConfiguration{
		Ledger:     ledger,
		ExporterID: exporterID,
	}
}

type Pipeline struct {
	bun.BaseModel `bun:"table:_system.pipelines"`

	PipelineConfiguration
	CreatedAt time.Time `json:"createdAt" bun:"created_at"`
	ID        string    `json:"id" bun:"id,pk"`
	Enabled   bool      `json:"enabled" bun:"enabled"`
	LastLogID *uint64   `json:"lastLogID,omitempty" bun:"last_log_id"`
	Error     string    `json:"error,omitempty" bun:"error"`
}

func NewPipeline(pipelineConfiguration PipelineConfiguration) Pipeline {
	return Pipeline{
		ID:                    uuid.NewString(),
		PipelineConfiguration: pipelineConfiguration,
		Enabled:               true,
		CreatedAt:             time.Now(),
		LastLogID:             nil,
	}
}
