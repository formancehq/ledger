//go:build it

package read_test

import (
	"context"
	"flag"
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/test/performance/pkg/env"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	ledgerFlag string
	configFlag string
)

func init() {
	flag.StringVar(&ledgerFlag, "ledger", "default", "Ledger to use")
	flag.StringVar(&configFlag, "config", "", "Configuration to use")
}

type Operation interface {
	Run(ctx context.Context, env env.Env) error
}

type operationCommon struct {
	Filter map[string]any `yaml:"filter"`
	PIT    time.Time      `yaml:"pit"`
}

type operatorListAccounts struct {
	operationCommon `yaml:",inline"`
	Expand          []string `yaml:"expand"`
}

func (o operatorListAccounts) Run(ctx context.Context, env env.Env) error {
	_, err := env.Client().Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
		Ledger:      ledgerFlag,
		Expand:      pointer.For(strings.Join(o.Expand, ",")),
		Pit:         pointer.For(o.PIT),
		RequestBody: o.Filter,
	})
	return err
}

type operatorListTransactions struct {
	operationCommon `yaml:",inline"`
	Expand          []string `yaml:"expand"`
}

func (o operatorListTransactions) Run(ctx context.Context, env env.Env) error {
	_, err := env.Client().Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
		Ledger:      ledgerFlag,
		Expand:      pointer.For(strings.Join(o.Expand, ",")),
		Pit:         pointer.For(o.PIT),
		RequestBody: o.Filter,
	})
	return err
}

type operationListVolumes struct {
	operationCommon `yaml:",inline"`
	OOT             time.Time `yaml:"oot"`
	InsertionDate   *bool
	GroupBy         *int64
}

func (o operationListVolumes) Run(ctx context.Context, env env.Env) error {
	_, err := env.Client().Ledger.V2.GetVolumesWithBalances(ctx, operations.V2GetVolumesWithBalancesRequest{
		Ledger:        ledgerFlag,
		EndTime:       pointer.For(o.PIT),
		StartTime:     pointer.For(o.OOT),
		InsertionDate: o.InsertionDate,
		GroupBy:       o.GroupBy,
		RequestBody:   o.Filter,
	})
	return err
}

type Query struct {
	Name      string    `yaml:"name"`
	Operation Operation `yaml:"operation"`
}

func (q *Query) UnmarshalYAML(value *yaml.Node) error {
	type aux struct {
		Operation string `yaml:"operation"`
		Name      string `yaml:"name"`
	}
	x := &aux{}
	if err := value.Decode(x); err != nil {
		return err
	}

	switch x.Operation {
	case "list-accounts":
		q.Operation = &operatorListAccounts{}
	case "list-volumes":
		q.Operation = &operationListVolumes{}
	case "list-transactions":
		q.Operation = &operatorListTransactions{}
	default:
		return fmt.Errorf("unsupported operation: %s", x.Operation)
	}
	q.Name = x.Name

	return value.Decode(q.Operation)
}

type Configuration struct {
	Queries []Query
}

func BenchmarkRead(b *testing.B) {
	if configFlag == "" {
		b.Fatalf("No configuration provided")
	}
	env.Start()

	cfg := &Configuration{}
	f, err := os.Open(configFlag)
	if err != nil {
		b.Fatalf("Failed to open configuration file: %s", err)
	}

	err = yaml.NewDecoder(f).Decode(cfg)
	if err != nil {
		b.Fatalf("Failed to decode configuration file: %s", err)
	}

	env := env.Factory.Create(context.Background(), b)
	ctx := logging.TestingContext()

	for _, query := range cfg.Queries {
		b.Run(query.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				require.NoError(b, query.Operation.Run(ctx, env))
			}
		})
	}
}
