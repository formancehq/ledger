package ledger

import (
	"context"
	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/ledger/internal/storage/workers/lockmonitor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"strings"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source monitor.go -destination monitor_generated_test.go -package ledger_test . Recorder
type Recorder interface {
	// ledger/account/asset/counter
	Record(ctx context.Context, stats map[string]map[string]map[string]int)
}

type otlpRecorder struct {
	metric metric.Int64Histogram
}

func (o otlpRecorder) Record(ctx context.Context, stats map[string]map[string]map[string]int) {
	for ledger, accounts := range stats {
		for account, assets := range accounts {
			for asset, count := range assets {
				o.metric.Record(ctx, int64(count), metric.WithAttributes(
					attribute.String("ledger", ledger),
					attribute.String("account", account),
					attribute.String("asset", asset),
				))
			}
		}
	}
}

func newOtlpRecorder(metric metric.Int64Histogram) Recorder {
	return &otlpRecorder{
		metric: metric,
	}
}

type accountsVolumesMonitor struct {
	recorder Recorder
}

func (a accountsVolumesMonitor) Accept(ctx context.Context, locks []lockmonitor.Lock) {

	filteredLocks := collectionutils.Filter(locks, func(l lockmonitor.Lock) bool {
		blockedStatement, err := l.BlockedStatement.GetParsedResult()
		if err != nil {
			return false // Skip locks with unparsable statements
		}

		if !a.isAccountsVolumesUpdate(blockedStatement) {
			return false // Skip locks that are not related to accounts volumes
		}

		currentStatementInBlockingProcess, err := l.CurrentStatementInBlockingProcess.GetParsedResult()
		if err != nil {
			return false
		}

		if !a.isAccountsVolumesUpdate(currentStatementInBlockingProcess) {
			return false // Skip locks that are not related to accounts volumes
		}

		return true
	})

	computeLockRef := func(lock lockmonitor.Lock) uint16 {
		if lock.BlockedPID < lock.BlockingPID {
			return uint16(lock.BlockedPID)<<8 | uint16(lock.BlockingPID)
		}
		return uint16(lock.BlockingPID)<<8 | uint16(lock.BlockedPID)
	}

	visited := make(map[uint16]any)

	// ledger/account/asset/counter
	stats := make(map[string]map[string]map[string]int)
	for _, lock := range filteredLocks {

		lockRef := computeLockRef(lock)

		_, ok := visited[lockRef]
		if ok {
			continue // Skip already visited
		}

		blockedStatementAccounts := a.findInvolvedAccountsAndAssets(lock.BlockedStatement)
		blockingStatementAccounts := a.findInvolvedAccountsAndAssets(lock.CurrentStatementInBlockingProcess)

		for ledger := range blockedStatementAccounts {
			if _, ok := blockingStatementAccounts[ledger]; !ok {
				continue
			}

			for account, assets := range intersect(
				blockedStatementAccounts[ledger],
				blockingStatementAccounts[ledger],
			) {
				if _, ok := stats[ledger]; !ok {
					stats[ledger] = make(map[string]map[string]int)
				}
				if _, ok := stats[ledger][account]; !ok {
					stats[ledger][account] = make(map[string]int)
				}
				for asset := range assets {
					stats[ledger][account][asset]++
				}
			}
		}

		visited[lockRef] = struct{}{}
	}

	a.recorder.Record(ctx, stats)
}

func (a accountsVolumesMonitor) findInvolvedAccountsAndAssets(statement lockmonitor.Statement) map[string]map[string]map[string]struct{} {

	ret := make(map[string]map[string]map[string]struct{})
	for _, exprs := range statement.
		MustParseResult()[0].
		AST.(*tree.Insert).
		Rows.
		Select.(*tree.ValuesClause).
		Rows {
		address := exprs[0].(*tree.StrVal).RawString()
		asset := exprs[1].(*tree.StrVal).RawString()
		ledger := exprs[4].(*tree.StrVal).RawString()

		if _, ok := ret[ledger]; !ok {
			ret[ledger] = make(map[string]map[string]struct{})
		}
		if _, ok := ret[ledger][address]; !ok {
			ret[ledger][address] = make(map[string]struct{})
		}

		ret[ledger][address][asset] = struct{}{}
	}

	return ret
}

func (a accountsVolumesMonitor) isAccountsVolumesUpdate(parseResult parser.Statements) bool {
	if len(parseResult) != 1 {
		return false
	}
	stmt := parseResult[0]

	insert, ok := stmt.AST.(*tree.Insert)
	if !ok {
		return false
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	insert.Table.Format(fmtCtx)
	if !strings.HasSuffix(fmtCtx.String(), ".accounts_volumes") { // Representation include the schema name we don't need at this point
		return false
	}

	return true
}

var _ lockmonitor.Monitor = (*accountsVolumesMonitor)(nil)

func NewAccountsVolumesMonitor(recorder Recorder) lockmonitor.Monitor {
	return &accountsVolumesMonitor{
		recorder: recorder,
	}
}

func intersect(entry1 map[string]map[string]struct{}, entry2 map[string]map[string]struct{}) map[string]map[string]struct{} {
	ret := make(map[string]map[string]struct{})
	for key1, value1 := range entry1 {
		if value2, ok := entry2[key1]; ok {
			// If the key exists in both maps, find the intersection of their values
			intersection := make(map[string]struct{})
			for asset := range value1 {
				if _, exists := value2[asset]; exists {
					intersection[asset] = struct{}{}
				}
			}
			if len(intersection) > 0 {
				ret[key1] = intersection
			}
		}
	}

	return ret
}
