//go:build it

package restore_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/formancehq/ledger/internal"
	v2 "github.com/formancehq/ledger/internal/api/v2"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"
)

const fundingScript = `send [USD/2 100] (
 source = @world
 destination = @alice
)`

func TestSEC79Restore(t *testing.T) {
	server := newServer(t)
	t.Run("real export restores", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		target, l := f.ledger(t)
		require.NoError(t, f.restore(target, logs))
		require.Equal(t, 1, f.count(t, l, "transactions"))
		restored := f.export(t, target)
		require.Equal(t, logs, restored)
	})
	t.Run("nonempty initializing ledger accepts the next batch", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		target, l := f.ledger(t)
		require.NoError(t, f.restore(target, logs[:1]))
		require.NoError(t, f.restore(target, logs[1:]))
		require.Equal(t, 2, f.count(t, l, "logs"))
		require.Equal(t, logs, f.export(t, target))
	})
	t.Run("hash failure rolls back earlier log", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		broken := append([]ledger.Log(nil), logs...)
		broken[1].Hash = []byte("invalid")
		target, l := f.ledger(t)
		err := f.restore(target, broken)
		require.Error(t, err)
		require.NotContains(t, err.Error(), base64.StdEncoding.EncodeToString(logs[1].Hash))
		require.Zero(t, f.count(t, l, "logs"))
		require.Zero(t, f.count(t, l, "transactions"))
		require.Zero(t, f.count(t, l, "accounts_volumes"))
		require.NoError(t, f.restore(target, logs))
	})
	t.Run("oversized log identifier rejected without poisoning ledger", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		logs[0].ID = pointer.For(uint64(math.MaxInt64))
		// The first-log hash intentionally does not cover its ID. This is a valid
		// caller-calculated content hash, not proof that an ID is sequence-safe.
		logs[0].Hash = nil
		logs[0].ComputeHash(nil)
		target, l := f.ledger(t)
		require.Error(t, f.restore(target, logs))
		require.Zero(t, f.count(t, l, "logs"))
		f.create(t, target, fundingScript)
	})
}

// fctl sends at most 100 newline-delimited logs per HTTP request. A failed
// request must preserve earlier committed batches and permit resuming after them.
func TestSEC79ChunkedHTTPResume(t *testing.T) {
	f := newFixture(t, newServer(t))
	source, _ := f.ledger(t)
	for range 205 {
		f.create(t, source, fundingScript)
	}
	logs := f.export(t, source)
	target, item := f.ledger(t)
	router := v2.NewRouter(f.system, jwt.NewNoAuth(), "test")
	encode := func(batch []ledger.Log) []byte {
		var body bytes.Buffer
		for _, log := range batch {
			require.NoError(t, json.NewEncoder(&body).Encode(log))
		}
		return body.Bytes()
	}
	post := func(body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/"+item.Name+"/logs/import", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec
	}
	accept := func(batch []ledger.Log) {
		code := post(encode(batch)).Code
		require.GreaterOrEqual(t, code, 200)
		require.Less(t, code, 300)
	}
	accept(logs[:100])
	require.Equal(t, 100, f.count(t, item, "logs"))
	// This ID is above the first stored ID but below the actual maximum.
	// Comparing with an arbitrary/ascending page instead of max(id) is unsafe.
	rejected := post(encode(logs[1:2]))
	require.Equal(t, http.StatusBadRequest, rejected.Code)
	var response struct {
		ErrorCode string `json:"errorCode"`
	}
	require.NoError(t, json.Unmarshal(rejected.Body.Bytes(), &response))
	require.Equal(t, "IMPORT", response.ErrorCode)
	require.Equal(t, 100, f.count(t, item, "logs"))
	broken := append(encode(logs[100:200]), []byte("{\n")...)
	require.Equal(t, http.StatusInternalServerError, post(broken).Code)
	require.Equal(t, 100, f.count(t, item, "logs"))
	require.Equal(t, 100, f.count(t, item, "transactions"))
	require.Equal(t, logs[:100], f.export(t, target))
	accept(logs[100:200])
	accept(logs[200:])
	require.Equal(t, logs, f.export(t, target))
	next := f.create(t, target, fundingScript)
	require.Equal(t, *logs[len(logs)-1].ID+1, *next.ID)
	// A first live write closes restoration admission even for a fresh batch.
	require.ErrorContains(t, f.restore(target, logs[:1]), "initializing")
}

// A missing ID below max(id) must be refused even when uniqueness and hash
// checking cannot reject it. This distinguishes the maximum from an ASC page.
func TestSEC79RejectsStoredGapHTTP(t *testing.T) {
	f := newFixture(t, newServer(t))
	source, _ := f.ledger(t)
	for range 3 {
		f.create(t, source, fundingScript)
	}
	logs := f.export(t, source)
	cfg := ledger.NewDefaultConfiguration()
	cfg.Bucket = "_default"
	cfg.Features = features.DefaultFeatures.With(features.FeatureHashLogs, "DISABLED")
	item, err := ledger.New("sec79_gap", cfg)
	require.NoError(t, err)
	_, err = f.driver.CreateLedger(f.ctx, item)
	require.NoError(t, err)
	target, err := f.system.GetLedgerController(f.ctx, item.Name)
	require.NoError(t, err)
	prefix := []ledger.Log{logs[0], logs[2]}
	rehash(prefix)
	require.NoError(t, f.restore(target, prefix))
	body, err := json.Marshal(logs[1])
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	v2.NewRouter(f.system, jwt.NewNoAuth(), "test").ServeHTTP(rec,
		httptest.NewRequest(http.MethodPost, "/"+item.Name+"/logs/import", bytes.NewReader(body)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	var response struct {
		ErrorCode string `json:"errorCode"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "IMPORT", response.ErrorCode)
	require.Equal(t, 2, f.count(t, item, "logs"))
	require.Equal(t, 2, f.count(t, item, "transactions"))
}

func TestSEC79RestoreCompatibilityAndInvariants(t *testing.T) {
	server := newServer(t)
	for _, lastID := range []uint64{math.MaxInt64 - 2, math.MaxInt64 - 1} {
		t.Run(fmt.Sprintf("ID %d leaves its native successors available", lastID), func(t *testing.T) {
			f := newFixture(t, server)
			source, _ := f.ledger(t)
			f.create(t, source, fundingScript)
			logs := f.export(t, source)
			logs[0].ID = pointer.For(lastID)
			payload := logs[0].Data.(ledger.CreatedTransaction)
			payload.Transaction.ID = pointer.For(lastID)
			logs[0].Data = payload
			rehash(logs)
			target, _ := f.ledger(t)
			require.NoError(t, f.restore(target, logs))
			for id := lastID + 1; id <= math.MaxInt64; id++ {
				next := f.create(t, target, fundingScript)
				require.Equal(t, id, *next.ID)
				require.Equal(t, id, *next.Data.(ledger.CreatedTransaction).Transaction.ID)
			}
			_, _, _, err := target.CreateTransaction(f.ctx, transactionParameters(fundingScript))
			require.ErrorContains(t, err, "reached maximum value")
		})
	}
	t.Run("sequence gaps in real exports remain supported", func(t *testing.T) {
		f := newFixture(t, server)
		source, l := f.ledger(t)
		f.create(t, source, fundingScript)
		_, err := f.db.NewRaw(`select setval(?, 7, false), setval(?, 9, false)`, fmt.Sprintf(`"%s"."transaction_id_%d"`, l.Bucket, l.ID), fmt.Sprintf(`"%s"."log_id_%d"`, l.Bucket, l.ID)).Exec(f.ctx)
		require.NoError(t, err)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		require.EqualValues(t, 9, *logs[1].ID)
		target, _ := f.ledger(t)
		require.NoError(t, f.restore(target, logs))
		next := f.create(t, target, fundingScript)
		require.EqualValues(t, 10, *next.ID)
		require.EqualValues(t, 8, *next.Data.(ledger.CreatedTransaction).Transaction.ID)
	})
	t.Run("transaction IDs may decrease in log order", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		for i, id := range []uint64{7, 3} {
			payload := logs[i].Data.(ledger.CreatedTransaction)
			payload.Transaction.ID = pointer.For(id)
			logs[i].Data = payload
		}
		rehash(logs)
		target, _ := f.ledger(t)
		require.NoError(t, f.restore(target, logs))
		require.Equal(t, logs, f.export(t, target))
		next := f.create(t, target, fundingScript)
		require.EqualValues(t, 3, *next.ID)
		require.EqualValues(t, 8, *next.Data.(ledger.CreatedTransaction).Transaction.ID)
	})
	t.Run("historical ID zero and zero posting amount remain supported", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		logs[0].ID = pointer.For(uint64(0))
		payload := logs[0].Data.(ledger.CreatedTransaction)
		payload.Transaction.ID = pointer.For(uint64(0))
		payload.Transaction.Postings = append(payload.Transaction.Postings, ledger.NewPosting("world", "alice", "USD/2", big.NewInt(0)))
		logs[0].Data = payload
		rehash(logs)
		target, _ := f.ledger(t)
		require.NoError(t, f.restore(target, logs))
		next := f.create(t, target, fundingScript)
		require.EqualValues(t, 1, *next.ID)
		require.EqualValues(t, 1, *next.Data.(ledger.CreatedTransaction).Transaction.ID)
	})
	t.Run("authorized overdraft export restores", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, `send [USD/2 100] (
 source = @alice allowing unbounded overdraft
 destination = @bob
)`)
		logs := f.export(t, source)
		target, _ := f.ledger(t)
		require.NoError(t, f.restore(target, logs))
		require.Equal(t, logs, f.export(t, target))
	})
	t.Run("valid revert export restores", func(t *testing.T) {
		f := newFixture(t, server)
		logs := f.revertLogs(t)
		target, _ := f.ledger(t)
		require.NoError(t, f.restore(target, logs))
		require.Equal(t, logs, f.export(t, target))
	})
	for _, test := range []struct {
		name   string
		mutate func([]ledger.Log)
	}{
		{"missing log ID", func(logs []ledger.Log) { logs[0].ID = nil }},
		{"log ID maxUint64", func(logs []ledger.Log) { logs[0].ID = pointer.For(uint64(math.MaxUint64)) }},
		{"duplicate log ID", func(logs []ledger.Log) { logs[1].ID = logs[0].ID }},
		{"decreasing log IDs", func(logs []ledger.Log) { logs[0].ID, logs[1].ID = logs[1].ID, logs[0].ID }},
		{"duplicate transaction ID", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertTransaction.ID = p.RevertedTransaction.ID
			logs[1].Data = p
		}},
		{"missing transaction ID", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.ID = nil
			logs[0].Data = p
		}},
		{"transaction ID maxInt64", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.ID = pointer.For(uint64(math.MaxInt64))
			logs[0].Data = p
		}},
		{"empty postings", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.Postings = nil
			logs[0].Data = p
		}},
		{"missing amount", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.Postings[0].Amount = nil
			logs[0].Data = p
		}},
		{"invalid source address", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.Postings[0].Source = "invalid address"
			logs[0].Data = p
		}},
		{"invalid destination address", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.Postings[0].Destination = "invalid address"
			logs[0].Data = p
		}},
		{"invalid asset", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.Postings[0].Asset = "invalid asset"
			logs[0].Data = p
		}},
		{"new transaction already reverted", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.RevertedAt = logs[1].Data.(ledger.RevertedTransaction).RevertedTransaction.RevertedAt
			logs[0].Data = p
		}},
		{"negative posting", func(logs []ledger.Log) {
			p := logs[0].Data.(ledger.CreatedTransaction)
			p.Transaction.Postings[0].Amount = big.NewInt(-1)
			logs[0].Data = p
		}},
		{"unrelated revert", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertTransaction.Postings = ledger.Postings{ledger.NewPosting("world", "mallory", "USD/2", big.NewInt(1000))}
			logs[1].Data = p
		}},
		{"forged original and revert", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertedTransaction.Postings = ledger.Postings{ledger.NewPosting("world", "mallory", "USD/2", big.NewInt(1000))}
			p.RevertTransaction.Postings = p.RevertedTransaction.Postings.Reverse()
			logs[1].Data = p
		}},
		{"forged original timestamp", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertedTransaction.Timestamp = p.RevertTransaction.Timestamp
			logs[1].Data = p
		}},
		{"forged original reference", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertedTransaction.Reference = "different-original-reference"
			logs[1].Data = p
		}},
		{"missing revert target", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertedTransaction.ID = pointer.For(uint64(99))
			logs[1].Data = p
		}},
		{"missing revert timestamp", func(logs []ledger.Log) {
			p := logs[1].Data.(ledger.RevertedTransaction)
			p.RevertedTransaction.RevertedAt = nil
			logs[1].Data = p
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t, server)
			logs := f.revertLogs(t)
			test.mutate(logs)
			rehash(logs)
			target, l := f.ledger(t)
			var err error
			require.NotPanics(t, func() { err = f.restore(target, logs) })
			require.Error(t, err)
			require.Zero(t, f.count(t, l, "logs"))
			require.Zero(t, f.count(t, l, "transactions"))
			f.create(t, target, fundingScript)
		})
	}
	t.Run("second revert of same original rejected atomically", func(t *testing.T) {
		f := newFixture(t, server)
		logs := f.revertLogs(t)
		duplicate := logs[1]
		duplicate.ID = pointer.For(*duplicate.ID + 1)
		p := duplicate.Data.(ledger.RevertedTransaction)
		p.RevertTransaction.ID = pointer.For(*p.RevertTransaction.ID + 1)
		duplicate.Data = p
		logs = append(logs, duplicate)
		rehash(logs)
		target, l := f.ledger(t)
		require.Error(t, f.restore(target, logs))
		require.Zero(t, f.count(t, l, "logs"))
		require.Zero(t, f.count(t, l, "transactions"))
	})
	t.Run("malformed HTTP suffix rolls back the valid prefix", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		_, l := f.ledger(t)
		body, err := json.Marshal(logs[0])
		require.NoError(t, err)
		body = append(body, []byte("\n{")...)
		router := v2.NewRouter(f.system, jwt.NewNoAuth(), "test")
		req := httptest.NewRequest(http.MethodPost, "/"+l.Name+"/logs/import", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Zero(t, f.count(t, l, "logs"))
		require.Zero(t, f.count(t, l, "transactions"))
	})
}

func (f *fixture) revertLogs(t *testing.T) []ledger.Log {
	t.Helper()
	source, _ := f.ledger(t)
	original := f.create(t, source, fundingScript)
	_, _, _, err := source.RevertTransaction(f.ctx, ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{Input: ledgercontroller.RevertTransaction{TransactionID: *original.Data.(ledger.CreatedTransaction).Transaction.ID}})
	require.NoError(t, err)
	return f.export(t, source)
}

func rehash(logs []ledger.Log) {
	var prev *ledger.Log
	for i := range logs {
		logs[i].Hash = nil
		logs[i].ComputeHash(prev)
		prev = &logs[i]
	}
}

func TestSEC79ConcurrentAdmission(t *testing.T) {
	server := newServer(t)
	t.Run("first live write wins admission", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		target, l := f.ledger(t)
		writer, _, err := target.BeginTX(f.ctx, nil)
		require.NoError(t, err)
		defer func() { _ = writer.Rollback(f.ctx) }()
		f.create(t, writer, fundingScript)
		importer, err := f.system.GetLedgerController(f.ctx, l.Name)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- f.restore(importer, logs) }()
		require.Eventually(t, func() bool { return f.advisoryWaiters(t) > 0 }, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, writer.Commit(f.ctx))
		require.Error(t, <-done)
		require.Equal(t, 1, f.count(t, l, "logs"))
		require.Equal(t, 1, f.count(t, l, "transactions"))
	})
	t.Run("import wins and serializes another import and live write", func(t *testing.T) {
		f := newFixture(t, server)
		source, _ := f.ledger(t)
		f.create(t, source, fundingScript)
		logs := f.export(t, source)
		target, l := f.ledger(t)
		stream := make(chan ledger.Log)
		imported := make(chan error, 1)
		go func() { imported <- target.Import(f.ctx, stream) }()
		// Sending on an unbuffered channel can only finish after the importer has
		// acquired its production ledger lock and checked admission.
		stream <- logs[0]
		second, err := f.system.GetLedgerController(f.ctx, l.Name)
		require.NoError(t, err)
		secondDone := make(chan error, 1)
		go func() { secondDone <- f.restore(second, logs) }()
		writer, err := f.system.GetLedgerController(f.ctx, l.Name)
		require.NoError(t, err)
		type writeResult struct {
			log *ledger.Log
			err error
		}
		written := make(chan writeResult, 1)
		go func() {
			log, _, _, err := writer.CreateTransaction(f.ctx, transactionParameters(fundingScript))
			written <- writeResult{log, err}
		}()
		require.Eventually(t, func() bool { return f.advisoryWaiters(t) >= 2 }, 5*time.Second, 10*time.Millisecond)
		close(stream)
		require.NoError(t, <-imported)
		require.Error(t, <-secondDone)
		result := <-written
		require.NoError(t, result.err)
		require.Equal(t, *logs[0].ID+1, *result.log.ID)
		require.Equal(t, 2, f.count(t, l, "logs"))
		require.Equal(t, 2, f.count(t, l, "transactions"))
	})
}

func (f *fixture) advisoryWaiters(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, f.db.NewRaw(`select count(*) from pg_locks where locktype = 'advisory' and not granted and database = (select oid from pg_database where datname = current_database())`).Scan(f.ctx, &n))
	return n
}
