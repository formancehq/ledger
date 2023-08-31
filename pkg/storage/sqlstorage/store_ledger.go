package sqlstorage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bits-and-blooms/bloom"
	"github.com/formancehq/stack/libs/go-libs/logging"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

type Store struct {
	executorProvider func(ctx context.Context) (executor, error)
	schema           Schema
	onClose          func(ctx context.Context) error
	onDelete         func(ctx context.Context) error
	lastLog          *core.Log
	lastTx           *core.ExpandedTransaction
	bloom            *bloom.BloomFilter
	cache            *cache.Cache
}

func (s *Store) error(err error) error {
	if err == nil {
		return nil
	}
	return errorFromFlavor(Flavor(s.schema.Flavor()), err)
}

func (s *Store) Schema() Schema {
	return s.schema
}

func (s *Store) Name() string {
	return s.schema.Name()
}

func (s *Store) Delete(ctx context.Context) error {
	if err := s.schema.Delete(ctx); err != nil {
		return err
	}
	return errors.Wrap(s.onDelete(ctx), "deleting ledger store")
}

func (s *Store) Initialize(ctx context.Context) (bool, error) {
	logging.FromContext(ctx).Debug("Initialize store")

	migrations, err := CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return false, err
	}

	modified, err := Migrate(ctx, s.schema, migrations...)
	if err != nil {
		return modified, err
	}

	hasMore := true
	q := ledger.NewAccountsQuery()
	for hasMore {
		addresses, err := s.GetAccountAddresses(ctx, *q)
		if err != nil {
			return modified, err
		}

		for _, address := range addresses.Data {
			fmt.Println("ADDING ADDRESS", string(address))
			s.bloom.AddString(string(address))
		}

		hasMore = addresses.HasMore

		if hasMore {
			res, err := base64.RawURLEncoding.DecodeString(addresses.Next)
			if err != nil {
				return modified, err
			}

			token := AccPaginationToken{}
			if err := json.Unmarshal(res, &token); err != nil {
				return modified, err
			}

			q = q.
				WithOffset(token.Offset).
				WithAfterAddress(token.AfterAddress).
				WithAddressFilter(token.AddressRegexpFilter).
				WithBalanceFilter(token.BalanceFilter).
				WithBalanceOperatorFilter(token.BalanceOperatorFilter).
				WithMetadataFilter(token.MetadataFilter).
				WithPageSize(token.PageSize)
		}
	}

	return modified, err
}

func (s *Store) Close(ctx context.Context) error {
	return s.onClose(ctx)
}

func NewStore(schema Schema, executorProvider func(ctx context.Context) (executor, error),
	onClose, onDelete func(ctx context.Context) error) *Store {

	const (
		bloomFilterSizeEnvVar      = "NUMARY_BLOOM_FILTER_SIZE"
		bloomFilterErrorRateEnvVar = "NUMARY_BLOOM_FILTER_ERROR_RATE"
	)

	var (
		bloomSize      uint64 = 100000
		bloomErrorRate        = 0.01
		err            error
	)
	if bloomSizeFromEnv := os.Getenv(bloomFilterSizeEnvVar); bloomSizeFromEnv != "" {
		bloomSize, err = strconv.ParseUint(bloomSizeFromEnv, 10, 64)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprint("Parsing", bloomFilterSizeEnvVar, "env var")))
		}
	}
	if bloomErrorRateFromEnv := os.Getenv(bloomFilterErrorRateEnvVar); bloomErrorRateFromEnv != "" {
		bloomErrorRate, err = strconv.ParseFloat(bloomErrorRateFromEnv, 64)
		if err != nil {
			panic(errors.Wrap(err, fmt.Sprint("Parsing", bloomFilterErrorRateEnvVar, "env var")))
		}
	}

	return &Store{
		executorProvider: executorProvider,
		schema:           schema,
		onClose:          onClose,
		onDelete:         onDelete,
		bloom:            bloom.NewWithEstimates(uint(bloomSize), bloomErrorRate),
		cache:            cache.New(5*time.Minute, 10*time.Minute),
	}
}

var _ ledger.Store = &Store{}
