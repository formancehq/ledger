package storage

import (
	"io"

	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"

	"github.com/spf13/viper"
)

const (
	StoreWorkerMaxPendingSize           = "store-worker-max-pending-size"
	StoreWorkerMaxWriteChanSize         = "store-worker-max-write-chan-size"
	StoragePostgresConnectionStringFlag = "storage-postgres-conn-string"
	StoragePostgresMaxIdleConnsFlag     = "storage-postgres-max-idle-conns"
	StoragePostgresConnMaxIdleTimeFlag  = "storage-postgres-conn-max-idle-time"
	StoragePostgresMaxOpenConns         = "storage-postgres-max-open-conns"
)

func ConnectionOptionsFromFlags(v *viper.Viper, output io.Writer, debug bool) bunconnect.ConnectionOptions {
	return bunconnect.ConnectionOptions{
		DatabaseSourceName: v.GetString(StoragePostgresConnectionStringFlag),
		Debug:              debug,
		Writer:             output,
		MaxIdleConns:       v.GetInt(StoragePostgresMaxIdleConnsFlag),
		ConnMaxIdleTime:    v.GetDuration(StoragePostgresConnMaxIdleTimeFlag),
		MaxOpenConns:       v.GetInt(StoragePostgresMaxOpenConns),
	}
}
