package balancehistoryarchive

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicArchiveJSONUsesCamelCase(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(struct {
		Config Config     `json:"config"`
		Ref    Ref        `json:"ref"`
		Stats  CacheStats `json:"stats"`
	}{
		Config: Config{BaseBucketID: "bucket", OwnerID: "node-1", CacheDir: "cache", CacheMaxBytes: 42},
		Ref:    Ref{Version: 1, Size: 23, RecordCount: 2},
		Stats:  CacheStats{Bytes: 11, Entries: 3, Leases: 1},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{
			"config":{"baseBucketId":"bucket","ownerId":"node-1","cacheDir":"cache","cacheMaxBytes":42},
		"ref":{"version":1,"sha256":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"size":23,"recordCount":2},
		"stats":{"bytes":11,"entries":3,"leases":1}
	}`, string(encoded))
}
