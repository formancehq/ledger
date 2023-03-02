package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func Test_ScriptCommands(t *testing.T) {

	db := pgtesting.NewPostgresDatabase(t)

	ledger := uuid.NewString()
	viper.Set("name", ledger)
	viper.Set(storageDriverFlag, "postgres")
	viper.Set(storagePostgresConnectionStringFlag, db.ConnString())
	require.NoError(t, NewStorageInit().Execute())

	d1 := []byte(`
		send [EUR 1] (
			source = @world
			destination = @alice
		)`)
	path := filepath.Join(os.TempDir(), "script")
	require.NoError(t, os.WriteFile(path, d1, 0644))

	httpServer := httptest.NewServer(http.HandlerFunc(scriptSuccessHandler))
	defer func() {
		httpServer.CloseClientConnections()
		httpServer.Close()
	}()

	tests := map[string]struct {
		args  []string
		flags map[string]any
		want  error
	}{
		"not enough args": {args: []string{path}, flags: map[string]any{}, want: errors.New("accepts 2 arg(s), received 1")},
		"success":         {args: []string{ledger, path}, flags: map[string]any{bindFlag: httpServer.URL[7:]}, want: nil},
		"preview":         {args: []string{ledger, path}, flags: map[string]any{previewFlag: true}, want: nil},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for i, f := range tc.flags {
				viper.Set(i, f)
			}
			cmd := NewScriptExec()
			cmd.SetArgs(tc.args)
			got := cmd.Execute()
			if tc.want != nil {
				if got == nil {
					t.Fatalf("an error is expected, but got nil")
				}
				diff := cmp.Diff(tc.want.Error(), got.Error())
				if diff != "" {
					t.Fatalf(diff)
				}
			}
		})
	}
}

func scriptSuccessHandler(w http.ResponseWriter, _ *http.Request) {
	resp := controllers.ScriptResponse{
		ErrorResponse: api.ErrorResponse{},
		Transaction: &core.ExpandedTransaction{
			Transaction: core.Transaction{
				TransactionData: core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(1),
							Asset:       "EUR",
						},
					},
					Timestamp: time.Now(),
				},
			},
			PreCommitVolumes:  nil,
			PostCommitVolumes: nil,
		},
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		fmt.Printf("ERR:%s\n", err)
	}
}
