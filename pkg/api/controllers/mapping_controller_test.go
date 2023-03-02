package controllers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/formancehq/ledger/pkg/api"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestMapping(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				m := core.Mapping{
					Contracts: []core.Contract{
						{
							Name:    "default",
							Account: "*",
							Expr: &core.ExprGt{
								Op1: core.VariableExpr{
									Name: "balance",
								},
								Op2: core.ConstantExpr{
									Value: 0,
								},
							},
						},
					},
				}
				rsp := internal.SaveMapping(t, h, m)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.LoadMapping(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				m2, _ := internal.DecodeSingleResponse[core.Mapping](t, rsp.Body)

				data, err := json.Marshal(m)
				require.NoError(t, err)
				m1AsMap := make(map[string]any)
				require.NoError(t, json.Unmarshal(data, &m1AsMap))

				data, err = json.Marshal(m2)
				require.NoError(t, err)
				m2AsMap := make(map[string]any)
				require.NoError(t, json.Unmarshal(data, &m2AsMap))

				assert.EqualValues(t, m1AsMap, m2AsMap)
				return nil
			},
		})
	}))
}

func TestLoadEmptyMapping(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.LoadMapping(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				m, _ := internal.DecodeSingleResponse[core.Mapping](t, rsp.Body)

				assert.EqualValues(t, core.Mapping{}, m)
				return nil
			},
		})
	}))
}
