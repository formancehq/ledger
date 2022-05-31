package controllers_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestMapping(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				m := core.Mapping{
					Contracts: []core.Contract{
						{
							Expr: &core.ExprGt{
								Op1: core.VariableExpr{Name: "balance"},
								Op2: core.ConstantExpr{Value: float64(0)},
							},
							Account: "*",
						},
					},
				}
				rsp := internal.SaveMapping(t, h, m)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.LoadMapping(h)
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				m2 := core.Mapping{}
				internal.DecodeSingleResponse(t, rsp.Body, &m2)

				assert.EqualValues(t, m, m2)
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

				m := core.Mapping{}
				internal.DecodeSingleResponse(t, rsp.Body, &m)

				assert.EqualValues(t, core.Mapping{}, m)
				return nil
			},
		})
	}))
}
