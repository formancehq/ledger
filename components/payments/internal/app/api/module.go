package api

import (
	"context"
	"encoding/json"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/logging"

	"github.com/formancehq/payments/internal/app/connectors/bankingcircle"
	"github.com/formancehq/payments/internal/app/connectors/currencycloud"

	"github.com/formancehq/go-libs/auth"
	"github.com/formancehq/go-libs/oauth2/oauth2introspect"
	"github.com/formancehq/go-libs/otlp"
	"github.com/formancehq/payments/internal/app/connectors/dummypay"
	"github.com/formancehq/payments/internal/app/connectors/modulr"
	"github.com/formancehq/payments/internal/app/connectors/stripe"
	"github.com/formancehq/payments/internal/app/connectors/wise"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

//nolint:gosec // false positive
const (
	otelTracesFlag                  = "otel-traces"
	authBasicEnabledFlag            = "auth-basic-enabled"
	authBasicCredentialsFlag        = "auth-basic-credentials"
	authBearerEnabledFlag           = "auth-bearer-enabled"
	authBearerIntrospectURLFlag     = "auth-bearer-introspect-url"
	authBearerAudienceFlag          = "auth-bearer-audience"
	authBearerAudiencesWildcardFlag = "auth-bearer-audiences-wildcard"

	serviceName = "Payments"
)

func HTTPModule(serviceInfo api.ServiceInfo) fx.Option {
	return fx.Options(
		fx.Invoke(func(m *mux.Router, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						//nolint:gomnd // allow timeout values
						srv := &http.Server{
							Handler:      m,
							Addr:         "0.0.0.0:8080",
							WriteTimeout: 15 * time.Second,
							ReadTimeout:  15 * time.Second,
						}

						err := srv.ListenAndServe()
						if err != nil {
							panic(err)
						}
					}()

					return nil
				},
			})
		}),
		fx.Supply(serviceInfo),
		fx.Provide(fx.Annotate(httpRouter, fx.ParamTags(``, ``, `group:"connectorHandlers"`))),
		addConnector[dummypay.Config](dummypay.NewLoader()),
		addConnector[modulr.Config](modulr.NewLoader()),
		addConnector[stripe.Config](stripe.NewLoader()),
		addConnector[wise.Config](wise.NewLoader()),
		addConnector[currencycloud.Config](currencycloud.NewLoader()),
		addConnector[bankingcircle.Config](bankingcircle.NewLoader()),
	)
}

func httpRecoveryFunc(ctx context.Context, e interface{}) {
	if viper.GetBool(otelTracesFlag) {
		otlp.RecordAsError(ctx, e)
	} else {
		logrus.Errorln(e)
		debug.PrintStack()
	}
}

func httpCorsHandler() func(http.Handler) http.Handler {
	return cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut},
		AllowCredentials: true,
	}).Handler
}

func httpServeFunc(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		handler.ServeHTTP(w, r)
	})
}

func sharedAuthMethods() []auth.Method {
	methods := make([]auth.Method, 0)

	if viper.GetBool(authBasicEnabledFlag) {
		credentials := auth.Credentials{}

		for _, kv := range viper.GetStringSlice(authBasicCredentialsFlag) {
			parts := strings.SplitN(kv, ":", 2)
			credentials[parts[0]] = auth.Credential{
				Password: parts[1],
			}
		}

		methods = append(methods, auth.NewHTTPBasicMethod(credentials))
	}

	if viper.GetBool(authBearerEnabledFlag) {
		methods = append(methods, auth.NewHttpBearerMethod(
			auth.NewIntrospectionValidator(
				oauth2introspect.NewIntrospecter(viper.GetString(authBearerIntrospectURLFlag)),
				viper.GetBool(authBearerAudiencesWildcardFlag),
				auth.AudienceIn(viper.GetStringSlice(authBearerAudienceFlag)...),
			),
		))
	}

	return methods
}

func handleServerError(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	logging.GetLogger(r.Context()).Error(err)
	// TODO: Opentracing
	err = json.NewEncoder(w).Encode(api.ErrorResponse{
		ErrorCode:    "INTERNAL",
		ErrorMessage: err.Error(),
	})
	if err != nil {
		panic(err)
	}
}

func handleValidationError(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusBadRequest)
	logging.GetLogger(r.Context()).Error(err)
	// TODO: Opentracing
	err = json.NewEncoder(w).Encode(api.ErrorResponse{
		ErrorCode:    "VALIDATION",
		ErrorMessage: err.Error(),
	})
	if err != nil {
		panic(err)
	}
}

func pageSizeQueryParam(r *http.Request) (int, error) {
	if value := r.URL.Query().Get("pageSize"); value != "" {
		ret, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return 0, err
		}

		return int(ret), nil
	}

	return 0, nil
}
