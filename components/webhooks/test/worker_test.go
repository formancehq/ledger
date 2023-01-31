package test_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/formancehq/webhooks/cmd/flag"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/kafka"
	"github.com/formancehq/webhooks/pkg/security"
	"github.com/formancehq/webhooks/pkg/server"
	"github.com/formancehq/webhooks/pkg/worker"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestWorkerMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sqldb := sql.OpenDB(
		pgdriver.NewConnector(
			pgdriver.WithDSN(viper.GetString(flag.StoragePostgresConnString))))
	db := bun.NewDB(sqldb, pgdialect.New())
	defer db.Close()

	require.NoError(t, db.Ping())

	// Cleanup tables
	require.NoError(t, db.ResetModel(ctx, (*webhooks.Config)(nil)))

	// New test server with success handler
	httpServerSuccess := httptest.NewServer(http.HandlerFunc(webhooksSuccessHandler))
	defer func() {
		httpServerSuccess.CloseClientConnections()
		httpServerSuccess.Close()
	}()

	// New test server with fail handler
	httpServerFail := httptest.NewServer(http.HandlerFunc(webhooksFailHandler))
	defer func() {
		httpServerFail.CloseClientConnections()
		httpServerFail.Close()
	}()

	serverApp := fxtest.New(t,
		fx.Supply(httpServerSuccess.Client()),
		server.StartModule(
			viper.GetString(flag.HttpBindAddressServer)))
	require.NoError(t, serverApp.Start(context.Background()))

	cfgSuccess := webhooks.ConfigUser{
		Endpoint:   httpServerSuccess.URL,
		Secret:     secret,
		EventTypes: []string{"unknown", fmt.Sprintf("%s.%s", app1, type1)},
	}
	require.NoError(t, cfgSuccess.Validate())

	cfgFail := webhooks.ConfigUser{
		Endpoint:   httpServerFail.URL,
		Secret:     secret,
		EventTypes: []string{"unknown", fmt.Sprintf("%s.%s", app2, type2)},
	}
	require.NoError(t, cfgFail.Validate())

	requestServer(t, http.MethodPost, server.PathConfigs, http.StatusOK, cfgSuccess)
	requestServer(t, http.MethodPost, server.PathConfigs, http.StatusOK, cfgFail)

	t.Run("success", func(t *testing.T) {
		require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

		retrySchedule = []time.Duration{time.Second}
		viper.Set(flag.RetriesSchedule, retrySchedule)

		workerApp := fxtest.New(t,
			fx.Supply(httpServerSuccess.Client()),
			worker.StartModule(
				viper.GetString(flag.HttpBindAddressWorker),
				viper.GetDuration(flag.RetriesCron),
				retrySchedule,
			))
		require.NoError(t, workerApp.Start(context.Background()))

		healthCheckWorker(t)

		expectedSentWebhooks := 1
		kafkaClient, kafkaTopics, err := kafka.NewClient()
		require.NoError(t, err)

		by1, err := json.Marshal(event1)
		require.NoError(t, err)
		by3, err := json.Marshal(event3)
		require.NoError(t, err)

		records := []*kgo.Record{
			{Topic: kafkaTopics[0], Value: by1},
			{Topic: kafkaTopics[0], Value: by3},
		}
		err = kafkaClient.ProduceSync(context.Background(), records...).FirstErr()
		require.NoError(t, err)
		kafkaClient.Close()

		t.Run("webhooks", func(t *testing.T) {
			msgs := 0
			for msgs != expectedSentWebhooks {
				var results []webhooks.Attempt
				require.NoError(t, db.NewSelect().Model(&results).Scan(ctx))
				msgs = len(results)
				if msgs != expectedSentWebhooks {
					time.Sleep(time.Second)
				} else {
					for _, res := range results {
						require.Equal(t, webhooks.StatusAttemptSuccess, res.Status)
						require.Equal(t, 0, res.RetryAttempt)
					}
				}
			}
			time.Sleep(time.Second)
			require.Equal(t, expectedSentWebhooks, msgs)
		})

		require.NoError(t, workerApp.Stop(context.Background()))
	})

	t.Run("failure", func(t *testing.T) {
		require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

		retrySchedule = []time.Duration{time.Second}
		viper.Set(flag.RetriesSchedule, retrySchedule)

		workerApp := fxtest.New(t,
			fx.Supply(httpServerFail.Client()),
			worker.StartModule(
				viper.GetString(flag.HttpBindAddressWorker),
				viper.GetDuration(flag.RetriesCron),
				retrySchedule,
			))
		require.NoError(t, workerApp.Start(context.Background()))

		healthCheckWorker(t)

		expectedSentWebhooks := 1
		kafkaClient, kafkaTopics, err := kafka.NewClient()
		require.NoError(t, err)

		by2, err := json.Marshal(event2)
		require.NoError(t, err)
		by3, err := json.Marshal(event3)
		require.NoError(t, err)

		records := []*kgo.Record{
			{Topic: kafkaTopics[0], Value: by2},
			{Topic: kafkaTopics[0], Value: by3},
		}
		err = kafkaClient.ProduceSync(context.Background(), records...).FirstErr()
		require.NoError(t, err)
		kafkaClient.Close()

		t.Run("webhooks", func(t *testing.T) {
			msgs := 0
			for msgs != expectedSentWebhooks {
				var results []webhooks.Attempt
				require.NoError(t, db.NewSelect().Model(&results).Scan(ctx))
				msgs = len(results)
				if msgs != expectedSentWebhooks {
					time.Sleep(time.Second)
				} else {
					for _, res := range results {
						require.Equal(t, webhooks.StatusAttemptToRetry, res.Status)
						require.Equal(t, 0, res.RetryAttempt)
					}
				}
			}
			time.Sleep(time.Second)
			require.Equal(t, expectedSentWebhooks, msgs)
		})

		require.NoError(t, workerApp.Stop(context.Background()))
	})

	t.Run("disabled config should not receive webhooks", func(t *testing.T) {
		resBody := requestServer(t, http.MethodGet, server.PathConfigs, http.StatusOK)
		cur := decodeCursorResponse[webhooks.Config](t, resBody)
		require.Equal(t, 2, len(cur.Data))
		require.NoError(t, resBody.Close())

		resBody = requestServer(t, http.MethodPut, server.PathConfigs+"/"+cur.Data[1].ID+server.PathDeactivate, http.StatusOK)
		c, ok := decodeSingleResponse[webhooks.Config](t, resBody)
		require.Equal(t, true, ok)
		require.Equal(t, false, c.Active)

		require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

		retrySchedule = []time.Duration{time.Second}
		viper.Set(flag.RetriesSchedule, retrySchedule)

		workerApp := fxtest.New(t,
			fx.Supply(httpServerSuccess.Client()),
			worker.StartModule(
				viper.GetString(flag.HttpBindAddressWorker),
				viper.GetDuration(flag.RetriesCron),
				retrySchedule,
			))
		require.NoError(t, workerApp.Start(context.Background()))

		healthCheckWorker(t)

		kafkaClient, kafkaTopics, err := kafka.NewClient()
		require.NoError(t, err)

		by1, err := json.Marshal(event1)
		require.NoError(t, err)

		records := []*kgo.Record{
			{Topic: kafkaTopics[0], Value: by1},
		}
		err = kafkaClient.ProduceSync(context.Background(), records...).FirstErr()
		require.NoError(t, err)
		kafkaClient.Close()

		time.Sleep(3 * time.Second)

		var results []webhooks.Attempt
		require.NoError(t, db.NewSelect().Model(&results).Scan(ctx))
		require.Equal(t, 0, len(results))

		require.NoError(t, workerApp.Stop(context.Background()))
	})

	require.NoError(t, serverApp.Stop(context.Background()))
}

func TestWorkerRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sqldb := sql.OpenDB(
		pgdriver.NewConnector(
			pgdriver.WithDSN(viper.GetString(flag.StoragePostgresConnString))))
	db := bun.NewDB(sqldb, pgdialect.New())

	require.NoError(t, db.Ping())

	t.Run("1 attempt to retry with success", func(t *testing.T) {
		require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

		// New test server with success handler
		httpServerSuccess := httptest.NewServer(http.HandlerFunc(webhooksSuccessHandler))
		defer func() {
			httpServerSuccess.CloseClientConnections()
			httpServerSuccess.Close()
		}()

		failedAttempt := webhooks.Attempt{
			CreatedAt: time.Now().UTC(),
			ID:        uuid.NewString(),
			WebhookID: uuid.NewString(),
			Config: webhooks.Config{
				ConfigUser: webhooks.ConfigUser{
					Endpoint:   httpServerSuccess.URL,
					Secret:     secret,
					EventTypes: []string{type1},
				},
				ID:        uuid.NewString(),
				Active:    true,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
			Payload:        fmt.Sprintf("{\"type\":\"%s\"}", type1),
			StatusCode:     http.StatusNotFound,
			Status:         webhooks.StatusAttemptToRetry,
			RetryAttempt:   0,
			NextRetryAfter: time.Now().UTC(),
		}

		_, err := db.NewInsert().Model(&failedAttempt).Exec(ctx)
		require.NoError(t, err)

		retrySchedule = []time.Duration{time.Second, time.Second, time.Second}
		viper.Set(flag.RetriesSchedule, retrySchedule)

		workerApp := fxtest.New(t,
			fx.Supply(httpServerSuccess.Client()),
			worker.StartModule(
				viper.GetString(flag.HttpBindAddressWorker),
				viper.GetDuration(flag.RetriesCron),
				retrySchedule))
		require.NoError(t, workerApp.Start(context.Background()))

		healthCheckWorker(t)

		expectedAttempts := 2

		attempts := 0
		for attempts != expectedAttempts {
			var results []webhooks.Attempt
			require.NoError(t, db.NewSelect().Model(&results).Order("created_at DESC").Scan(ctx))
			attempts = len(results)
			if attempts != expectedAttempts {
				time.Sleep(time.Second)
			} else {
				// First attempt should be successful
				require.Equal(t, webhooks.StatusAttemptSuccess, results[0].Status)
				require.Equal(t, expectedAttempts-1, results[0].RetryAttempt)
			}
		}
		time.Sleep(time.Second)
		require.Equal(t, expectedAttempts, attempts)

		require.NoError(t, workerApp.Stop(context.Background()))
	})

	t.Run("retrying an attempt until failed at the end of the schedule", func(t *testing.T) {
		require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

		// New test server with fail handler
		httpServerFail := httptest.NewServer(http.HandlerFunc(webhooksFailHandler))
		defer func() {
			httpServerFail.CloseClientConnections()
			httpServerFail.Close()
		}()

		failedAttempt := webhooks.Attempt{
			CreatedAt: time.Now().UTC(),
			ID:        uuid.NewString(),
			WebhookID: uuid.NewString(),
			Config: webhooks.Config{
				ConfigUser: webhooks.ConfigUser{
					Endpoint:   httpServerFail.URL,
					Secret:     secret,
					EventTypes: []string{type1},
				},
				ID:        uuid.NewString(),
				Active:    true,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
			Payload:        fmt.Sprintf("{\"type\":\"%s\"}", type1),
			StatusCode:     http.StatusNotFound,
			Status:         webhooks.StatusAttemptToRetry,
			RetryAttempt:   0,
			NextRetryAfter: time.Now().UTC(),
		}

		_, err := db.NewInsert().Model(&failedAttempt).Exec(ctx)
		require.NoError(t, err)

		retrySchedule = []time.Duration{time.Second, time.Second, time.Second}
		viper.Set(flag.RetriesSchedule, retrySchedule)

		workerApp := fxtest.New(t,
			fx.Supply(httpServerFail.Client()),
			worker.StartModule(
				viper.GetString(flag.HttpBindAddressWorker),
				viper.GetDuration(flag.RetriesCron),
				retrySchedule))
		require.NoError(t, workerApp.Start(context.Background()))

		healthCheckWorker(t)

		expectedAttempts := 4

		attempts := 0
		for attempts != expectedAttempts {
			var results []webhooks.Attempt
			require.NoError(t, db.NewSelect().Model(&results).Order("created_at DESC").Scan(ctx))
			attempts = len(results)
			if attempts != expectedAttempts {
				time.Sleep(time.Second)
			} else {
				// First attempt should be failed
				require.Equal(t, webhooks.StatusAttemptFailed, results[0].Status)
				require.Equal(t, expectedAttempts-1, results[0].RetryAttempt)
			}
		}
		time.Sleep(time.Second)
		require.Equal(t, expectedAttempts, attempts)

		require.NoError(t, workerApp.Stop(context.Background()))
	})

	t.Run("retry long schedule", func(t *testing.T) {
		retrySchedule = []time.Duration{time.Hour}
		viper.Set(flag.RetriesSchedule, retrySchedule)

		require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

		// New test server with fail handler
		httpServerFail := httptest.NewServer(http.HandlerFunc(webhooksFailHandler))
		defer func() {
			httpServerFail.CloseClientConnections()
			httpServerFail.Close()
		}()

		failedAttempt := webhooks.Attempt{
			CreatedAt: time.Now().UTC(),
			ID:        uuid.NewString(),
			WebhookID: uuid.NewString(),
			Config: webhooks.Config{
				ConfigUser: webhooks.ConfigUser{
					Endpoint:   httpServerFail.URL,
					Secret:     secret,
					EventTypes: []string{type1},
				},
				ID:        uuid.NewString(),
				Active:    true,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
			Payload:        fmt.Sprintf("{\"type\":\"%s\"}", type1),
			StatusCode:     http.StatusNotFound,
			Status:         webhooks.StatusAttemptToRetry,
			RetryAttempt:   0,
			NextRetryAfter: time.Now().UTC(),
		}

		_, err := db.NewInsert().Model(&failedAttempt).Exec(ctx)
		require.NoError(t, err)

		workerApp := fxtest.New(t,
			fx.Supply(httpServerFail.Client()),
			worker.StartModule(
				viper.GetString(flag.HttpBindAddressWorker),
				viper.GetDuration(flag.RetriesCron),
				retrySchedule))
		require.NoError(t, workerApp.Start(context.Background()))

		healthCheckWorker(t)

		time.Sleep(3 * time.Second)

		var results []webhooks.Attempt
		require.NoError(t, db.NewSelect().Model(&results).Scan(ctx))
		attempts := len(results)
		require.Equal(t, 2, attempts)
		require.Equal(t, webhooks.StatusAttemptFailed, results[0].Status)
		require.Equal(t, webhooks.StatusAttemptFailed, results[1].Status)

		require.NoError(t, workerApp.Stop(context.Background()))
	})
}

func webhooksSuccessHandler(w http.ResponseWriter, r *http.Request) {
	id := r.Header.Get("formance-webhook-id")
	ts := r.Header.Get("formance-webhook-timestamp")
	signatures := r.Header.Get("formance-webhook-signature")
	timeInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ok, err := security.Verify(signatures, id, timeInt, secret, payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, "security.Verify NOK", http.StatusBadRequest)
		return
	}

	_, _ = fmt.Fprintf(w, "WEBHOOK RECEIVED: MOCK OK RESPONSE\n")
	return
}

func webhooksFailHandler(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, "WEBHOOKS RECEIVED: MOCK ERROR RESPONSE", http.StatusNotFound)
	return
}
