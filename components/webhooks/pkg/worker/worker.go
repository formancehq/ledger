package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/formancehq/go-libs/logging"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/kafka"
	"github.com/formancehq/webhooks/pkg/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Worker struct {
	httpClient *http.Client
	store      storage.Store

	kafkaClient kafka.Client
	kafkaTopics []string

	retriesCron     time.Duration
	retriesSchedule []time.Duration

	stopChan chan chan struct{}
}

func NewWorker(store storage.Store, httpClient *http.Client, retriesCron time.Duration, retriesSchedule []time.Duration) (*Worker, error) {
	kafkaClient, kafkaTopics, err := kafka.NewClient()
	if err != nil {
		return nil, errors.Wrap(err, "kafka.NewClient")
	}

	return &Worker{
		httpClient:      httpClient,
		store:           store,
		kafkaClient:     kafkaClient,
		kafkaTopics:     kafkaTopics,
		retriesCron:     retriesCron,
		retriesSchedule: retriesSchedule,
		stopChan:        make(chan chan struct{}),
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	msgChan := make(chan *kgo.Record)
	errChan := make(chan error)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	go fetchMessages(ctxWithCancel, w.kafkaClient, msgChan, errChan)
	go w.attemptRetries(ctxWithCancel, errChan)

	for {
		select {
		case ch := <-w.stopChan:
			logging.GetLogger(ctx).Debug("worker: received from stopChan")
			close(ch)
			return nil
		case <-ctx.Done():
			logging.GetLogger(ctx).Debugf("worker: context done: %s", ctx.Err())
			return nil
		case err := <-errChan:
			return errors.Wrap(err, "kafka.Worker")
		case msg := <-msgChan:
			ctx = logging.ContextWithLogger(ctx,
				logging.GetLogger(ctx).WithFields(map[string]any{
					"offset": msg.Offset,
				}))
			logging.GetLogger(ctx).WithFields(map[string]any{
				"time":      msg.Timestamp.UTC().Format(time.RFC3339),
				"partition": msg.Partition,
				"headers":   msg.Headers,
			}).Debug("worker: new kafka message fetched")

			w.kafkaClient.PauseFetchTopics(w.kafkaTopics...)

			if err := w.processMessage(ctx, msg.Value); err != nil {
				return errors.Wrap(err, "worker.Worker.processMessage")
			}

			w.kafkaClient.ResumeFetchTopics(w.kafkaTopics...)
		}
	}
}

func (w *Worker) Stop(ctx context.Context) {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		logging.GetLogger(ctx).Debugf("worker stopped: context done: %s", ctx.Err())
		return
	case w.stopChan <- ch:
		select {
		case <-ctx.Done():
			logging.GetLogger(ctx).Debugf("worker stopped via stopChan: context done: %s", ctx.Err())
			return
		case <-ch:
			logging.GetLogger(ctx).Debug("worker stopped via stopChan")
		}
	default:
		logging.GetLogger(ctx).Debug("trying to stop worker: no communication")
	}
}

func fetchMessages(ctx context.Context, kafkaClient kafka.Client, msgChan chan *kgo.Record, errChan chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := kafkaClient.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				logging.GetLogger(ctx).Errorf("POLL: %+v", errs)
				for _, err := range errs {
					select {
					case <-ctx.Done():
						return
					default:
						errChan <- fmt.Errorf(
							"kafka.Client.PollRecords: topic: %s: partition: %d: %w",
							err.Topic, err.Partition, err.Err)
					}
				}
			}

			fetches.EachRecord(func(record *kgo.Record) {
				msgChan <- record
			})
		}
	}
}

func (w *Worker) processMessage(ctx context.Context, msgValue []byte) error {
	var ev webhooks.EventMessage
	if err := json.Unmarshal(msgValue, &ev); err != nil {
		return errors.Wrap(err, "json.Unmarshal event message")
	}

	eventApp := strings.ToLower(ev.App)
	eventType := strings.ToLower(ev.Type)

	if eventApp == "" {
		ev.Type = eventType
	} else {
		ev.Type = strings.Join([]string{eventApp, eventType}, ".")
	}

	filter := map[string]any{
		"event_types": ev.Type,
		"active":      true,
	}
	logging.GetLogger(ctx).Debugf("searching configs with filter: %+v", filter)
	cfgs, err := w.store.FindManyConfigs(ctx, filter)
	if err != nil {
		return errors.Wrap(err, "storage.store.FindManyConfigs")
	}

	for _, cfg := range cfgs {
		logging.GetLogger(ctx).Debugf("found one config: %+v", cfg)
		data, err := json.Marshal(ev)
		if err != nil {
			return errors.Wrap(err, "json.Marshal event message")
		}

		attempt, err := webhooks.MakeAttempt(ctx, w.httpClient, w.retriesSchedule, uuid.NewString(),
			uuid.NewString(), 0, cfg, data, false)
		if err != nil {
			return errors.Wrap(err, "sending webhook")
		}

		if attempt.Status == webhooks.StatusAttemptSuccess {
			logging.GetLogger(ctx).Infof(
				"webhook sent with ID %s to %s of type %s",
				attempt.WebhookID, cfg.Endpoint, ev.Type)
		}

		if err := w.store.InsertOneAttempt(ctx, attempt); err != nil {
			return errors.Wrap(err, "storage.store.InsertOneAttempt")
		}
	}

	return nil
}

var ErrNoAttemptsFound = errors.New("attemptRetries: no attempts found")

func (w *Worker) attemptRetries(ctx context.Context, errChan chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Find all webhookIDs ready to be retried
			webhookIDs, err := w.store.FindWebhookIDsToRetry(ctx)
			if err != nil {
				errChan <- errors.Wrap(err, "storage.Store.FindWebhookIDsToRetry")
				continue
			} else {
				logging.GetLogger(ctx).Debugf(
					"found %d distinct webhookIDs to retry: %+v", len(webhookIDs), webhookIDs)
			}

			for _, webhookID := range webhookIDs {
				atts, err := w.store.FindAttemptsToRetryByWebhookID(ctx, webhookID)
				if err != nil {
					errChan <- errors.Wrap(err, "storage.Store.FindAttemptsToRetryByWebhookID")
					continue
				}
				if len(atts) == 0 {
					errChan <- fmt.Errorf("%w for webhookID: %s", ErrNoAttemptsFound, webhookID)
					continue
				}

				newAttemptNb := atts[0].RetryAttempt + 1
				attempt, err := webhooks.MakeAttempt(ctx, w.httpClient, w.retriesSchedule, uuid.NewString(),
					webhookID, newAttemptNb, atts[0].Config, []byte(atts[0].Payload), false)
				if err != nil {
					errChan <- errors.Wrap(err, "webhooks.MakeAttempt")
					continue
				}

				if err := w.store.InsertOneAttempt(ctx, attempt); err != nil {
					errChan <- errors.Wrap(err, "storage.Store.InsertOneAttempt retried")
					continue
				}

				if _, err := w.store.UpdateAttemptsStatus(ctx, webhookID, attempt.Status); err != nil {
					if errors.Is(err, storage.ErrAttemptsNotModified) {
						continue
					}
					errChan <- errors.Wrap(err, "storage.Store.UpdateAttemptsStatus")
					continue
				}
			}
		}

		time.Sleep(w.retriesCron)
	}
}
