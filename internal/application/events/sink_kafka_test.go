//go:build kafka

package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestConfigureSASL_Empty(t *testing.T) {
	t.Parallel()

	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{SASLMechanism: ""})
	require.NoError(t, err)
	require.False(t, saramaCfg.Net.SASL.Enable)
}

func TestConfigureSASL_Plain(t *testing.T) {
	t.Parallel()

	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{
		SASLMechanism: "PLAIN",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	require.NoError(t, err)
	require.True(t, saramaCfg.Net.SASL.Enable)
	require.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), saramaCfg.Net.SASL.Mechanism)
	require.Equal(t, "user", saramaCfg.Net.SASL.User)
	require.Equal(t, "pass", saramaCfg.Net.SASL.Password)
}

func TestConfigureSASL_SCRAMSHA256(t *testing.T) {
	t.Parallel()

	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	require.NoError(t, err)
	require.True(t, saramaCfg.Net.SASL.Enable)
	require.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256), saramaCfg.Net.SASL.Mechanism)
	require.NotNil(t, saramaCfg.Net.SASL.SCRAMClientGeneratorFunc)

	// Verify the generator produces a working scramClient
	client := saramaCfg.Net.SASL.SCRAMClientGeneratorFunc()
	require.NotNil(t, client)
}

func TestConfigureSASL_SCRAMSHA512(t *testing.T) {
	t.Parallel()

	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{
		SASLMechanism: "SCRAM-SHA-512",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	require.NoError(t, err)
	require.True(t, saramaCfg.Net.SASL.Enable)
	require.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512), saramaCfg.Net.SASL.Mechanism)
	require.NotNil(t, saramaCfg.Net.SASL.SCRAMClientGeneratorFunc)
}

func TestConfigureSASL_Unsupported(t *testing.T) {
	t.Parallel()

	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{
		SASLMechanism: "OAUTHBEARER",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported SASL mechanism")
}

func TestConfigureSASL_CaseInsensitive(t *testing.T) {
	t.Parallel()

	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{
		SASLMechanism: "plain",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	require.NoError(t, err)
	require.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), saramaCfg.Net.SASL.Mechanism)
}

func TestScramClient_Begin(t *testing.T) {
	t.Parallel()

	// Use SCRAM-SHA-256 generator
	saramaCfg := sarama.NewConfig()
	err := configureSASL(saramaCfg, KafkaSinkConfig{
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "user",
		SASLPassword:  "pass",
	})
	require.NoError(t, err)

	client := saramaCfg.Net.SASL.SCRAMClientGeneratorFunc()
	err = client.Begin("user", "pass", "")
	require.NoError(t, err)

	// After Begin, conversation should not be done
	require.False(t, client.Done())

	// Step should return the initial client-first message
	response, err := client.Step("")
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestKafkaSinkPublishSanitizesDeliveryError(t *testing.T) {
	t.Parallel()
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	producer := mocks.NewAsyncProducer(t, cfg)
	producer.ExpectInputAndFail(errors.New("authentication failed with broker-password"))
	sink := &KafkaSink{
		producer: producer,
		topic:    "ledger-events",
		format:   FormatProto,
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
		errors:   newSinkErrorSanitizer(nil, "broker-password"),
	}
	go sink.dispatchDeliveries()
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := sink.Publish(ctx, []*eventspb.Event{{LogSequence: 42}})
	require.Error(t, err)
	require.NoError(t, ctx.Err(), "test must observe the producer failure, not a timeout")
	var failures sarama.ProducerErrors
	require.ErrorAs(t, err, &failures)
	require.Len(t, failures, 1)
	require.Contains(t, err.Error(), "ledger-events")
	require.Contains(t, err.Error(), "authentication failed")
	require.NotContains(t, err.Error(), "broker-password")
}
