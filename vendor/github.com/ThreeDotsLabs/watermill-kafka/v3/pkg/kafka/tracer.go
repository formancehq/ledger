package kafka

import (
	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
)

type SaramaTracer interface {
	WrapConsumer(sarama.Consumer) sarama.Consumer
	WrapPartitionConsumer(sarama.PartitionConsumer) sarama.PartitionConsumer
	WrapConsumerGroupHandler(sarama.ConsumerGroupHandler) sarama.ConsumerGroupHandler
	WrapSyncProducer(*sarama.Config, sarama.SyncProducer) sarama.SyncProducer
}

type OTELSaramaTracer struct{}

func NewOTELSaramaTracer() SaramaTracer {
	return OTELSaramaTracer{}
}

func (t OTELSaramaTracer) WrapConsumer(c sarama.Consumer) sarama.Consumer {
	return otelsarama.WrapConsumer(c)
}

func (t OTELSaramaTracer) WrapConsumerGroupHandler(h sarama.ConsumerGroupHandler) sarama.ConsumerGroupHandler {
	return otelsarama.WrapConsumerGroupHandler(h)
}

func (t OTELSaramaTracer) WrapPartitionConsumer(pc sarama.PartitionConsumer) sarama.PartitionConsumer {
	return otelsarama.WrapPartitionConsumer(pc)
}

func (t OTELSaramaTracer) WrapSyncProducer(cfg *sarama.Config, p sarama.SyncProducer) sarama.SyncProducer {
	return otelsarama.WrapSyncProducer(cfg, p)
}
