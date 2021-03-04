// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/internal/retry"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	key = "partitionKey"
)

// Kafka allows reading/writing to a Kafka consumer group
type Kafka struct {
	options  Options
	producer sarama.SyncProducer
	logger   logger.Logger
	cg       sarama.ConsumerGroup
	topics   map[string]bool
	cancel   context.CancelFunc
	consumer consumer
	backOff  backoff.BackOff
	config   *sarama.Config
}

// NewKafka returns a new kafka pubsub instance
func NewKafka(l logger.Logger) pubsub.PubSub {
	return &Kafka{logger: l}
}

// Init does metadata parsing and connection establishment
func (k *Kafka) Init(metadata pubsub.Metadata) error {
	// Default options
	k.options = Options{
		BackOffConfig: retry.DefaultBackOffConfig,
	}

	// Decode options from metadata properties
	if err := k.options.Decode(metadata.Properties); err != nil {
		return errors.Errorf("kafka: configuration error: %v", err)
	}

	// Validate the decoded options
	if err := k.options.Validate(); err != nil {
		return err
	}

	k.logger.Debugf("Using %s as ConsumerGroup name", k.options.ConsumerID)
	k.logger.Debugf("Found brokers: %v", k.options.Brokers)

	p, err := k.getSyncProducer()
	if err != nil {
		return err
	}

	k.producer = p

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0

	if k.options.AuthRequired {
		updateAuthInfo(config, k.options.SaslUsername, k.options.SaslPassword)
	}

	k.config = config

	k.topics = make(map[string]bool)

	k.logger.Debug("Kafka message bus initialization complete")

	return nil
}

// Publish message to Kafka cluster
func (k *Kafka) Publish(req *pubsub.PublishRequest) error {
	k.logger.Debugf("Publishing topic %v with data: %v", req.Topic, req.Data)

	msg := &sarama.ProducerMessage{
		Topic: req.Topic,
		Value: sarama.ByteEncoder(req.Data),
	}

	if val, ok := req.Metadata[key]; ok && val != "" {
		msg.Key = sarama.StringEncoder(val)
	}

	partition, offset, err := k.producer.SendMessage(msg)

	k.logger.Debugf("Partition: %v, offset: %v", partition, offset)

	if err != nil {
		return err
	}

	return nil
}

func (k *Kafka) addTopic(newTopic string) []string {
	// Add topic to our map of topics
	k.topics[newTopic] = true

	topics := make([]string, len(k.topics))

	i := 0
	for topic := range k.topics {
		topics[i] = topic
		i++
	}

	return topics
}

// Close down consumer group resources, refresh once
func (k *Kafka) closeSubscripionResources() {
	if k.cg != nil {
		k.cancel()
		err := k.cg.Close()
		if err != nil {
			k.logger.Errorf("Error closing consumer group: %v", err)
		}

		k.consumer.once.Do(func() {
			close(k.consumer.ready)
			k.consumer.once = sync.Once{}
		})
	}
}

// Subscribe to topic in the Kafka cluster
// This call cannot block like its sibling in bindings/kafka because of where this is invoked in runtime.go
func (k *Kafka) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	if k.options.ConsumerID == "" {
		return errors.New("kafka: consumerID must be set to subscribe")
	}

	topics := k.addTopic(req.Topic)

	// Close resources and reset synchronization primitives
	k.closeSubscripionResources()

	cg, err := sarama.NewConsumerGroup(k.options.Brokers, k.options.ConsumerID, k.config)
	if err != nil {
		return err
	}

	k.cg = cg

	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel

	ready := make(chan bool)
	k.consumer = consumer{
		k:        k,
		ready:    ready,
		callback: handler,
	}

	go func() {
		defer func() {
			k.logger.Debugf("Closing ConsumerGroup for topics: %v", topics)
			err := k.cg.Close()
			if err != nil {
				k.logger.Errorf("Error closing consumer group: %v", err)
			}
		}()

		k.logger.Debugf("Subscribed and listening to topics: %s", topics)

		for {
			k.logger.Debugf("Starting loop to consume.")
			// Consume the requested topic
			innerError := k.cg.Consume(ctx, topics, &(k.consumer))
			if innerError != nil {
				k.logger.Errorf("Error consuming %v: %v", topics, innerError)
			}

			// If the context was cancelled, as is the case when handling SIGINT and SIGTERM below, then this pops
			// us out of the consume loop
			if ctx.Err() != nil {
				k.logger.Debugf("Context error, stopping consumer: %v", ctx.Err())

				return
			}
		}
	}()

	<-ready

	return nil
}

func (k *Kafka) getSyncProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	if k.options.AuthRequired {
		updateAuthInfo(config, k.options.SaslUsername, k.options.SaslPassword)
	}

	if k.options.MaxMessageBytes > 0 {
		config.Producer.MaxMessageBytes = k.options.MaxMessageBytes
	}

	producer, err := sarama.NewSyncProducer(k.options.Brokers, config)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

func updateAuthInfo(config *sarama.Config, saslUsername, saslPassword string) {
	config.Net.SASL.Enable = true
	config.Net.SASL.User = saslUsername
	config.Net.SASL.Password = saslPassword
	config.Net.SASL.Mechanism = sarama.SASLTypePlaintext

	config.Net.TLS.Enable = true
	// nolint: gosec
	config.Net.TLS.Config = &tls.Config{
		// InsecureSkipVerify: true,
		ClientAuth: 0,
	}
}

func (k *Kafka) Close() error {
	k.closeSubscripionResources()

	return k.producer.Close()
}

func (k *Kafka) Features() []pubsub.Feature {
	return nil
}

type consumer struct {
	k        *Kafka
	ready    chan bool
	callback func(msg *pubsub.NewMessage) error
	once     sync.Once
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if c.callback == nil {
		return fmt.Errorf("nil consumer callback")
	}

	b := c.k.options.NewBackOffWithContext(session.Context())
	for message := range claim.Messages() {
		msg := pubsub.NewMessage{
			Topic: message.Topic,
			Data:  message.Value,
		}
		if err := retry.RetryNotifyRecover(func() error {
			c.k.logger.Debugf("Processing Kafka message: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
			err := c.callback(&msg)
			if err == nil {
				session.MarkMessage(message, "")
			}

			return err
		}, b, func(err error, d time.Duration) {
			c.k.logger.Errorf("Error processing Kafka message: %s/%d/%d [key=%s]. Retrying...", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
		}, func() {
			c.k.logger.Infof("Successfully processed Kafka message after it previously failed: %s/%d/%d [key=%s]", message.Topic, message.Partition, message.Offset, asBase64String(message.Key))
		}); err != nil {
			return err
		}
	}

	return nil
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
	c.once.Do(func() {
		close(c.ready)
	})

	return nil
}

// asBase64String implements the `fmt.Stringer` interface in order to print
// `[]byte` as a base 64 encoded string.
// It is used above to log the message key. The call to `EncodeToString`
// only occurs for logs that are written based on the logging level.
type asBase64String []byte

func (s asBase64String) String() string {
	return base64.StdEncoding.EncodeToString(s)
}
