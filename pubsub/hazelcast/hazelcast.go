package hazelcast

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
	hazelcastCore "github.com/hazelcast/hazelcast-go-client/core"

	"github.com/dapr/components-contrib/internal/retry"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	hazelcastServers           = "hazelcastServers"
	hazelcastBackOffMaxRetries = "backOffMaxRetries"
)

type metadata struct {
	hazelcastServers string
}

type Hazelcast struct {
	client   hazelcast.Client
	logger   logger.Logger
	metadata metadata

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.BackOffConfig
}

// NewHazelcastPubSub returns a new hazelcast pub-sub implementation
func NewHazelcastPubSub(logger logger.Logger) pubsub.PubSub {
	return &Hazelcast{logger: logger}
}

func parseHazelcastMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[hazelcastServers]; ok && val != "" {
		m.hazelcastServers = val
	} else {
		return m, errors.New("hazelcast error: missing hazelcast servers")
	}

	return m, nil
}

func (p *Hazelcast) Init(metadata pubsub.Metadata) error {
	m, err := parseHazelcastMetadata(metadata)
	if err != nil {
		return err
	}

	p.metadata = m
	hzConfig := hazelcast.NewConfig()

	servers := m.hazelcastServers
	hzConfig.NetworkConfig().AddAddress(strings.Split(servers, ",")...)

	p.client, err = hazelcast.NewClientWithConfig(hzConfig)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to create new client, %v", err)
	}

	p.ctx, p.cancel = context.WithCancel(context.Background())

	backOffConfig, err := retry.DecodeConfig(metadata.Properties)
	if err != nil {
		return err
	}
	p.backOffConfig = backOffConfig

	return nil
}

func (p *Hazelcast) Publish(req *pubsub.PublishRequest) error {
	topic, err := p.client.GetTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to get topic for %s", req.Topic)
	}

	if err = topic.Publish(req.Data); err != nil {
		return fmt.Errorf("hazelcast error: failed to publish data, %v", err)
	}

	return nil
}

func (p *Hazelcast) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	topic, err := p.client.GetTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to get topic for %s", req.Topic)
	}

	_, err = topic.AddMessageListener(&hazelcastMessageListener{p, topic.Name(), handler})
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to add new listener, %v", err)
	}

	return nil
}

func (p *Hazelcast) Close() error {
	p.cancel()
	p.client.Shutdown()

	return nil
}

func (p *Hazelcast) Features() []pubsub.Feature {
	return nil
}

type hazelcastMessageListener struct {
	p             *Hazelcast
	topicName     string
	pubsubHandler func(msg *pubsub.NewMessage) error
}

func (l *hazelcastMessageListener) OnMessage(message hazelcastCore.Message) error {
	msg, ok := message.MessageObject().([]byte)
	if !ok {
		return errors.New("hazelcast error: cannot cast message to byte array")
	}

	if err := l.handleMessageObject(msg); err != nil {
		l.p.logger.Error("Failure processing Hazelcast message")

		return err
	}

	return nil
}

func (l *hazelcastMessageListener) handleMessageObject(message []byte) error {
	pubsubMsg := pubsub.NewMessage{
		Data:  message,
		Topic: l.topicName,
	}

	b := l.p.backOffConfig.NewBackOffWithContext(l.p.ctx)

	return retry.RetryNotifyRecover(func() error {
		l.p.logger.Debug("Processing Hazelcast message")

		return l.pubsubHandler(&pubsubMsg)
	}, b, func(err error, d time.Duration) {
		l.p.logger.Error("Error processing Hazelcast message. Retrying...")
	}, func() {
		l.p.logger.Info("Successfully processed Hazelcast message after it previously failed")
	})
}
