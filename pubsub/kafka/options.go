// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/internal/config"
	"github.com/dapr/components-contrib/internal/retry"
)

// Configuration errors
var (
	// ErrMissingBrokers indicates that the brokers were not configured
	ErrMissingBrokers = errors.New("kafka error: missing 'brokers' attribute")
	// ErrMissingSASLUsername indicates that authRequired is true but the SASL username was not specified
	ErrMissingSASLUsername = errors.New("kafka error: missing SASL Username")
	// ErrMissingSASLPassword indicates that authRequired is true but the SASL password was not specified
	ErrMissingSASLPassword = errors.New("kafka error: missing SASL Password")
)

// Options encapsulates the metadata settings from the component configuration.
type Options struct {
	Brokers             config.CommaDelimitedString `mapstructure:"brokers"`
	ConsumerID          string                      `mapstructure:"consumerID"`
	AuthRequired        bool                        `mapstructure:"authRequired"`
	SaslUsername        string                      `mapstructure:"saslUsername"`
	SaslPassword        string                      `mapstructure:"saslPassword"`
	MaxMessageBytes     int                         `mapstructure:"maxMessageBytes"`
	retry.BackOffConfig `mapstructure:",squash"`
}

func (o *Options) Decode(in interface{}) error {
	return config.Decode(in, o)
}

// Validate may return an error with misconfiguration details.
func (o *Options) Validate() error {
	if len(o.Brokers) == 0 {
		return ErrMissingBrokers
	}

	if o.AuthRequired {
		if o.SaslUsername == "" {
			return ErrMissingSASLUsername
		}
		if o.SaslPassword == "" {
			return ErrMissingSASLPassword
		}
	}

	return nil
}
