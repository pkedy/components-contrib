// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package kafka

import (
	"testing"

	"github.com/dapr/components-contrib/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidate(t *testing.T) {
	tests := map[string]struct {
		settings map[string]string
		expected *Options
		error    string
	}{
		"success": {
			settings: map[string]string{
				"consumerID":      "a",
				"brokers":         "a",
				"authRequired":    "true",
				"saslUsername":    "sassafras",
				"saslPassword":    "sassapass",
				"maxMessageBytes": "2048",
			},
			expected: &Options{
				ConsumerID:      "a",
				Brokers:         []string{"a"},
				AuthRequired:    true,
				SaslUsername:    "sassafras",
				SaslPassword:    "sassapass",
				MaxMessageBytes: 2048,
			},
		},
		"want brokers": {
			settings: map[string]string{},
			error:    "kafka error: missing 'brokers' attribute",
		},
		"want SASL username": {
			settings: map[string]string{
				"brokers":      "akfak.com:9092",
				"authRequired": "true",
			},
			error: "kafka error: missing SASL Username",
		},
		"want SASL password": {
			settings: map[string]string{
				"brokers":      "akfak.com:9092",
				"authRequired": "true",
				"saslUsername": "sassafras",
			},
			error: "kafka error: missing SASL Password",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var actual Options
			err := config.Decode(tc.settings, &actual)
			require.NoError(t, err)
			err = actual.Validate()

			if tc.error != "" {
				if assert.Error(t, err) {
					assert.Equal(t, tc.error, err.Error())
				}
			} else {
				if assert.NoError(t, err) {
					assert.Equal(t, tc.expected, &actual)
				}
			}
		})
	}
}
