package retry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/internal/retry"
)

var errRetry = errors.New("Testing")

func TestDecode(t *testing.T) {
	tests := map[string]struct {
		config    map[string]interface{}
		overrides func(config *retry.BackOffConfig)
		err       string
	}{
		"invalid policy type": {
			config: map[string]interface{}{
				"backOffPolicy": "invalid",
			},
			overrides: nil,
			err:       "1 error(s) decoding:\n\n* error decoding 'backOffPolicy': invalid PolicyType \"invalid\": unexpected back off policy type: invalid",
		},
		"default": {
			config:    map[string]interface{}{},
			overrides: nil,
			err:       "",
		},
		"constant default": {
			config: map[string]interface{}{
				"backOffPolicy": "constant",
			},
			overrides: nil,
			err:       "",
		},
		"constant with duraction": {
			config: map[string]interface{}{
				"backOffPolicy":   "constant",
				"backOffDuration": "10s",
			},
			overrides: func(config *retry.BackOffConfig) {
				config.Duration = 10 * time.Second
			},
			err: "",
		},
		"exponential default": {
			config: map[string]interface{}{
				"backOffPolicy": "exponential",
			},
			overrides: func(config *retry.BackOffConfig) {
				config.Policy = retry.PolicyExponential
			},
			err: "",
		},
		"exponential with string settings": {
			config: map[string]interface{}{
				"backOffPolicy":              "exponential",
				"backOffInitialInterval":     "1000", // 1s
				"backOffRandomizationFactor": "1.0",
				"backOffMultiplier":          "2.0",
				"backOffMaxInterval":         "120000",  // 2m
				"backOffMaxElapsedTime":      "1800000", // 30m
			},
			overrides: func(config *retry.BackOffConfig) {
				config.Policy = retry.PolicyExponential
				config.InitialInterval = 1 * time.Second
				config.RandomizationFactor = 1.0
				config.Multiplier = 2.0
				config.MaxInterval = 2 * time.Minute
				config.MaxElapsedTime = 30 * time.Minute
			},
			err: "",
		},
		"exponential with typed settings": {
			config: map[string]interface{}{
				"backOffPolicy":              "exponential",
				"backOffInitialInterval":     "1000ms", // 1s
				"backOffRandomizationFactor": 1.0,
				"backOffMultiplier":          2.0,
				"backOffMaxInterval":         "120s", // 2m
				"backOffMaxElapsedTime":      "30m",  // 30m
			},
			overrides: func(config *retry.BackOffConfig) {
				config.Policy = retry.PolicyExponential
				config.InitialInterval = 1 * time.Second
				config.RandomizationFactor = 1.0
				config.Multiplier = 2.0
				config.MaxInterval = 2 * time.Minute
				config.MaxElapsedTime = 30 * time.Minute
			},
			err: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := retry.DecodeConfig(tc.config)
			if tc.err != "" {
				if assert.Error(t, err) {
					assert.Equal(t, tc.err, err.Error())
				}
			} else {
				config := retry.DefaultBackOffConfig
				if tc.overrides != nil {
					tc.overrides(&config)
				}
				assert.Equal(t, config, actual, "unexpected decoded configuration")
			}
		})
	}
}

func TestRetryNotifyRecoverMaxRetries(t *testing.T) {
	config := retry.DefaultBackOffConfig
	config.MaxRetries = 3
	config.Duration = 1

	var operationCalls, notifyCalls, recoveryCalls int

	b := config.NewBackOff()
	err := retry.RetryNotifyRecover(func() error {
		operationCalls++

		return errRetry
	}, b, func(err error, d time.Duration) {
		notifyCalls++
	}, func() {
		recoveryCalls++
	})

	assert.Error(t, err)
	assert.Equal(t, errRetry, err)
	assert.Equal(t, 4, operationCalls)
	assert.Equal(t, 1, notifyCalls)
	assert.Equal(t, 0, recoveryCalls)
}

func TestRetryNotifyRecoverRecovery(t *testing.T) {
	config := retry.DefaultBackOffConfig
	config.MaxRetries = 3
	config.Duration = 1

	var operationCalls, notifyCalls, recoveryCalls int

	b := config.NewBackOff()
	err := retry.RetryNotifyRecover(func() error {
		operationCalls++

		if operationCalls >= 2 {
			return nil
		}

		return errRetry
	}, b, func(err error, d time.Duration) {
		notifyCalls++
	}, func() {
		recoveryCalls++
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, operationCalls)
	assert.Equal(t, 1, notifyCalls)
	assert.Equal(t, 1, recoveryCalls)
}

func TestRetryNotifyRecoverCancel(t *testing.T) {
	config := retry.DefaultBackOffConfig
	config.Policy = retry.PolicyExponential
	config.InitialInterval = 10 * time.Millisecond

	var notifyCalls, recoveryCalls int

	ctx, cancel := context.WithCancel(context.Background())
	b := config.NewBackOffWithContext(ctx)
	errC := make(chan error, 1)

	go func() {
		errC <- retry.RetryNotifyRecover(func() error {
			return errRetry
		}, b, func(err error, d time.Duration) {
			notifyCalls++
		}, func() {
			recoveryCalls++
		})
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()

	err := <-errC
	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
	assert.Equal(t, 1, notifyCalls)
	assert.Equal(t, 0, recoveryCalls)
}
