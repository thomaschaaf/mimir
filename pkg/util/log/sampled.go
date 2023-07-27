// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"context"
	"time"

	"go.uber.org/atomic"
)

type SampledError struct {
	Err     error
	Sampler *Sampler
}

func (s SampledError) Error() string { return s.Err.Error() }
func (s SampledError) Unwrap() error { return s.Err }

// This method is called by common logging module.
func (s SampledError) ShouldLog(_ context.Context, _ time.Duration) bool {
	return s.Sampler == nil || s.Sampler.Sample()
}

type Sampler struct {
	freq  int64
	count atomic.Int64
}

func NewSampler(freq int64) *Sampler {
	if freq == 0 {
		return nil
	}
	return &Sampler{freq: freq}
}

func (e *Sampler) Sample() bool {
	count := e.count.Inc()
	return count%e.freq == 0
}
