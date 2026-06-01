package domain

import (
	"math"
	"time"
)


type RetryPolicy struct {
	MaxAttempts        int           	
	InitialInterval    time.Duration 
	BackoffCoefficient float64       
}

var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts:        3,
	InitialInterval:    1 * time.Second,
	BackoffCoefficient: 2.0,
}

var NoRetry = RetryPolicy{
	MaxAttempts:        1,
	InitialInterval:    0,
	BackoffCoefficient: 1.0,
}

// IntervalFor returns how long to wait before the given attempt number.
// attempt=1 means the first retry (after the original attempt failed).
func (r RetryPolicy) IntervalFor(attempt int) time.Duration {
	multiplier := math.Pow(r.BackoffCoefficient, float64(attempt-1))
	return time.Duration(float64(r.InitialInterval) * multiplier)
}

// ShouldRetry returns true if another attempt is allowed.
func (r RetryPolicy) ShouldRetry(attemptsDone int) bool {
	return attemptsDone < r.MaxAttempts
}