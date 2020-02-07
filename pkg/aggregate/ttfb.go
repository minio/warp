package aggregate

import (
	"fmt"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// TTFB contains times to first byte if applicable.
type TTFB struct {
	AverageMillis int `json:"average_millis"`
	MedianMillis  int `json:"median_millis"`
	FastestMillis int `json:"fastest_millis"`
	SlowestMillis int `json:"slowest_millis"`
}

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.AverageMillis == 0 {
		return ""
	}
	return fmt.Sprintf("Average: %v, Median: %v, Best: %v, Worst: %v",
		time.Duration(t.AverageMillis)*time.Millisecond,
		time.Duration(t.MedianMillis)*time.Millisecond,
		time.Duration(t.FastestMillis)*time.Millisecond,
		time.Duration(t.SlowestMillis)*time.Millisecond)
}

// TtfbFromBench converts from bench.TTFB
func TtfbFromBench(t bench.TTFB) *TTFB {
	if t.Average <= 0 {
		return nil
	}
	return &TTFB{
		AverageMillis: durToMillis(t.Average),
		MedianMillis:  durToMillis(t.Median),
		FastestMillis: durToMillis(t.Best),
		SlowestMillis: durToMillis(t.Worst),
	}
}
