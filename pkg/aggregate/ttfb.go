/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package aggregate

import (
	"fmt"
	"time"

	"github.com/minio/warp/pkg/bench"
)

// TTFB contains times to first byte if applicable.
type TTFB struct {
	AverageMillis     float64       `json:"average_millis"`
	FastestMillis     float64       `json:"fastest_millis"`
	P25Millis         float64       `json:"p25_millis"`
	MedianMillis      float64       `json:"median_millis"`
	P75Millis         float64       `json:"p75_millis"`
	P90Millis         float64       `json:"p90_millis"`
	P99Millis         float64       `json:"p99_millis"`
	SlowestMillis     float64       `json:"slowest_millis"`
	StdDevMillis      float64       `json:"std_dev_millis"`
	PercentilesMillis *[101]float64 `json:"percentiles_millis,omitempty"`
}

// AsBench converts to bench.TTFB.
// Provide the byN value to scale the values (typically merged count).
func (t *TTFB) AsBench(byN int) bench.TTFB {
	if t == nil {
		return bench.TTFB{}
	}
	if byN == 0 {
		byN = 1
	}
	millisToDurF := func(millis float64) time.Duration {
		if millis == 0 {
			return 0
		}
		return time.Duration(millis * float64(time.Millisecond))
	}
	var pct [101]time.Duration
	invBy := 1.0 / float64(byN)
	if t.PercentilesMillis != nil {
		for i, v := range t.PercentilesMillis {
			pct[i] = millisToDurF(v * invBy)
		}
	}
	return bench.TTFB{
		Average:     millisToDurF(t.AverageMillis * invBy),
		Worst:       millisToDurF(t.SlowestMillis),
		Best:        millisToDurF(t.FastestMillis),
		P25:         millisToDurF(t.P25Millis * invBy),
		Median:      millisToDurF(t.MedianMillis * invBy),
		P75:         millisToDurF(t.P75Millis * invBy),
		P90:         millisToDurF(t.P90Millis * invBy),
		P99:         millisToDurF(t.P99Millis * invBy),
		StdDev:      millisToDurF(t.StdDevMillis * invBy),
		Percentiles: pct,
	}
}

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.AverageMillis == 0 {
		return ""
	}
	fMilli := float64(time.Millisecond)
	return fmt.Sprintf("Avg: %v, Best: %v, 25th: %v, Median: %v, 75th: %v, 90th: %v, 99th: %v, Worst: %v StdDev: %v",
		time.Duration(t.AverageMillis*fMilli).Round(time.Millisecond),
		time.Duration(t.FastestMillis*fMilli).Round(time.Millisecond),
		time.Duration(t.P25Millis*fMilli).Round(time.Millisecond),
		time.Duration(t.MedianMillis*fMilli).Round(time.Millisecond),
		time.Duration(t.P75Millis*fMilli).Round(time.Millisecond),
		time.Duration(t.P90Millis*fMilli).Round(time.Millisecond),
		time.Duration(t.P99Millis*fMilli).Round(time.Millisecond),
		time.Duration(t.SlowestMillis*fMilli).Round(time.Millisecond),
		time.Duration(t.StdDevMillis*fMilli).Round(time.Millisecond))
}

func (t *TTFB) add(other TTFB) {
	t.AverageMillis += other.AverageMillis
	t.MedianMillis += other.MedianMillis
	if other.FastestMillis != 0 {
		// Deal with 0 value being the best always.
		t.FastestMillis = min(t.FastestMillis, other.FastestMillis)
		if t.FastestMillis == 0 {
			t.FastestMillis = other.FastestMillis
		}
	}
	t.SlowestMillis = max(t.SlowestMillis, other.SlowestMillis)
	t.P25Millis += other.P25Millis
	t.P75Millis += other.P75Millis
	t.P90Millis += other.P90Millis
	t.P99Millis += other.P99Millis
	t.StdDevMillis += other.StdDevMillis
}

func (t TTFB) StringByN(n int) string {
	if t.AverageMillis == 0 || n == 0 {
		return ""
	}
	// rounder...
	fMilli := float64(time.Millisecond)
	fn := 1.0 / float64(n)
	return fmt.Sprintf("Avg: %v, Best: %v, 25th: %v, Median: %v, 75th: %v, 90th: %v, 99th: %v, Worst: %v StdDev: %v",
		time.Duration(t.AverageMillis*fMilli*fn).Round(time.Millisecond),
		time.Duration(t.FastestMillis*fMilli).Round(time.Millisecond),
		time.Duration(t.P25Millis*fMilli*fn).Round(time.Millisecond),
		time.Duration(t.MedianMillis*fMilli*fn).Round(time.Millisecond),
		time.Duration(t.P75Millis*fMilli*fn).Round(time.Millisecond),
		time.Duration(t.P90Millis*fMilli*fn).Round(time.Millisecond),
		time.Duration(t.P99Millis*fMilli*fn).Round(time.Millisecond),
		time.Duration(t.SlowestMillis*fMilli).Round(time.Millisecond),
		time.Duration(t.StdDevMillis*fMilli*fn).Round(time.Millisecond))
}

// TtfbFromBench converts from bench.TTFB
func TtfbFromBench(t bench.TTFB) *TTFB {
	if t.Average <= 0 {
		return nil
	}
	t2 := TTFB{
		AverageMillis: durToMillisF(t.Average),
		SlowestMillis: durToMillisF(t.Worst),
		P25Millis:     durToMillisF(t.P25),
		MedianMillis:  durToMillisF(t.Median),
		P75Millis:     durToMillisF(t.P75),
		P90Millis:     durToMillisF(t.P90),
		P99Millis:     durToMillisF(t.P99),
		StdDevMillis:  durToMillisF(t.StdDev),
		FastestMillis: durToMillisF(t.Best),
	}
	t2.PercentilesMillis = &[101]float64{}
	for i, v := range t.Percentiles[:] {
		t2.PercentilesMillis[i] = durToMillisF(v)
	}
	return &t2
}
