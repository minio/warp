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

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.AverageMillis == 0 {
		return ""
	}
	return fmt.Sprintf("Avg: %v, Best: %v, 25th: %v, Median: %v, 75th: %v, 90th: %v, 99th: %v, Worst: %v StdDev: %v",
		time.Duration(t.AverageMillis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.FastestMillis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.P25Millis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.MedianMillis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.P75Millis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.P90Millis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.P99Millis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.SlowestMillis)*time.Millisecond.Round(time.Millisecond),
		time.Duration(t.StdDevMillis)*time.Millisecond.Round(time.Millisecond))
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
	hN := n / 2
	return fmt.Sprintf("Avg: %v, Best: %v, 25th: %v, Median: %v, 75th: %v, 90th: %v, 99th: %v, Worst: %v StdDev: %v",
		time.Duration((hN+int(t.AverageMillis))/n)*time.Millisecond,
		time.Duration(t.FastestMillis)*time.Millisecond,
		time.Duration((hN+int(t.P25Millis))/n)*time.Millisecond,
		time.Duration((hN+int(t.MedianMillis))/n)*time.Millisecond,
		time.Duration((hN+int(t.P75Millis))/n)*time.Millisecond,
		time.Duration((hN+int(t.P90Millis))/n)*time.Millisecond,
		time.Duration((hN+int(t.P99Millis))/n)*time.Millisecond,
		time.Duration(t.SlowestMillis)*time.Millisecond,
		time.Duration((hN+int(t.StdDevMillis))/n)*time.Millisecond)
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
