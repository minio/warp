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
	AverageMillis int `json:"average_millis"`
	FastestMillis int `json:"fastest_millis"`
	P25Millis     int `json:"p25_millis"`
	MedianMillis  int `json:"median_millis"`
	P75Millis     int `json:"p75_millis"`
	P90Millis     int `json:"p90_millis"`
	P99Millis     int `json:"p99_millis"`
	SlowestMillis int `json:"slowest_millis"`
}

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.AverageMillis == 0 {
		return ""
	}
	return fmt.Sprintf("Avg: %v, Best: %v, 25th: %v, Median: %v, 75th: %v, 90th: %v, 99th: %v, Worst: %v",
		time.Duration(t.AverageMillis)*time.Millisecond,
		time.Duration(t.FastestMillis)*time.Millisecond,
		time.Duration(t.P25Millis)*time.Millisecond,
		time.Duration(t.MedianMillis)*time.Millisecond,
		time.Duration(t.P75Millis)*time.Millisecond,
		time.Duration(t.P90Millis)*time.Millisecond,
		time.Duration(t.P99Millis)*time.Millisecond,
		time.Duration(t.SlowestMillis)*time.Millisecond)
}

// TtfbFromBench converts from bench.TTFB
func TtfbFromBench(t bench.TTFB) *TTFB {
	if t.Average <= 0 {
		return nil
	}
	return &TTFB{
		AverageMillis: durToMillis(t.Average),
		SlowestMillis: durToMillis(t.Worst),
		P25Millis:     durToMillis(t.P25),
		MedianMillis:  durToMillis(t.Median),
		P75Millis:     durToMillis(t.P75),
		P90Millis:     durToMillis(t.P90),
		P99Millis:     durToMillis(t.P99),
		FastestMillis: durToMillis(t.Best),
	}
}
