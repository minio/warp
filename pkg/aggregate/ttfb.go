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
	MedianMillis  int `json:"median_millis"`
	FastestMillis int `json:"fastest_millis"`
	SlowestMillis int `json:"slowest_millis"`
}

// String returns a human printable version of the time to first byte.
func (t TTFB) String() string {
	if t.AverageMillis == 0 {
		return ""
	}
	return fmt.Sprintf("Avg: %v, Median: %v, Best: %v, Worst: %v",
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
