/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
