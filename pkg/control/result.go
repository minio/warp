/*
 * Warp (C) 2019-2026 MinIO, Inc.
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

package control

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/klauspost/compress/zstd"

	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

// loadResult reads a benchmark result file (aggregated JSON or raw CSV, both
// zstandard-compressed) into an aggregate.Realtime ready for the dashboard.
func loadResult(path string) (*aggregate.Realtime, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	z, err := zstd.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("decompress %q: %w", path, err)
	}
	defer z.Close()

	buf := bufio.NewReader(z)
	first, err := buf.Peek(1)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}

	// Aggregated results start with a JSON object; everything else is raw CSV ops.
	if bytes.Equal(first, []byte("{")) {
		var rt aggregate.Realtime
		if err := json.NewDecoder(buf).Decode(&rt); err != nil {
			return nil, fmt.Errorf("parse aggregated result %q: %w", path, err)
		}
		return &rt, nil
	}

	opCh := make(chan bench.Operation, 10000)
	errCh := make(chan error, 1)
	go func() {
		errCh <- bench.StreamOperationsFromCSV(buf, false, 0, 0, nil, opCh)
	}()
	rt := aggregate.Live(opCh, nil, "", nil)
	if err := <-errCh; err != nil {
		return nil, fmt.Errorf("parse CSV result %q: %w", path, err)
	}
	return rt, nil
}
