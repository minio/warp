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

package iceberg

import (
	"context"
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// CommitConfig holds configuration for commit retry behavior.
type CommitConfig struct {
	MaxRetries  int
	BackoffBase time.Duration
	BackoffMax  time.Duration
}

// DefaultCommitConfig returns sensible defaults for commit retries.
func DefaultCommitConfig() CommitConfig {
	return CommitConfig{
		MaxRetries:  10,
		BackoffBase: 100 * time.Millisecond,
		BackoffMax:  5 * time.Second,
	}
}

// CommitResult holds the result of a commit operation.
type CommitResult struct {
	Success  bool
	Retries  int
	Duration time.Duration
	Err      error
}

// CommitWithRetry commits files to an Iceberg table with exponential backoff on conflict.
func CommitWithRetry(ctx context.Context, tbl *table.Table, files []string, cfg CommitConfig) CommitResult {
	start := time.Now()
	result := CommitResult{}

	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			result.Retries++

			backoff := cfg.BackoffBase * time.Duration(1<<uint(attempt-1))
			if backoff > cfg.BackoffMax {
				backoff = cfg.BackoffMax
			}
			jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
			backoff += jitter

			select {
			case <-ctx.Done():
				result.Err = ctx.Err()
				result.Duration = time.Since(start)
				return result
			case <-time.After(backoff):
			}

			// Refresh table metadata before retry
			if err := tbl.Refresh(ctx); err != nil {
				continue
			}
		}

		err := doCommit(ctx, tbl, files)
		if err == nil {
			result.Success = true
			result.Duration = time.Since(start)
			return result
		}

		if !IsConflictError(err) {
			result.Err = err
			result.Duration = time.Since(start)
			return result
		}
	}

	result.Err = errors.New("max retries exceeded for commit")
	result.Duration = time.Since(start)
	return result
}

func doCommit(ctx context.Context, tbl *table.Table, files []string) error {
	tx := tbl.NewTransaction()
	err := tx.AddFiles(ctx, files, iceberg.Properties{
		"written-by": "warp-iceberg",
		"timestamp":  time.Now().Format(time.RFC3339),
	}, true)
	if err != nil {
		return err
	}

	_, err = tx.Commit(ctx)
	return err
}

// IsConflictError checks if an error is a commit conflict that can be retried.
func IsConflictError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "409") ||
		strings.Contains(errStr, "Conflict") ||
		strings.Contains(errStr, "conflict") ||
		strings.Contains(errStr, "Requirement failed") ||
		strings.Contains(errStr, "requirement failed")
}
