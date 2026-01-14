package iceberg

import (
	"context"
	"errors"
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
