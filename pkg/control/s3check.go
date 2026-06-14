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
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// checkTarget validates that the target's endpoint, credentials and bucket are
// usable: it connects and confirms the bucket exists and is accessible. It
// returns nil on success or a descriptive error.
func checkTarget(ctx context.Context, t *Target) error {
	if t.Endpoint == "" || t.Bucket == "" {
		return fmt.Errorf("endpoint and bucket are required")
	}

	cl, err := minio.New(t.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(t.AccessKey, t.SecretKey, ""),
		Secure: t.TLS,
		Region: t.Region,
	})
	if err != nil {
		return fmt.Errorf("invalid endpoint: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	exists, err := cl.BucketExists(ctx, t.Bucket)
	if err != nil {
		// Surfaces auth failures, DNS/TLS errors, connection refused, etc.
		return fmt.Errorf("%w", err)
	}
	if !exists {
		return fmt.Errorf("bucket %q not found or not accessible", t.Bucket)
	}
	return nil
}
