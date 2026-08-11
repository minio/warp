//go:build rdma

/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package bench

// HasRDMA reports whether warp was built with -tags=rdma, i.e. whether
// S3-over-RDMA dispatch (--rdma=cpu) is available. It requires libminiocpp
// at build and run time.
const HasRDMA = true
