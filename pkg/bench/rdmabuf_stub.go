//go:build !rdma

/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package bench

import "io"

// HasRDMA reports whether warp was built with -tags=rdma, i.e. whether
// S3-over-RDMA dispatch is available. It requires libminiocpp at build and
// run time.
const HasRDMA = false

// HasRDMAGPU reports whether --rdma=gpu can work on this host. Without
// -tags=rdma there is no RDMA dispatch at all, GPU or otherwise.
func HasRDMAGPU() bool { return false }

// RDMAGPUUnavailable explains why HasRDMAGPU is false.
func RDMAGPUUnavailable() string { return "this warp binary was built without RDMA support" }

// bindGPUThread is a no-op without -tags=rdma; there is no CUDA context.
func bindGPUThread() error { return nil }

// allocRDMAGPU is unsupported on builds without -tags=rdma. Selecting
// --rdma=gpu produces a clear error rather than silently falling back
// to CPU memory.
func allocRDMAGPU(int) (*rdmaBuf, error) {
	return nil, errRDMAGPUUnsupported
}

func stageToGPU(*rdmaBuf, io.Reader, int) error {
	return errRDMAGPUUnsupported
}

func freeRDMAGPU(*rdmaBuf) {}
