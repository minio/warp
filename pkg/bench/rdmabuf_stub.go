//go:build !rdma

/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package bench

import "io"

// HasRDMA reports whether warp was built with -tags=rdma, i.e. whether
// GPU-Direct RDMA (--rdma=gpu) is available.
const HasRDMA = false

// bindGPUThread is a no-op without -tags=rdma; there is no CUDA context.
func bindGPUThread() error { return nil }

// allocRDMAGPU is unsupported on builds without -tags=rdma. Selecting
// --rdma=gpu produces a clear error rather than silently falling back
// to CPU memory.
func allocRDMAGPU(int) (*rdmaBuf, error) {
	return nil, errRDMAGPUUnsupported
}

func stageToGPU(*rdmaBuf, io.Reader) error {
	return errRDMAGPUUnsupported
}

func freeRDMAGPU(*rdmaBuf) {}
