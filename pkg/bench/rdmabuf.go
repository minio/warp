/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package bench

import (
	"errors"
	"fmt"
	"io"
	"unsafe"
)

// RDMAMode constants accepted by --rdma.
const (
	RDMAModeOff = ""
	RDMAModeCPU = "cpu"
	RDMAModeGPU = "gpu"
)

// rdmaBuf is a per-op buffer registered with the NIC for S3-over-RDMA.
// For CPU mode, ptr points into pinned host memory (a Go []byte that we
// keep alive via the holder field). For GPU mode, ptr is a CUDA device
// pointer returned by cudaMalloc — see rdmabuf_rdma.go.
type rdmaBuf struct {
	ptr    unsafe.Pointer
	size   int
	mode   string
	holder []byte // CPU mode: keeps the backing slice alive
}

// allocRDMABuf returns a per-op buffer suitable for opts.RDMABuffer.
// `src`, when non-nil, is drained into the buffer (used by PUT to stage
// the generator output). For GPU mode the source data is uploaded via
// cudaMemcpy in stageToRDMABuf; for CPU mode it is a plain io.ReadFull.
func allocRDMABuf(mode string, size int) (*rdmaBuf, error) {
	switch mode {
	case RDMAModeCPU:
		b := make([]byte, size)
		if size == 0 {
			return &rdmaBuf{mode: mode, size: 0, holder: b}, nil
		}
		return &rdmaBuf{
			ptr:    unsafe.Pointer(&b[0]),
			size:   size,
			mode:   mode,
			holder: b,
		}, nil
	case RDMAModeGPU:
		return allocRDMAGPU(size)
	default:
		return nil, fmt.Errorf("rdma: unknown mode %q (want %q or %q)",
			mode, RDMAModeCPU, RDMAModeGPU)
	}
}

// stageToRDMABuf drains src into the buffer. For CPU buffers this is a
// straight ReadFull into the backing slice. For GPU buffers the bytes
// are first read into a small CPU bounce buffer and then uploaded via
// cudaMemcpy host-to-device (see rdmabuf_rdma.go).
func stageToRDMABuf(b *rdmaBuf, src io.Reader) error {
	if b == nil || b.size == 0 || src == nil {
		return nil
	}
	switch b.mode {
	case RDMAModeCPU:
		_, err := io.ReadFull(src, b.holder)
		return err
	case RDMAModeGPU:
		return stageToGPU(b, src)
	default:
		return fmt.Errorf("rdma: unknown mode %q", b.mode)
	}
}

// freeRDMABuf releases CUDA device memory; for CPU buffers it lets the
// Go runtime reclaim the backing slice once the rdmaBuf goes out of
// scope.
func freeRDMABuf(b *rdmaBuf) {
	if b == nil {
		return
	}
	if b.mode == RDMAModeGPU {
		freeRDMAGPU(b)
	}
}

// errRDMAGPUUnsupported is returned by the stub GPU allocator built
// without -tags=rdma.
var errRDMAGPUUnsupported = errors.New(
	"rdma=gpu requires building warp with -tags=rdma (libcudart + libminiocpp)")
