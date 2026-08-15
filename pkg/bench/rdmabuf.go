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

	"github.com/minio/minio-go/v7"
)

// RDMAMode constants accepted by --rdma.
const (
	RDMAModeOff = ""
	RDMAModeCPU = "cpu"
	RDMAModeGPU = "gpu"
)

// MaxRDMADescriptorSize is the largest transfer a single RDMA descriptor
// can name: the x-amz-rdma-token carries the window size in a 32-bit field.
// A larger object cannot go out as one transfer and has to be streamed as
// multipart instead.
const MaxRDMADescriptorSize = 1<<32 - 1

// rdmaBuf is a per-op buffer registered with the NIC for S3-over-RDMA.
// For CPU mode, ptr is page-aligned host memory from libminiocpp. For GPU
// mode, ptr is a CUDA device pointer returned by cudaMalloc — see
// rdmabuf_gpu.go. Both live outside the Go heap: minio-go retains the
// pointer across the cgo boundary, which Go memory may not do.
type rdmaBuf struct {
	ptr  unsafe.Pointer
	size int
	mode string
}

// allocRDMABuf returns a buffer suitable for opts.RDMABuffer. Workers keep
// one and grow it as needed, so it must be released with freeRDMABuf when
// the worker exits. PUT fills it via stageToRDMABuf; GET has the server
// write into it.
func allocRDMABuf(mode string, size int) (*rdmaBuf, error) {
	switch mode {
	case RDMAModeCPU:
		if size <= 0 {
			return &rdmaBuf{mode: mode}, nil
		}
		ptr := minio.AlignedBuffer(size)
		if ptr == nil {
			return nil, fmt.Errorf("rdma: aligned buffer allocation of %d bytes failed", size)
		}
		return &rdmaBuf{ptr: ptr, size: size, mode: mode}, nil
	case RDMAModeGPU:
		return allocRDMAGPU(size)
	default:
		return nil, fmt.Errorf("rdma: unknown mode %q (want %q or %q)",
			mode, RDMAModeCPU, RDMAModeGPU)
	}
}

// stageToRDMABuf reads n bytes from src into the front of the buffer, which
// may be larger because it is reused across operations. For CPU buffers this
// is a straight ReadFull. For GPU buffers the bytes are first read into a CPU
// bounce buffer and then uploaded via cudaMemcpy host-to-device (see
// rdmabuf_gpu.go).
func stageToRDMABuf(b *rdmaBuf, src io.Reader, n int) error {
	if b == nil || n <= 0 || src == nil {
		return nil
	}
	if n > b.size {
		return fmt.Errorf("rdma: staging %d bytes into a %d byte buffer", n, b.size)
	}
	switch b.mode {
	case RDMAModeCPU:
		_, err := io.ReadFull(src, unsafe.Slice((*byte)(b.ptr), n))
		return err
	case RDMAModeGPU:
		return stageToGPU(b, src, n)
	default:
		return fmt.Errorf("rdma: unknown mode %q", b.mode)
	}
}

// freeRDMABuf releases the buffer's off-heap memory. Neither the CPU nor
// the GPU allocation is garbage collected, so every allocRDMABuf must be
// paired with this or a benchmark leaks a buffer per operation.
func freeRDMABuf(b *rdmaBuf) {
	if b == nil || b.ptr == nil {
		return
	}
	switch b.mode {
	case RDMAModeCPU:
		minio.FreeAlignedBuffer(b.ptr)
	case RDMAModeGPU:
		freeRDMAGPU(b)
	}
	b.ptr = nil
	b.size = 0
}

// errRDMAGPUUnsupported is returned when the CUDA runtime is unavailable,
// either because warp was built without -tags=rdma or because libcudart
// could not be loaded on this host.
var errRDMAGPUUnsupported = errors.New(
	"rdma=gpu requires an RDMA build of warp and the CUDA runtime on this host")

// rdmaWindowOrDefault sizes the pinned buffer warp hands to a streaming RDMA
// PUT. The buffer selects the RDMA path rather than carrying the object --
// libminiocpp allocates and registers the part buffers itself -- so this only
// needs to be a valid registration, and a small one keeps a run with many
// workers from pinning more than it needs.
func rdmaWindowOrDefault(window int64) int {
	if window > 0 {
		return int(window)
	}
	return 64 << 20
}

// rdmaPool is a fixed set of pinned buffers, one per worker, allocated and
// registered before the benchmark starts.
//
// Allocating inside the timed loop charges the first operation of every worker
// for a page-aligned allocation and an ibv_reg_mr that pins the pages -- work
// that has nothing to do with the transfer being measured, and that a run with
// many workers pays repeatedly as buffers grow. Paying it up front keeps it out
// of the measurement entirely.
type rdmaPool struct {
	bufs []*rdmaBuf
}

// newRDMAPool allocates n buffers of size bytes. It cleans up whatever it
// managed to allocate if one fails, so a partial pool never escapes.
func newRDMAPool(mode string, n, size int) (*rdmaPool, error) {
	if mode == RDMAModeOff || n <= 0 || size <= 0 {
		return nil, nil
	}
	p := &rdmaPool{bufs: make([]*rdmaBuf, 0, n)}
	for i := 0; i < n; i++ {
		b, err := allocRDMABuf(mode, size)
		if err != nil {
			p.Free()
			return nil, fmt.Errorf("rdma pool: buffer %d of %d (%d bytes each): %w", i+1, n, size, err)
		}
		p.bufs = append(p.bufs, b)
	}
	return p, nil
}

// Get returns the buffer belonging to worker i, or nil if there is no pool.
// Workers never share, so no locking is needed.
func (p *rdmaPool) Get(i int) *rdmaBuf {
	if p == nil || len(p.bufs) == 0 {
		return nil
	}
	return p.bufs[i%len(p.bufs)]
}

// Free releases every buffer in the pool.
func (p *rdmaPool) Free() {
	if p == nil {
		return
	}
	for _, b := range p.bufs {
		if b != nil {
			freeRDMABuf(b)
		}
	}
	p.bufs = nil
}
