//go:build rdma

/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package bench

// #cgo LDFLAGS: -lcudart
// #include <cuda_runtime.h>
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"io"
	"unsafe"
)

// allocRDMAGPU allocates a CUDA device buffer of size bytes. The
// returned rdmaBuf carries the device pointer in ptr; the NIC GPU-
// Direct RDMA-writes / reads into it via minio-go's RDMA dispatch.
func allocRDMAGPU(size int) (*rdmaBuf, error) {
	if size <= 0 {
		return &rdmaBuf{mode: RDMAModeGPU}, nil
	}
	var devPtr unsafe.Pointer
	if rc := C.cudaMalloc(&devPtr, C.size_t(size)); rc != 0 {
		return nil, fmt.Errorf("cudaMalloc(%d): cudaError=%d", size, int(rc))
	}
	return &rdmaBuf{ptr: devPtr, size: size, mode: RDMAModeGPU}, nil
}

// stageToGPU reads `size` bytes from src into a CPU bounce buffer, then
// cudaMemcpys host-to-device into the registered GPU buffer. The bounce
// buffer is reused per chunk to keep the per-op allocation budget low.
func stageToGPU(b *rdmaBuf, src io.Reader) error {
	if b == nil || b.size == 0 || src == nil {
		return nil
	}
	host := make([]byte, b.size)
	if _, err := io.ReadFull(src, host); err != nil {
		return err
	}
	rc := C.cudaMemcpy(b.ptr,
		unsafe.Pointer(&host[0]),
		C.size_t(b.size),
		C.cudaMemcpyHostToDevice)
	if rc != 0 {
		return fmt.Errorf("cudaMemcpy H2D: cudaError=%d", int(rc))
	}
	return nil
}

// freeRDMAGPU releases the CUDA device buffer.
func freeRDMAGPU(b *rdmaBuf) {
	if b == nil || b.ptr == nil {
		return
	}
	C.cudaFree(b.ptr)
	b.ptr = nil
}
