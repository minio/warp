//go:build rdma && cuda

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

// HasRDMAGPU reports whether warp was built with -tags=rdma,cuda, i.e.
// whether GPU-Direct RDMA (--rdma=gpu) is available. It requires the CUDA
// runtime at build and run time, on top of everything HasRDMA needs.
const HasRDMAGPU = true

// bindGPUThread makes the primary CUDA context current on the calling OS
// thread. cuFileBufRegister resolves the device via cuCtxGetDevice, which
// fails with CUDA_ERROR_INVALID_CONTEXT on any thread that has never issued a
// CUDA call — and cgo runs a goroutine's calls on whatever thread it likes, so
// registration lands on a context-less thread and the whole transfer falls
// back. Callers must runtime.LockOSThread() first so the binding still holds
// when the RDMA dispatch reaches libcufile.
func bindGPUThread() error {
	if rc := C.cudaSetDevice(0); rc != 0 {
		return fmt.Errorf("cudaSetDevice(0): %s", C.GoString(C.cudaGetErrorString(rc)))
	}
	// cudaSetDevice defers context creation; force it now so the first
	// registration on this thread already has a current context.
	if rc := C.cudaFree(nil); rc != 0 {
		return fmt.Errorf("cudaFree(nil): %s", C.GoString(C.cudaGetErrorString(rc)))
	}
	return nil
}

// allocRDMAGPU allocates a CUDA device buffer of size bytes. The
// returned rdmaBuf carries the device pointer in ptr; the NIC GPU-
// Direct RDMA-writes / reads into it via minio-go's RDMA dispatch.
func allocRDMAGPU(size int) (*rdmaBuf, error) {
	if size <= 0 {
		return &rdmaBuf{mode: RDMAModeGPU}, nil
	}
	var devPtr unsafe.Pointer
	if rc := C.cudaMalloc(&devPtr, C.size_t(size)); rc != 0 {
		return nil, fmt.Errorf("cudaMalloc(%d): %s", size, C.GoString(C.cudaGetErrorString(rc)))
	}
	return &rdmaBuf{ptr: devPtr, size: size, mode: RDMAModeGPU}, nil
}

// stageToGPU reads n bytes from src into a CPU bounce buffer, then
// cudaMemcpys host-to-device into the registered GPU buffer. The bounce
// buffer is allocated per call at n bytes; for very large objects this
// means the full object size is resident in host memory during staging.
func stageToGPU(b *rdmaBuf, src io.Reader, n int) error {
	if b == nil || n <= 0 || src == nil {
		return nil
	}
	host := make([]byte, n)
	if _, err := io.ReadFull(src, host); err != nil {
		return err
	}
	rc := C.cudaMemcpy(b.ptr,
		unsafe.Pointer(&host[0]),
		C.size_t(n),
		C.cudaMemcpyHostToDevice)
	if rc != 0 {
		return fmt.Errorf("cudaMemcpy H2D: %s", C.GoString(C.cudaGetErrorString(rc)))
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
