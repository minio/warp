//go:build rdma

/*
 * Warp (C) 2019-2026 MinIO, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package bench

// The CUDA runtime is resolved with dlopen rather than linked, so one warp
// binary serves both --rdma=cpu and --rdma=gpu. Linking libcudart would put it
// in DT_NEEDED and stop the binary from starting at all on a host without CUDA,
// which is most hosts that only want CPU-mode RDMA. Declaring the entry points
// here also means no CUDA headers are needed to build.

// #cgo LDFLAGS: -ldl
// #include <dlfcn.h>
// #include <stddef.h>
//
// typedef int (*warp_set_device_fn)(int);
// typedef int (*warp_malloc_fn)(void **, size_t);
// typedef int (*warp_free_fn)(void *);
// typedef int (*warp_memcpy_fn)(void *, const void *, size_t, int);
// typedef const char *(*warp_error_string_fn)(int);
// typedef int (*warp_get_version_fn)(int *);
//
// static void *warp_cudart;
// static warp_set_device_fn warp_set_device;
// static warp_malloc_fn warp_malloc;
// static warp_free_fn warp_free;
// static warp_malloc_fn warp_malloc_host;
// static warp_free_fn warp_free_host;
// static warp_memcpy_fn warp_memcpy;
// static warp_error_string_fn warp_error_string;
//
// // The lowest runtime version rejected as too new, and what the driver
// // supports, so the CLI can say why --rdma=gpu is unavailable. Lowest, so a
// // host carrying several runtimes is told about the nearest miss and hence
// // the smallest driver upgrade that would help.
// static int warp_cuda_rejected_runtime;
// static int warp_cuda_driver;
//
// // warp_cuda_try opens one candidate and keeps it only if the installed
// // driver is new enough to run it. A host can carry several runtimes, and
// // loading one the driver cannot support fails every later call with
// // "CUDA driver version is insufficient for CUDA runtime version".
// static int warp_cuda_try(const char *soname) {
//     warp_get_version_fn driver_version, runtime_version;
//     int driver = 0, runtime = 0;
//     void *h = dlopen(soname, RTLD_LAZY | RTLD_LOCAL);
//     if (h == NULL) {
//         return 0;
//     }
//     warp_set_device = (warp_set_device_fn)dlsym(h, "cudaSetDevice");
//     warp_malloc = (warp_malloc_fn)dlsym(h, "cudaMalloc");
//     warp_free = (warp_free_fn)dlsym(h, "cudaFree");
//     warp_malloc_host = (warp_malloc_fn)dlsym(h, "cudaMallocHost");
//     warp_free_host = (warp_free_fn)dlsym(h, "cudaFreeHost");
//     warp_memcpy = (warp_memcpy_fn)dlsym(h, "cudaMemcpy");
//     warp_error_string = (warp_error_string_fn)dlsym(h, "cudaGetErrorString");
//     driver_version = (warp_get_version_fn)dlsym(h, "cudaDriverGetVersion");
//     runtime_version = (warp_get_version_fn)dlsym(h, "cudaRuntimeGetVersion");
//     if (warp_set_device == NULL || warp_malloc == NULL || warp_free == NULL ||
//         warp_malloc_host == NULL || warp_free_host == NULL ||
//         warp_memcpy == NULL || warp_error_string == NULL ||
//         driver_version == NULL || runtime_version == NULL) {
//         dlclose(h);
//         return 0;
//     }
//     if (driver_version(&driver) != 0 || runtime_version(&runtime) != 0 ||
//         driver < runtime) {
//         if (runtime > 0 &&
//             (warp_cuda_rejected_runtime == 0 || runtime < warp_cuda_rejected_runtime)) {
//             warp_cuda_rejected_runtime = runtime;
//             warp_cuda_driver = driver;
//         }
//         warp_set_device = NULL;
//         warp_malloc = NULL;
//         warp_free = NULL;
//         warp_malloc_host = NULL;
//         warp_free_host = NULL;
//         warp_memcpy = NULL;
//         warp_error_string = NULL;
//         dlclose(h);
//         return 0;
//     }
//     warp_cudart = h;
//     return 1;
// }
//
// static int warp_cuda_load(void) {
//     static const char *sonames[] = {
//         "libcudart.so.13", "libcudart.so.12", "libcudart.so.11.0",
//         "libcudart.so", NULL,
//     };
//     int i;
//     if (warp_cudart != NULL) {
//         return 1;
//     }
//     for (i = 0; sonames[i] != NULL; i++) {
//         if (warp_cuda_try(sonames[i])) {
//             return 1;
//         }
//     }
//     return 0;
// }
//
// static int warp_cuda_rejected(void) { return warp_cuda_rejected_runtime; }
// static int warp_cuda_driver_version(void) { return warp_cuda_driver; }
//
// static int warp_cuda_set_device(int device) { return warp_set_device(device); }
// static int warp_cuda_malloc(void **p, size_t n) { return warp_malloc(p, n); }
// static int warp_cuda_free(void *p) { return warp_free(p); }
// static int warp_cuda_malloc_host(void **p, size_t n) { return warp_malloc_host(p, n); }
// static int warp_cuda_free_host(void *p) { return warp_free_host(p); }
// // 1 is cudaMemcpyHostToDevice, fixed by the CUDA runtime ABI.
// static int warp_cuda_memcpy_h2d(void *dst, const void *src, size_t n) {
//     return warp_memcpy(dst, src, n, 1);
// }
// static const char *warp_cuda_error(int rc) { return warp_error_string(rc); }
import "C"

import (
	"fmt"
	"io"
	"sync"
	"unsafe"
)

// HasRDMA reports whether warp was built with -tags=rdma, i.e. whether
// S3-over-RDMA dispatch is available. It requires libminiocpp at build and
// run time.
const HasRDMA = true

var cudaLoad = sync.OnceValue(func() bool { return C.warp_cuda_load() != 0 })

// HasRDMAGPU reports whether the CUDA runtime could be loaded, i.e. whether
// --rdma=gpu can work on this host. Unlike HasRDMA this is a runtime property:
// the same binary runs with and without CUDA present.
func HasRDMAGPU() bool { return cudaLoad() }

// RDMAGPUUnavailable explains why HasRDMAGPU is false. A host carrying a CUDA
// runtime newer than its driver is the common case, and it is worth naming:
// the fix is a driver upgrade, not installing CUDA.
func RDMAGPUUnavailable() string {
	if cudaLoad() {
		return ""
	}
	if rejected := int(C.warp_cuda_rejected()); rejected > 0 {
		return fmt.Sprintf("the oldest CUDA runtime on this host is %s, but the NVIDIA driver only supports up to %s; upgrade the driver or install a matching runtime",
			cudaVersion(rejected), cudaVersion(int(C.warp_cuda_driver_version())))
	}
	return "no usable CUDA runtime (libcudart) was found on this host"
}

// cudaVersion renders CUDA's packed version integer, e.g. 12070 as "12.7".
func cudaVersion(v int) string {
	if v <= 0 {
		return "none"
	}
	return fmt.Sprintf("%d.%d", v/1000, (v%1000)/10)
}

func cudaErr(rc C.int) string { return C.GoString(C.warp_cuda_error(rc)) }

// bindGPUThread makes the primary CUDA context current on the calling OS
// thread. This dates from the cuFile transport, where cuFileBufRegister
// resolved the device through cuCtxGetDevice and failed with
// CUDA_ERROR_INVALID_CONTEXT on any thread that had never issued a CUDA call —
// and cgo runs a goroutine's calls on whatever thread it likes, so registration
// landed on a context-less thread and the whole transfer fell back. Callers
// must runtime.LockOSThread() first so the binding still holds when the RDMA
// dispatch reaches the transport.
//
// libs3rdma registers with ibv_reg_mr and classifies the pointer through the
// CUDA driver API, so it may not need a bound context at all. That has not been
// measured; the binding is cheap and it stays until someone does.
func bindGPUThread() error {
	if !cudaLoad() {
		return errRDMAGPUUnsupported
	}
	if rc := C.warp_cuda_set_device(0); rc != 0 {
		return fmt.Errorf("cudaSetDevice(0): %s", cudaErr(rc))
	}
	// cudaSetDevice defers context creation; force it now so the first
	// registration on this thread already has a current context.
	if rc := C.warp_cuda_free(nil); rc != 0 {
		return fmt.Errorf("cudaFree(nil): %s", cudaErr(rc))
	}
	return nil
}

// allocRDMAGPU allocates a CUDA device buffer of size bytes, plus the pinned
// host buffer that PUT stages through. The returned rdmaBuf carries the device
// pointer in ptr; the NIC GPU-Direct RDMA-writes / reads into it via minio-go's
// RDMA dispatch.
//
// The staging buffer is page-locked because cudaMemcpy out of pageable memory
// cannot DMA directly -- the runtime copies through its own internal pinned
// buffers first. Allocating it here, with the pool, keeps both the allocation
// and the page-locking off the timed path. GET leaves it unused: the server
// writes to the device pointer.
func allocRDMAGPU(size int) (*rdmaBuf, error) {
	if !cudaLoad() {
		return nil, errRDMAGPUUnsupported
	}
	if size <= 0 {
		return &rdmaBuf{mode: RDMAModeGPU}, nil
	}
	var devPtr unsafe.Pointer
	if rc := C.warp_cuda_malloc(&devPtr, C.size_t(size)); rc != 0 {
		return nil, fmt.Errorf("cudaMalloc(%d): %s", size, cudaErr(rc))
	}
	var hostPtr unsafe.Pointer
	if rc := C.warp_cuda_malloc_host(&hostPtr, C.size_t(size)); rc != 0 {
		C.warp_cuda_free(devPtr)
		return nil, fmt.Errorf("cudaMallocHost(%d): %s", size, cudaErr(rc))
	}
	return &rdmaBuf{ptr: devPtr, host: hostPtr, size: size, mode: RDMAModeGPU}, nil
}

// stageToGPU reads n bytes from src into the buffer's pinned host staging
// area, then cudaMemcpys host-to-device into the registered GPU buffer. Both
// buffers come from allocRDMAGPU and are reused for the life of the worker.
func stageToGPU(b *rdmaBuf, src io.Reader, n int) error {
	if b == nil || n <= 0 || src == nil {
		return nil
	}
	if b.host == nil {
		return fmt.Errorf("rdma: gpu buffer of %d bytes has no host staging buffer", b.size)
	}
	if _, err := io.ReadFull(src, unsafe.Slice((*byte)(b.host), n)); err != nil {
		return err
	}
	rc := C.warp_cuda_memcpy_h2d(b.ptr, b.host, C.size_t(n))
	if rc != 0 {
		return fmt.Errorf("cudaMemcpy H2D: %s", cudaErr(rc))
	}
	return nil
}

// freeRDMAGPU releases the CUDA device buffer and its pinned host staging
// buffer.
func freeRDMAGPU(b *rdmaBuf) {
	if b == nil {
		return
	}
	if b.host != nil {
		C.warp_cuda_free_host(b.host)
		b.host = nil
	}
	if b.ptr == nil {
		return
	}
	C.warp_cuda_free(b.ptr)
	b.ptr = nil
}
