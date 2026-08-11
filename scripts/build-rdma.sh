#!/usr/bin/env bash
#
# Build warp with S3-over-RDMA support.
#
# Builds libminiocpp with RDMA enabled, then builds warp against it and
# packages a self-contained tarball: the binary resolves its bundled
# libminiocpp/cuObj shared objects through an $ORIGIN/lib rpath, so no
# LD_LIBRARY_PATH or system-wide install is needed on the target host.
#
# Usage:
#   scripts/build-rdma.sh [--gpu] [--version VERSION] [--out DIR]
#
#   --gpu        also link CUDA (adds -tags=cuda) so --rdma=gpu works.
#                Requires the CUDA runtime dev files at build time and the
#                CUDA runtime + driver on the machine running the benchmark.
#   --version    version string baked into `warp --version` (default: git describe)
#   --out        output directory (default: ./dist-rdma)
#
# Prerequisites (Debian/Ubuntu):
#   apt-get install -y cmake g++ git libibverbs-dev librdmacm-dev libnuma-dev
#   plus Go, and for --gpu the cuda-cudart-dev package.

set -euo pipefail

GPU=0
VERSION=""
OUT=""
WORK=""

while [ $# -gt 0 ]; do
	case "$1" in
	--gpu) GPU=1 ;;
	--version)
		VERSION="$2"
		shift
		;;
	--out)
		OUT="$2"
		shift
		;;
	--work)
		WORK="$2"
		shift
		;;
	-h | --help)
		sed -n '2,25p' "$0"
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 1
		;;
	esac
	shift
done

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-${REPO_DIR}/dist-rdma}"
WORK="${WORK:-${REPO_DIR}/.rdma-build}"
VERSION="${VERSION:-$(git -C "${REPO_DIR}" describe --tags --always --dirty 2>/dev/null || echo dev)}"

PREFIX="${WORK}/prefix"
mkdir -p "${WORK}" "${PREFIX}"

case "$(uname -m)" in
x86_64) CUOBJ_ARCH=x86_64 ;;
aarch64 | arm64) CUOBJ_ARCH=aarch64 ;;
*)
	echo "unsupported architecture: $(uname -m) (RDMA builds are linux/amd64 and linux/arm64 only)" >&2
	exit 1
	;;
esac

# vcpkg is pinned: its port scripts track the newest CMake, and a floating
# checkout breaks the build on whatever CMake the distribution ships.
VCPKG_REF="${VCPKG_REF:-2026.07.29}"
CMAKE_VERSION="$(cmake --version | head -1 | awk '{print $3}')"
if [ "$(printf '%s\n3.31\n' "${CMAKE_VERSION%.*}" | sort -V | head -1)" != "3.31" ]; then
	echo "cmake ${CMAKE_VERSION} is too old; vcpkg ${VCPKG_REF} needs 3.31 or newer" >&2
	exit 1
fi

if [ ! -d "${WORK}/vcpkg" ]; then
	git clone --depth 1 --branch "${VCPKG_REF}" https://github.com/microsoft/vcpkg "${WORK}/vcpkg"
fi
if [ ! -d "${WORK}/minio-cpp" ]; then
	git clone --depth 1 --branch "${MINIO_CPP_REF:-main}" \
		"${MINIO_CPP_REPO:-https://github.com/minio/minio-cpp}" "${WORK}/minio-cpp"
fi

echo ">>> building libminiocpp with RDMA"
"${WORK}/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
(
	cd "${WORK}/minio-cpp"
	"${WORK}/vcpkg/vcpkg" install
	cmake . -B ./build \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_SHARED_LIBS=ON \
		-DCMAKE_INSTALL_PREFIX="${PREFIX}" \
		-DCMAKE_TOOLCHAIN_FILE="${WORK}/vcpkg/scripts/buildsystems/vcpkg.cmake" \
		-DMINIO_CPP_ENABLE_RDMA:BOOL=ON
	cmake --build ./build --config Release -j "$(nproc)"
	cmake --install ./build
	mkdir -p "${PREFIX}/lib"
	cp -P vendor/cuobj/lib/"${CUOBJ_ARCH}"/* "${PREFIX}/lib/"
)

TAGS="kqueue,rdma"
NAME="warp-rdma"
if [ "${GPU}" = "1" ]; then
	TAGS="${TAGS},cuda"
	NAME="warp-rdma-gpu"
fi

STAGE="${WORK}/stage/${NAME}"
rm -rf "${STAGE}"
mkdir -p "${STAGE}/lib" "${OUT}"

echo ">>> building warp (tags: ${TAGS})"
COMMIT="$(git -C "${REPO_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)"
LDFLAGS="-s -w"
LDFLAGS="${LDFLAGS} -X github.com/minio/warp/pkg.ReleaseTag=${VERSION}"
LDFLAGS="${LDFLAGS} -X github.com/minio/warp/pkg.Version=${VERSION}"
LDFLAGS="${LDFLAGS} -X github.com/minio/warp/pkg.CommitID=${COMMIT}"
LDFLAGS="${LDFLAGS} -X github.com/minio/warp/pkg.ShortCommitID=${COMMIT:0:12}"
# --disable-new-dtags emits DT_RPATH rather than DT_RUNPATH. Only DT_RPATH is
# inherited by the dependencies of libminiocpp, which is how the bundled cuObj
# libraries get found without LD_LIBRARY_PATH.
LDFLAGS="${LDFLAGS} -extldflags=-Wl,-rpath,\$ORIGIN/lib,--disable-new-dtags"

(
	cd "${REPO_DIR}"
	# -rpath-link lets the linker resolve libminiocpp's own cuObj dependencies;
	# -L alone is not consulted for a shared library's DT_NEEDED entries.
	# Pre-set CGO_CFLAGS/CGO_LDFLAGS are kept, so a CUDA install outside the
	# default search paths can be pointed at with -I/-L.
	CGO_ENABLED=1 \
		CGO_CFLAGS="${CGO_CFLAGS:-} -I${PREFIX}/include" \
		CGO_LDFLAGS="${CGO_LDFLAGS:-} -L${PREFIX}/lib -Wl,-rpath-link,${PREFIX}/lib" \
		go build -trimpath -tags="${TAGS}" \
		-ldflags "${LDFLAGS}" -o "${STAGE}/warp" .
)

cp -P "${PREFIX}"/lib/libminiocpp.so* "${STAGE}/lib/"
cp -P "${PREFIX}"/lib/libcufile*.so* "${PREFIX}"/lib/libcuobj*.so* "${STAGE}/lib/"
cp "${REPO_DIR}/README.md" "${REPO_DIR}/LICENSE" "${STAGE}/"

GOARCH="$(go env GOARCH)"
TARBALL="${OUT}/${NAME}_linux_${GOARCH}.tar.gz"
tar -czf "${TARBALL}" -C "$(dirname "${STAGE}")" "${NAME}"
(cd "${OUT}" && sha256sum "$(basename "${TARBALL}")" >"$(basename "${TARBALL}").sha256sum")

echo ">>> ${TARBALL}"
"${STAGE}/warp" --version
