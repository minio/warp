#!/usr/bin/env bash
#
# Build warp with S3-over-RDMA support.
#
# Builds libminiocpp with RDMA enabled, then builds warp against it and
# packages a tarball: the binary resolves its bundled libminiocpp and libs3rdma
# through an $ORIGIN/lib rpath, so those need no LD_LIBRARY_PATH or system-wide
# install.
#
# The target host still supplies the rest of the RDMA stack -- libibverbs,
# librdmacm, libnuma and the vendor provider such as libmlx5 -- because those
# are tied to its kernel driver. --rdma=gpu additionally needs a CUDA runtime
# the installed NVIDIA driver supports.
#
# The result supports both --rdma=cpu and --rdma=gpu. CUDA is loaded with
# dlopen at run time rather than linked, so the same binary runs on hosts
# without CUDA and needs no CUDA packages to build.
#
# Usage:
#   scripts/build-rdma.sh [--version VERSION] [--out DIR]
#
#   --version    version string baked into `warp --version` (default: git describe)
#   --out        output directory (default: ./dist-rdma)
#
# Build prerequisites (Debian/Ubuntu):
#   apt-get install -y cmake g++ git libibverbs-dev librdmacm-dev libnuma-dev
#   plus Go. No CUDA package is needed, even for --rdma=gpu support.

set -euo pipefail

VERSION=""
OUT=""
WORK=""

need_value() {
	[ $# -ge 2 ] || {
		echo "$1 requires a value" >&2
		exit 1
	}
}

while [ $# -gt 0 ]; do
	case "$1" in
	--version)
		need_value "$@"
		VERSION="$2"
		shift
		;;
	--out)
		need_value "$@"
		OUT="$2"
		shift
		;;
	--work)
		need_value "$@"
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

# libs3rdma is picked by the host architecture, so the Go build has to target it
# too. Derive both from uname rather than the ambient GOARCH/GOOS, which would
# otherwise mislabel the archive or cross-build against host libs.
case "$(uname -m)" in
x86_64)
	S3RDMA_ARCH=x86_64
	GOARCH=amd64
	;;
aarch64 | arm64)
	S3RDMA_ARCH=aarch64
	GOARCH=arm64
	;;
*)
	echo "unsupported architecture: $(uname -m) (RDMA builds are linux/amd64 and linux/arm64 only)" >&2
	exit 1
	;;
esac
export GOARCH GOOS=linux

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
# A commit, not a tag: the RDMA transport moved to libs3rdma after v0.5.0 and no
# release carries it yet. Move this to the tag once one does. init + fetch
# rather than `clone --branch`, which only accepts a branch or tag.
if [ ! -d "${WORK}/minio-cpp" ]; then
	git init -q "${WORK}/minio-cpp"
	git -C "${WORK}/minio-cpp" remote add origin \
		"${MINIO_CPP_REPO:-https://github.com/minio/minio-cpp}"
	git -C "${WORK}/minio-cpp" fetch --depth 1 origin \
		"${MINIO_CPP_REF:-1fc511519a6f9ff55420d25cdf14b1ab3f764690}"
	git -C "${WORK}/minio-cpp" checkout --detach FETCH_HEAD
fi

echo ">>> building libminiocpp with RDMA"
"${WORK}/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
(
	cd "${WORK}/minio-cpp"
	"${WORK}/vcpkg/vcpkg" install
	cmake . -B ./build \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_SHARED_LIBS=OFF \
		-DCMAKE_INSTALL_PREFIX="${PREFIX}" \
		-DCMAKE_TOOLCHAIN_FILE="${WORK}/vcpkg/scripts/buildsystems/vcpkg.cmake" \
		-DMINIO_CPP_ENABLE_RDMA:BOOL=ON
	cmake --build ./build --config Release -j "$(nproc)"
	cmake --install ./build
	mkdir -p "${PREFIX}/lib"
	cp -P vendor/s3rdma/lib/"${S3RDMA_ARCH}"/* "${PREFIX}/lib/"
	# Collect vcpkg's static dependencies next to libminiocpp.a so the whole
	# static link resolves from one -L. cmake --install only places libminiocpp.
	cp -P vcpkg_installed/*/lib/*.a "${PREFIX}/lib/"
)

TAGS="kqueue,rdma"
NAME="warp-rdma"

# Static libminiocpp plus its transitive vcpkg archives, then libs3rdma. Kept in
# one file because .goreleaser/qreleaser.yaml and the CI workflows have to link
# exactly the same way.
RDMA_LINK_LIBS="$(cat "${REPO_DIR}/scripts/rdma-cgo-libs.txt")"

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
# inherited by a bundled library's own dependencies, which is how anything
# libs3rdma pulls in by DT_NEEDED is found without LD_LIBRARY_PATH. It resolves
# libibverbs from the host and libcuda through dlopen, so this matters less than
# the rpath on the binary itself, but the two have to agree.
LDFLAGS="${LDFLAGS} -extldflags=-Wl,-rpath,\$ORIGIN/lib,--disable-new-dtags"

(
	cd "${REPO_DIR}"
	# libminiocpp is static, so cgo -- which links with gcc, not g++ -- has to be
	# told about the C++ runtime and every transitive vcpkg archive by name. Only
	# libs3rdma stays dynamic; minio-cpp vendors it as a shared object.
	# Pre-set CGO_CFLAGS/CGO_LDFLAGS are kept, so a CUDA install outside the
	# default search paths can be pointed at with -I/-L.
	CGO_ENABLED=1 \
		CGO_CFLAGS="${CGO_CFLAGS:-} -I${PREFIX}/include" \
		CGO_LDFLAGS="${CGO_LDFLAGS:-} -L${PREFIX}/lib -Wl,-rpath-link,${PREFIX}/lib ${RDMA_LINK_LIBS}" \
		go build -trimpath -tags="${TAGS}" \
		-ldflags "${LDFLAGS}" -o "${STAGE}/warp" .
)

# libminiocpp is linked statically, so only libs3rdma is bundled: minio-cpp
# vendors it as a shared object with no static form.
cp -P "${PREFIX}"/lib/libs3rdma.so* "${STAGE}/lib/"
cp "${REPO_DIR}/README.md" "${REPO_DIR}/LICENSE" "${STAGE}/"

TARBALL="${OUT}/${NAME}_linux_${GOARCH}.tar.gz"
# Normalized entry order, timestamps and ownership, and gzip without its own
# timestamp, so identical inputs produce an identical archive and checksum.
tar --sort=name --mtime="@0" --owner=0 --group=0 --numeric-owner \
	-cf - -C "$(dirname "${STAGE}")" "${NAME}" | gzip -n >"${TARBALL}"
(cd "${OUT}" && sha256sum "$(basename "${TARBALL}")" >"$(basename "${TARBALL}").sha256sum")

echo ">>> ${TARBALL}"
"${STAGE}/warp" --version
