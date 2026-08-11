#!/usr/bin/env bash
#
# Provision a release host so qreleaser can build the `warp-rdma` target.
#
# The RDMA build links libminiocpp through cgo, so it cannot be cross-compiled
# and the toolchain has to exist on the release host itself -- the qreleaser
# checkout is wiped per run, so this cannot live in the build. Run it once per
# host (again when the pinned minio-cpp ref moves). It is idempotent.
#
# Installs into ${PREFIX} (default /usr/local, which is what
# .goreleaser/qreleaser.yaml and scripts/release-post-transform.sh look for
# when MINIOCPP_PREFIX is unset):
#
#   - libminiocpp.a built with RDMA enabled, plus its headers
#   - the vcpkg static archives it needs to link (scripts/rdma-cgo-libs.txt)
#   - the vendored cuObj/cuFile shared objects, which the release packaging
#     copies out of the prefix
#
# The host must already supply the RDMA stack itself -- libibverbs, librdmacm
# and libnuma with their -dev packages -- because those are tied to its kernel
# driver. No CUDA package is needed: CUDA is dlopened at run time.
#
# Usage:
#   scripts/setup-rdma-release-host.sh [--prefix DIR] [--work DIR] [--verify-only]
#
# Any existing libminiocpp in the prefix is moved aside to a timestamped
# backup directory rather than overwritten.

set -euo pipefail

PREFIX="${PREFIX:-/usr/local}"
WORK="${WORK:-${HOME}/.warp-rdma-host}"
VERIFY_ONLY=0

need_value() {
	[ $# -ge 2 ] || {
		echo "$1 requires a value" >&2
		exit 1
	}
}

while [ $# -gt 0 ]; do
	case "$1" in
	--prefix)
		need_value "$@"
		PREFIX="$2"
		shift
		;;
	--work)
		need_value "$@"
		WORK="$2"
		shift
		;;
	--verify-only)
		VERIFY_ONLY=1
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

case "$(uname -s)" in
Linux) ;;
*)
	echo "RDMA release builds are Linux only (got $(uname -s))" >&2
	exit 1
	;;
esac

case "$(uname -m)" in
x86_64) CUOBJ_ARCH=x86_64 ;;
aarch64 | arm64) CUOBJ_ARCH=aarch64 ;;
*)
	echo "unsupported architecture: $(uname -m) (RDMA builds are linux/amd64 and linux/arm64 only)" >&2
	exit 1
	;;
esac

# Pinned in lockstep with scripts/build-rdma.sh and .github/workflows/go-rdma.yml:
# a floating minio-cpp is what leaves a host with headers too old for the
# minio-go revision in go.mod, and vcpkg's port scripts track the newest CMake.
MINIO_CPP_REF="${MINIO_CPP_REF:-v0.5.0}"
MINIO_CPP_REPO="${MINIO_CPP_REPO:-https://github.com/minio/minio-cpp}"
VCPKG_REF="${VCPKG_REF:-2026.07.29}"
CMAKE_MIN="3.31"
CMAKE_VERSION="${CMAKE_VERSION:-3.31.6}"

as_root() {
	if [ -w "${PREFIX}" ]; then
		"$@"
	else
		sudo "$@"
	fi
}

# Assert the prefix can satisfy the exact link line qreleaser composes, so a
# half-provisioned host fails here in seconds instead of 20 minutes into a
# release.
verify_prefix() {
	local missing=0 lib name

	if [ ! -f "${PREFIX}/include/miniocpp/c_api.h" ]; then
		echo "MISSING: ${PREFIX}/include/miniocpp/c_api.h (libminiocpp too old for the minio-go revision in go.mod)" >&2
		missing=1
	fi

	for lib in $(tr ' ' '\n' <"${REPO_DIR}/scripts/rdma-cgo-libs.txt" | sed -n 's/^-l//p'); do
		case "${lib}" in
		# Supplied by the toolchain and the host RDMA stack, not the prefix.
		stdc++ | m | dl | pthread) continue ;;
		esac
		name="lib${lib}"
		if ! compgen -G "${PREFIX}/lib/${name}.a" >/dev/null &&
			! compgen -G "${PREFIX}/lib/${name}.so*" >/dev/null; then
			echo "MISSING: ${PREFIX}/lib/${name}.{a,so}" >&2
			missing=1
		fi
	done

	# scripts/release-post-transform.sh copies these out of the prefix by name.
	for name in libcuobjclient.so libcufile.so libcufile_rdma.so; do
		if ! compgen -G "${PREFIX}/lib/${name}*" >/dev/null; then
			echo "MISSING: ${PREFIX}/lib/${name}* (needed by release packaging)" >&2
			missing=1
		fi
	done

	[ "${missing}" = 0 ] || return 1
	echo ">>> ${PREFIX} satisfies the RDMA link line"
}

if [ "${VERIFY_ONLY}" = 1 ]; then
	verify_prefix
	exit 0
fi

echo ">>> installing build dependencies"
if command -v apt-get >/dev/null 2>&1; then
	sudo apt-get -qq update || true
	sudo apt-get -o DPkg::Lock::Timeout=600 -qy install --no-install-recommends \
		build-essential git curl zip unzip tar pkg-config \
		libibverbs-dev librdmacm-dev libnuma-dev
else
	echo "no apt-get; ensure a C++ toolchain and libibverbs/librdmacm/libnuma -dev are installed" >&2
fi

mkdir -p "${WORK}"

# Distribution CMake is routinely older than vcpkg needs. Keep the newer one in
# the work directory and on PATH for this run only, rather than upgrading a
# package other builds on a shared host depend on.
host_cmake="$(cmake --version 2>/dev/null | head -1 | awk '{print $3}' || true)"
if [ -z "${host_cmake}" ] ||
	[ "$(printf '%s\n%s\n' "${host_cmake%.*}" "${CMAKE_MIN}" | sort -V | head -1)" != "${CMAKE_MIN}" ]; then
	cmake_dir="${WORK}/cmake-${CMAKE_VERSION}-linux-${CUOBJ_ARCH}"
	if [ ! -x "${cmake_dir}/bin/cmake" ]; then
		echo ">>> cmake ${host_cmake:-none} is older than ${CMAKE_MIN}; fetching ${CMAKE_VERSION}"
		curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CUOBJ_ARCH}.tar.gz" |
			tar -xz -C "${WORK}"
	fi
	PATH="${cmake_dir}/bin:${PATH}"
	export PATH
fi
echo ">>> using cmake $(cmake --version | head -1 | awk '{print $3}')"

if [ ! -d "${WORK}/vcpkg" ]; then
	git clone --depth 1 --branch "${VCPKG_REF}" https://github.com/microsoft/vcpkg "${WORK}/vcpkg"
fi
if [ ! -d "${WORK}/minio-cpp" ]; then
	git clone --depth 1 --branch "${MINIO_CPP_REF}" "${MINIO_CPP_REPO}" "${WORK}/minio-cpp"
fi

echo ">>> building libminiocpp ${MINIO_CPP_REF} with RDMA"
"${WORK}/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
cd "${WORK}/minio-cpp"
"${WORK}/vcpkg/vcpkg" install
cmake . -B ./build \
	-DCMAKE_BUILD_TYPE=Release \
	-DBUILD_SHARED_LIBS=OFF \
	-DCMAKE_INSTALL_PREFIX="${PREFIX}" \
	-DCMAKE_TOOLCHAIN_FILE="${WORK}/vcpkg/scripts/buildsystems/vcpkg.cmake" \
	-DMINIO_CPP_ENABLE_RDMA:BOOL=ON
cmake --build ./build --config Release -j "$(nproc)"

# An older install left in place would keep its stale headers first on the
# include path, so move the whole thing aside instead of installing over it.
if [ -e "${PREFIX}/include/miniocpp" ] || compgen -G "${PREFIX}/lib/libminiocpp.*" >/dev/null; then
	backup="${PREFIX}/lib/warp-rdma-backup-$(date -u +%Y%m%dT%H%M%SZ)"
	echo ">>> moving the existing libminiocpp aside to ${backup}"
	as_root mkdir -p "${backup}"
	[ ! -e "${PREFIX}/include/miniocpp" ] || as_root mv "${PREFIX}/include/miniocpp" "${backup}/"
	for f in "${PREFIX}"/lib/libminiocpp.*; do
		[ -e "${f}" ] || continue
		as_root mv "${f}" "${backup}/"
	done
fi

echo ">>> installing into ${PREFIX}"
as_root cmake --install ./build
as_root mkdir -p "${PREFIX}/lib"
# The cuObj/cuFile libraries ship prebuilt in the checkout and have no static
# form; libminiocpp is static, so its transitive vcpkg archives have to sit
# beside it for the cgo link to resolve from one -L.
as_root cp -P vendor/cuobj/lib/"${CUOBJ_ARCH}"/* "${PREFIX}/lib/"
as_root cp -P vcpkg_installed/*/lib/*.a "${PREFIX}/lib/"
command -v ldconfig >/dev/null 2>&1 && as_root ldconfig || true

verify_prefix

# Same flags .goreleaser/qreleaser.yaml composes, so a link that works here
# works in the release.
if command -v go >/dev/null 2>&1 && [ -f "${REPO_DIR}/go.mod" ]; then
	echo ">>> smoke building warp with -tags=rdma"
	cd "${REPO_DIR}"
	CGO_ENABLED=1 \
		CGO_CFLAGS="-I${PREFIX}/include" \
		CGO_LDFLAGS="-L${PREFIX}/lib -Wl,-rpath-link,${PREFIX}/lib -Wl,-rpath,/usr/lib/warp -Wl,--enable-new-dtags $(cat "${REPO_DIR}/scripts/rdma-cgo-libs.txt")" \
		go build -trimpath -tags=kqueue,rdma -o "${WORK}/warp-rdma-smoke" .
	"${WORK}/warp-rdma-smoke" --version
fi

echo ">>> release host provisioned; qreleaser can build warp-rdma against ${PREFIX}"
