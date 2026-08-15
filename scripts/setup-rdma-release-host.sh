#!/usr/bin/env bash
#
# Provision a release host so qreleaser can build the `warp-rdma` target.
#
# The RDMA build links libminiocpp through cgo, so it cannot be cross-compiled
# from Go alone -- the C++ side has to exist on the release host for every
# published architecture. The qreleaser checkout is wiped per run, so this cannot
# live in the build. Run it once per host (again when the pinned minio-cpp ref
# moves). It is idempotent.
#
# On an amd64 host this provisions two prefixes, matching the two architectures
# .goreleaser/qreleaser.yaml publishes:
#
#   amd64  ${PREFIX}                        (default /usr/local, built natively)
#   arm64  ${PREFIX}/aarch64-linux-gnu      (cross-built)
#
# Each holds libminiocpp.a built with RDMA enabled plus its headers, the vcpkg
# static archives it links against (scripts/rdma-cgo-libs.txt), and the vendored
# libs3rdma shared object the release packaging copies out. The arm64 prefix
# path is fixed rather than host-dependent because qreleaser.yaml has to name it
# in a static override.
#
# The host must already supply the RDMA stack itself -- libibverbs, librdmacm and
# libnuma with their -dev packages -- because those are tied to its kernel
# driver. Cross-building additionally needs those packages for arm64, which means
# dpkg foreign-architecture support; scripts/rdma-cross/ holds the cmake
# toolchain and vcpkg triplet used for it. No CUDA package is needed for either
# architecture: CUDA is dlopened at run time.
#
# Usage:
#   scripts/setup-rdma-release-host.sh [--prefix DIR] [--work DIR]
#                                      [--no-cross] [--verify-only]
#
#   --no-cross     provision only the host architecture
#   --verify-only  check the existing prefixes and exit
#
# Any existing libminiocpp in a prefix is moved aside to a timestamped backup
# directory rather than overwritten.

set -euo pipefail

PREFIX="${PREFIX:-/usr/local}"
WORK="${WORK:-${HOME}/.warp-rdma-host}"
VERIFY_ONLY=0
CROSS=1

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
	--no-cross)
		CROSS=0
		;;
	--verify-only)
		VERIFY_ONLY=1
		;;
	-h | --help)
		sed -n '2,38p' "$0"
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
CROSS_DIR="${REPO_DIR}/scripts/rdma-cross"

case "$(uname -s)" in
Linux) ;;
*)
	echo "RDMA release builds are Linux only (got $(uname -s))" >&2
	exit 1
	;;
esac

case "$(uname -m)" in
x86_64) HOST_ARCH=amd64 ;;
aarch64 | arm64) HOST_ARCH=arm64 ;;
*)
	echo "unsupported architecture: $(uname -m) (RDMA builds are linux/amd64 and linux/arm64 only)" >&2
	exit 1
	;;
esac

# Pinned in lockstep with scripts/build-rdma.sh and .github/workflows/go-rdma.yml:
# a floating minio-cpp is what leaves a host with headers too old for the
# minio-go revision in go.mod, and vcpkg's port scripts track the newest CMake.
# v0.6.0 is the first release carrying the libs3rdma RDMA transport.
MINIO_CPP_REF="${MINIO_CPP_REF:-v0.6.0}"
MINIO_CPP_REPO="${MINIO_CPP_REPO:-https://github.com/minio/minio-cpp}"
VCPKG_REF="${VCPKG_REF:-2026.07.29}"
CMAKE_MIN="3.31"
CMAKE_VERSION="${CMAKE_VERSION:-3.31.6}"

TARGETS=("${HOST_ARCH}")
if [ "${CROSS}" = 1 ]; then
	if [ "${HOST_ARCH}" = amd64 ]; then
		TARGETS+=(arm64)
	else
		echo ">>> cross-building amd64 from ${HOST_ARCH} is not supported; provisioning ${HOST_ARCH} only" >&2
	fi
else
	# qreleaser publishes both architectures from one build and goreleaser cannot
	# skip just one of them, so a half-provisioned release host fails the release.
	echo ">>> --no-cross: provisioning ${HOST_ARCH} only. A release host needs both" >&2
	echo ">>> architectures, or WARP_SKIP_RDMA=true to drop the RDMA build entirely" >&2
fi

arch_prefix() {
	case "$1" in
	# Kept at the bare prefix so an amd64-only host stays provisioned exactly as
	# it was before arm64 was published.
	amd64) echo "${PREFIX}" ;;
	arm64) echo "${PREFIX}/aarch64-linux-gnu" ;;
	esac
}

arch_uname() {
	case "$1" in
	amd64) echo x86_64 ;;
	arm64) echo aarch64 ;;
	esac
}

arch_triplet() {
	case "$1" in
	amd64) echo x64-linux ;;
	arm64) echo arm64-linux ;;
	esac
}

# What readelf reports for a correctly-targeted object, so a prefix full of
# host-architecture archives cannot pass verification. readelf rather than
# objdump: objdump only knows its own target and calls everything else UNKNOWN!,
# which would fail every cross prefix.
arch_machine() {
	case "$1" in
	amd64) echo "Advanced Micro Devices X86-64" ;;
	arm64) echo "AArch64" ;;
	esac
}

# Reads the ELF machine of an object, an archive member or an executable.
elf_machine() {
	readelf -h "$1" 2>/dev/null | sed -n 's/^ *Machine: *//p' | head -1
}

# scripts/rdma-cgo-libs.txt names three kinds of library: the toolchain's own,
# libs3rdma, which minio-cpp vendors as a shared object with no static form, and
# everything else, which must come from an archive. A published binary that
# picks one of the last group up dynamically has a runtime dependency the
# package neither declares nor bundles, and fails to start on a customer host.
static_libs() {
	local lib
	for lib in $(tr ' ' '\n' <"${REPO_DIR}/scripts/rdma-cgo-libs.txt" | sed -n 's/^-l//p'); do
		case "${lib}" in
		stdc++ | m | dl | pthread) ;;
		s3rdma) ;;
		*) echo "${lib}" ;;
		esac
	done
}

arch_cc() {
	case "$1" in
	arm64) [ "${HOST_ARCH}" = arm64 ] && echo gcc || echo aarch64-linux-gnu-gcc ;;
	amd64) [ "${HOST_ARCH}" = amd64 ] && echo gcc || echo x86_64-linux-gnu-gcc ;;
	esac
}

arch_cxx() {
	case "$1" in
	arm64) [ "${HOST_ARCH}" = arm64 ] && echo g++ || echo aarch64-linux-gnu-g++ ;;
	amd64) [ "${HOST_ARCH}" = amd64 ] && echo g++ || echo x86_64-linux-gnu-g++ ;;
	esac
}

run_privileged() {
	if [ "$(id -u)" = 0 ]; then
		"$@"
	else
		sudo "$@"
	fi
}

# Writes into the prefix need privilege only when it is not already ours. Every
# per-architecture prefix lives under PREFIX, so its writability decides for all.
as_root() {
	if [ -w "${PREFIX}" ]; then
		"$@"
	else
		run_privileged "$@"
	fi
}

# Assert a prefix can satisfy the exact link line qreleaser composes for one
# architecture, so a half-provisioned or wrong-architecture host fails here in
# seconds instead of 20 minutes into a release.
verify_prefix() {
	local arch="$1"
	local prefix machine missing=0 lib name found
	prefix="$(arch_prefix "${arch}")"
	machine="$(arch_machine "${arch}")"

	if [ ! -f "${prefix}/include/miniocpp/c_api.h" ]; then
		echo "MISSING: ${prefix}/include/miniocpp/c_api.h (libminiocpp too old for the minio-go revision in go.mod)" >&2
		missing=1
	fi

	for lib in $(static_libs); do
		name="lib${lib}"
		if ! compgen -G "${prefix}/lib/${name}.a" >/dev/null; then
			echo "MISSING: ${prefix}/lib/${name}.a (${arch})" >&2
			missing=1
		fi
		# ld prefers a shared object over an archive in the same -L, so one left
		# here by an older install silently changes how the release links.
		if compgen -G "${prefix}/lib/${name}.so*" >/dev/null; then
			echo "SHADOWED: ${prefix}/lib/${name}.so* would be linked instead of ${name}.a (${arch})" >&2
			missing=1
		fi
	done

	# scripts/release-post-transform.sh copies this out of the prefix by name.
	for name in libs3rdma.so; do
		if ! compgen -G "${prefix}/lib/${name}*" >/dev/null; then
			echo "MISSING: ${prefix}/lib/${name}* (${arch}, needed by release packaging)" >&2
			missing=1
		fi
	done

	# Name checks alone cannot tell a cross prefix from one holding host-built
	# archives, which is the failure a shared vcpkg checkout invites. Check both
	# libminiocpp and a vcpkg archive, since they are produced by separate builds.
	for name in libminiocpp.a libssl.a; do
		[ -f "${prefix}/lib/${name}" ] || continue
		found="$(elf_machine "${prefix}/lib/${name}")"
		if [ "${found}" != "${machine}" ]; then
			echo "WRONG ARCH: ${prefix}/lib/${name} is '${found}', expected '${machine}' for ${arch}" >&2
			missing=1
		fi
	done

	[ "${missing}" = 0 ] || return 1
	echo ">>> ${prefix} satisfies the RDMA link line for ${arch}"
}

if [ "${VERIFY_ONLY}" = 1 ]; then
	rc=0
	for arch in "${TARGETS[@]}"; do
		verify_prefix "${arch}" || rc=1
	done
	exit "${rc}"
fi

echo ">>> provisioning for: ${TARGETS[*]}"

echo ">>> installing build dependencies"
if command -v apt-get >/dev/null 2>&1; then
	run_privileged apt-get -qq update || true
	run_privileged apt-get -o DPkg::Lock::Timeout=600 -qy install --no-install-recommends \
		build-essential git curl zip unzip tar pkg-config \
		libibverbs-dev librdmacm-dev libnuma-dev

	if printf '%s\n' "${TARGETS[@]}" | grep -qx arm64 && [ "${HOST_ARCH}" != arm64 ]; then
		echo ">>> installing the aarch64 cross toolchain and arm64 RDMA libraries"
		run_privileged dpkg --add-architecture arm64
		# The amd64 archive carries no arm64 packages; they live on ports.ubuntu.com.
		# Its arm64 index then 404s harmlessly, so update stays tolerant.
		if [ ! -f /etc/apt/sources.list.d/arm64-ports.list ]; then
			distro="$(. /etc/os-release && echo "${ID:-}")"
			codename="$(. /etc/os-release && echo "${VERSION_CODENAME:-}")"
			# Only Ubuntu splits arm64 onto a separate mirror; Debian serves every
			# architecture from its own archive, so adding this there would mix
			# distributions on a host that keeps the file.
			if [ "${distro}" = ubuntu ] && [ -n "${codename}" ]; then
				printf 'deb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports %s main universe\ndeb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports %s-updates main universe\ndeb [arch=arm64] http://ports.ubuntu.com/ubuntu-ports %s-security main universe\n' \
					"${codename}" "${codename}" "${codename}" |
					run_privileged tee /etc/apt/sources.list.d/arm64-ports.list >/dev/null
			fi
		fi
		run_privileged apt-get -qq update || true
		# libibverbs-dev and friends are Multi-Arch: same -- install both arches
		# together so adding :arm64 cannot drop :amd64 and break native builds.
		run_privileged apt-get -o DPkg::Lock::Timeout=600 -qy install --no-install-recommends \
			gcc-aarch64-linux-gnu g++-aarch64-linux-gnu \
			libibverbs-dev:amd64 librdmacm-dev:amd64 libnuma-dev:amd64 \
			libibverbs-dev:arm64 librdmacm-dev:arm64 libnuma-dev:arm64
	fi
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
	cmake_dir="${WORK}/cmake-${CMAKE_VERSION}-linux-$(arch_uname "${HOST_ARCH}")"
	if [ ! -x "${cmake_dir}/bin/cmake" ]; then
		echo ">>> cmake ${host_cmake:-none} is older than ${CMAKE_MIN}; fetching ${CMAKE_VERSION}"
		curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-$(arch_uname "${HOST_ARCH}").tar.gz" |
			tar -xz -C "${WORK}"
	fi
	PATH="${cmake_dir}/bin:${PATH}"
	export PATH
fi
echo ">>> using cmake $(cmake --version | head -1 | awk '{print $3}')"

# Enforce the pins on every run: a checkout left behind at an older ref is how a
# host ends up building unpinned sources after the pin moves.
sync_checkout() {
	local dir="$1" repo="$2" ref="$3" before=""
	if [ -d "${dir}/.git" ]; then
		before="$(git -C "${dir}" rev-parse HEAD)"
		git -C "${dir}" remote set-url origin "${repo}"
		# A tracked file modified in place would make the checkout below refuse to
		# run. Untracked build output is left alone: it is what makes a rerun
		# incremental, and a moved ref drops it explicitly.
		git -C "${dir}" fetch --depth 1 origin "${ref}"
		git -C "${dir}" reset --hard -q HEAD
	else
		# init + fetch rather than `clone --branch`, so a ref here can be a
		# release tag or a commit. MINIO_CPP_REF selects either.
		rm -rf "${dir}"
		git init -q "${dir}"
		git -C "${dir}" remote add origin "${repo}"
		git -C "${dir}" fetch --depth 1 origin "${ref}"
	fi
	git -C "${dir}" checkout --detach FETCH_HEAD
	CHECKOUT_CHANGED=0
	[ "${before}" = "$(git -C "${dir}" rev-parse HEAD)" ] || CHECKOUT_CHANGED=1
}

sync_checkout "${WORK}/vcpkg" https://github.com/microsoft/vcpkg "${VCPKG_REF}"
"${WORK}/vcpkg/bootstrap-vcpkg.sh" -disableMetrics

# One checkout per architecture. Sharing one would put two triplets' artifacts in
# the same vcpkg_installed and build tree, and the install step copies archives
# out of there by glob.
build_target() {
	local arch="$1"
	local src prefix triplet uarch vcpkg_args=() cmake_args=()
	src="${WORK}/minio-cpp-${arch}"
	prefix="$(arch_prefix "${arch}")"
	triplet="$(arch_triplet "${arch}")"
	uarch="$(arch_uname "${arch}")"

	sync_checkout "${src}" "${MINIO_CPP_REPO}" "${MINIO_CPP_REF}"
	# Otherwise the install step copies the previous ref's artifacts into the prefix.
	if [ "${CHECKOUT_CHANGED}" = 1 ]; then
		rm -rf "${src}/build" "${src}/vcpkg_installed"
	fi

	# A CMakeCache holding a different absolute source path -- a work directory
	# that moved -- makes cmake refuse to configure at all.
	if [ -f "${src}/build/CMakeCache.txt" ] &&
		! grep -qx "CMAKE_HOME_DIRECTORY:INTERNAL=${src}" "${src}/build/CMakeCache.txt"; then
		echo ">>> dropping a stale ${arch} build directory from a previous path"
		rm -rf "${src}/build"
	fi

	if [ "${arch}" != "${HOST_ARCH}" ]; then
		vcpkg_args+=(--overlay-triplets="${CROSS_DIR}/triplets")
		cmake_args+=(
			-DVCPKG_OVERLAY_TRIPLETS="${CROSS_DIR}/triplets"
			-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE="${CROSS_DIR}/aarch64-linux-gnu.cmake"
		)
	fi

	echo ">>> building libminiocpp ${MINIO_CPP_REF} with RDMA for ${arch} (${triplet})"
	cd "${src}"
	# --host-triplet keeps vcpkg's own build tools native while the port tree is
	# built for the target.
	"${WORK}/vcpkg/vcpkg" install \
		--triplet "${triplet}" \
		--host-triplet "$(arch_triplet "${HOST_ARCH}")" \
		"${vcpkg_args[@]}"

	cmake . -B ./build \
		-DCMAKE_BUILD_TYPE=Release \
		-DBUILD_SHARED_LIBS=OFF \
		-DCMAKE_INSTALL_PREFIX="${prefix}" \
		-DCMAKE_TOOLCHAIN_FILE="${WORK}/vcpkg/scripts/buildsystems/vcpkg.cmake" \
		-DVCPKG_TARGET_TRIPLET="${triplet}" \
		-DMINIO_CPP_ENABLE_RDMA:BOOL=ON \
		"${cmake_args[@]}"
	cmake --build ./build --config Release -j "$(nproc)"

	# An older install left in place would keep its stale headers first on the
	# include path, so move the whole thing aside instead of installing over it.
	# An install this run would reproduce byte for byte is not older, and backing
	# it up on every rerun would litter the prefix.
	local stale=() f lib
	if cmp -s "${src}/build/libminiocpp.a" "${prefix}/lib/libminiocpp.a"; then
		echo ">>> ${prefix} already holds this ${arch} build"
	else
		if [ -e "${prefix}/include/miniocpp" ]; then
			stale+=("${prefix}/include/miniocpp")
		fi
		for f in "${prefix}"/lib/libminiocpp.*; do
			if [ -e "${f}" ]; then
				stale+=("${f}")
			fi
		done
	fi

	# Shared objects that shadow the archives we install. ld picks these over the
	# .a in the same -L, which is how a release ends up depending on a library it
	# neither bundles nor declares.
	for lib in $(static_libs); do
		for f in "${prefix}"/lib/"lib${lib}".so*; do
			if [ -e "${f}" ]; then
				stale+=("${f}")
			fi
		done
	done

	if [ "${#stale[@]}" -gt 0 ]; then
		local backup="${prefix}/lib/warp-rdma-backup-$(date -u +%Y%m%dT%H%M%SZ)"
		echo ">>> moving ${#stale[@]} stale ${arch} file(s) aside to ${backup}"
		as_root mkdir -p "${backup}"
		for f in "${stale[@]}"; do
			as_root mv "${f}" "${backup}/"
		done
	fi

	echo ">>> installing ${arch} into ${prefix}"
	as_root cmake --install ./build
	as_root mkdir -p "${prefix}/lib"
	# libs3rdma ships prebuilt in the checkout and has no static form;
	# libminiocpp is static, so its transitive vcpkg archives have to sit
	# beside it for the cgo link to resolve from one -L. The triplet is named
	# explicitly so a stray second triplet directory cannot be picked up.
	as_root cp -P vendor/s3rdma/lib/"${uarch}"/* "${prefix}/lib/"
	as_root cp -P vcpkg_installed/"${triplet}"/lib/*.a "${prefix}/lib/"
	command -v ldconfig >/dev/null 2>&1 && as_root ldconfig || true

	verify_prefix "${arch}"
}

# Cross-built objects cannot be executed on the host, so the smoke build proves
# the link and the ELF target instead of running --version.
smoke_build() {
	local arch="$1"
	local prefix out
	prefix="$(arch_prefix "${arch}")"
	out="${WORK}/warp-rdma-smoke-${arch}"

	echo ">>> smoke building warp with -tags=rdma for ${arch}"
	cd "${REPO_DIR}"
	CGO_ENABLED=1 \
		GOOS=linux GOARCH="${arch}" \
		CC="$(arch_cc "${arch}")" \
		CXX="$(arch_cxx "${arch}")" \
		CGO_CFLAGS="-I${prefix}/include" \
		CGO_LDFLAGS="-L${prefix}/lib -Wl,-rpath-link,${prefix}/lib -Wl,-rpath,/usr/lib/warp -Wl,--enable-new-dtags $(cat "${REPO_DIR}/scripts/rdma-cgo-libs.txt")" \
		go build -trimpath -tags=kqueue,rdma -o "${out}" .

	local machine
	machine="$(elf_machine "${out}")"
	if [ "${machine}" != "$(arch_machine "${arch}")" ]; then
		echo "smoke build produced '${machine}', expected '$(arch_machine "${arch}")'" >&2
		return 1
	fi
	readelf -d "${out}" | grep -q '/usr/lib/warp' || {
		echo "smoke build is missing the /usr/lib/warp runpath" >&2
		return 1
	}

	# The published binary may only need the libs3rdma it ships and the base
	# C/C++ runtime. Anything else here is a dependency the package does not
	# declare, so the binary would fail to start on a customer host.
	local needed lib
	needed="$(readelf -d "${out}" | sed -n 's/.*Shared library: \[\(.*\)\]/\1/p')"
	for lib in $(static_libs); do
		if printf '%s\n' "${needed}" | grep -q "^lib${lib}\.so"; then
			echo "smoke build links lib${lib} dynamically; something in $(arch_prefix "${arch}")/lib shadows lib${lib}.a" >&2
			return 1
		fi
	done
	echo ">>> ${arch} smoke build links and targets ${machine}"

	[ "${arch}" = "${HOST_ARCH}" ] || return 0
	"${out}" --version
}

for arch in "${TARGETS[@]}"; do
	build_target "${arch}"
done

SMOKE_BUILT=0
if command -v go >/dev/null 2>&1 && [ -f "${REPO_DIR}/go.mod" ]; then
	for arch in "${TARGETS[@]}"; do
		smoke_build "${arch}"
	done
	SMOKE_BUILT=1
else
	echo ">>> WARNING: no Go toolchain or no go.mod under ${REPO_DIR}; skipped the smoke build" >&2
	echo ">>> rerun from a warp checkout to link-test before releasing; --verify-only" >&2
	echo ">>> rechecks the prefixes and links nothing" >&2
fi

if [ "${SMOKE_BUILT}" = 1 ]; then
	echo ">>> release host provisioned and link-tested for: ${TARGETS[*]}"
else
	echo ">>> release host provisioned for: ${TARGETS[*]}; link compatibility UNVERIFIED"
fi
