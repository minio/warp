#!/usr/bin/env bash
#
# qreleaser post-transform hook: package the S3-over-RDMA binary.
#
# Invoked by the q "transform" step as:
#   scripts/release-post-transform.sh <release-type> <version-tag>
# with the current directory set to the repository root.
#
# By the time this runs, goreleaser has built the RDMA binary (the `warp-rdma`
# build id) and transform-to-q-layout.sh has placed it -- signed, checksummed,
# suffix-named -- in the q layout the same way it handles FIPS:
#   warp-release/<release-type>/linux-<arch>/warp.<version-tag>.rdma
# So this hook only generates the deb/rpm/apk packages via pkger and drops them
# alongside; it neither builds nor signs the binary.
#
# The packages are drop-in: the payload is /usr/local/bin/warp, same as the
# stock package, so a customer installs the RDMA build instead of the stock one
# rather than beside it. The files are named warp-rdma_* to make that visible
# on the download page. Producing a co-installable warp.rdma package would need
# pkger to apply warp's semver rules to a flavored app name, which it does not.

set -euo pipefail

RELEASE_TYPE="${1:?missing release type (edge|release|hotfixes)}"
VERSION_TAG="${2:?missing version tag (e.g. v1.5.0)}"

case "${RELEASE_TYPE}" in
edge | release) ;;
*)
	echo "post-transform: RDMA packages are not shipped for '${RELEASE_TYPE}', skipping"
	exit 0
	;;
esac

if [ "$(uname -s)" != Linux ]; then
	echo "post-transform: RDMA packaging requires Linux (got $(uname -s)), skipping"
	exit 0
fi

if ! command -v pkger >/dev/null 2>&1; then
	echo "post-transform: pkger not found; qreleaser installs it with setup-deps" >&2
	exit 1
fi

MINIOCPP_PREFIX="${MINIOCPP_PREFIX:-/usr/local}"
STAGE_DIR=.rdma-pkg
trap 'rm -rf "${STAGE_DIR}"' EXIT

for arch in amd64 arm64; do
	layout_dir="warp-release/${RELEASE_TYPE}/linux-${arch}"
	binary="${layout_dir}/warp.${VERSION_TAG}.rdma"

	# A release host provisioned for only one architecture legitimately produces
	# only that binary, so a missing one is a skip rather than an error.
	if [ ! -f "${binary}" ]; then
		echo "post-transform: no RDMA binary for linux-${arch}, skipping"
		continue
	fi

	# arm64 is cross-built against its own prefix; see
	# scripts/setup-rdma-release-host.sh and .goreleaser/qreleaser.yaml.
	case "${arch}" in
	arm64) arch_prefix="${MINIOCPP_PREFIX}/aarch64-linux-gnu" ;;
	*) arch_prefix="${MINIOCPP_PREFIX}" ;;
	esac

	# pkger reads <releaseDir>/<goos>-<goarch>/<binary-name>.<version>, while the
	# q layout names the flavor <binary>.<version>.rdma. Stage a copy under the
	# name pkger expects, in a scratch dir so the published layout keeps only the
	# suffix-named binary.
	pkg_dir="${STAGE_DIR}/release/linux-${arch}"
	mkdir -p "${pkg_dir}" "${STAGE_DIR}/lib/${arch}"
	cp -p "${binary}" "${pkg_dir}/warp.rdma.${VERSION_TAG}"
	cp -P "${arch_prefix}"/lib/libs3rdma.so* "${STAGE_DIR}/lib/${arch}/"

	echo "post-transform: packaging RDMA artifacts for linux-${arch} (${VERSION_TAG})"
	pkger -a warp --binary-name warp.rdma -r "${VERSION_TAG}" -l AGPLv3 \
		-d "${STAGE_DIR}/release" \
		--contents ./pkg-scripts/rdma-contents.yaml \
		--deps ./pkg-scripts/rdma-deps.yaml \
		--ignore

	# pkger names the packages after the app, so rename on the way out to keep
	# them distinct from the stock warp packages already in the layout. Its own
	# checksums are written against the pre-rename names, so regenerate them
	# here rather than carrying across files that no longer match.
	for pkg in "${pkg_dir}"/warp_*.deb "${pkg_dir}"/warp-*.rpm "${pkg_dir}"/warp_*.apk; do
		[ -e "${pkg}" ] || continue
		base="$(basename "${pkg}")"
		renamed="${base/warp/warp-rdma}"
		mv "${pkg}" "${layout_dir}/${renamed}"
		(cd "${layout_dir}" && sha256sum "${renamed}" >"${renamed}.sha256sum")
	done

	echo "post-transform: RDMA packages placed in ${layout_dir}:"
	ls -la "${layout_dir}"/warp-rdma_* "${layout_dir}"/warp-rdma-* 2>/dev/null || true
done
