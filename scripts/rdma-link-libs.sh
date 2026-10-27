#!/usr/bin/env bash
#
# Print the cgo link line for the S3-over-RDMA build, derived from minio-cpp's
# own pkg-config metadata.
#
# libminio is linked statically, so cgo -- which links with gcc, not g++ -- has
# to name the C++ runtime and every transitive archive explicitly. That list was
# maintained by hand in scripts/rdma-cgo-libs.txt until minio-cpp learned to
# declare it: 1.0.0 swapped curlpp for cpp-httplib (adding brotli) and stopped
# taking vcpkg's zlib, and the hand-written list went stale without a word.
#
# Only the -l names and -pthread are taken. The -L that pkg-config would emit
# points into the vcpkg tree the prefix was built from, which on a cross build
# is the wrong architecture; every caller already passes -L<prefix>/lib, where
# the archives are collected. Taking names only is what makes one derivation
# correct for both architectures.
#
# Usage:
#   scripts/rdma-link-libs.sh PREFIX [MINIO_CPP_SRC]
#
#   PREFIX          the libminio install prefix (holds lib/pkgconfig/miniocpp.pc)
#   MINIO_CPP_SRC   the minio-cpp source tree, whose vcpkg_installed/ holds the
#                   .pc files for the private dependencies. Omit it only when
#                   those are already on PKG_CONFIG_PATH.

set -euo pipefail

if [ $# -lt 1 ]; then
	echo "usage: $0 PREFIX [MINIO_CPP_SRC]" >&2
	exit 1
fi

prefix="$1"
src="${2:-}"

command -v pkg-config >/dev/null 2>&1 || {
	echo "rdma-link-libs: pkg-config not found" >&2
	exit 1
}

pc_path="${prefix}/lib/pkgconfig"
if [ -n "${src}" ]; then
	for dir in "${src}"/vcpkg_installed/*/lib/pkgconfig; do
		[ -d "${dir}" ] && pc_path="${pc_path}:${dir}"
	done
fi
export PKG_CONFIG_PATH="${pc_path}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"

if ! pkg-config --exists miniocpp; then
	echo "rdma-link-libs: no miniocpp.pc on ${PKG_CONFIG_PATH}" >&2
	exit 1
fi

libs="$(pkg-config --static --libs-only-l miniocpp)"
other="$(pkg-config --static --libs-only-other miniocpp)"

# A .pc that resolves but names no library means the prefix is not the one this
# build needs. Fail here rather than emitting a link line that drops libminio.
case " ${libs} " in
*" -lminio "*) ;;
*)
	echo "rdma-link-libs: miniocpp.pc does not name -lminio; wrong or stale prefix?" >&2
	exit 1
	;;
esac

# -pthread arrives through Libs.private and is not a -l flag, so --libs-only-l
# drops it. Take it from the other flags rather than the whole set, which can
# also carry -Wl options that have no place in a name-only list.
threads=""
case " ${other} " in
*" -pthread "*) threads=" -pthread" ;;
esac

# pkg-config cannot know about the C++ runtime: it describes a C interface, and
# nothing in the .pc records that the archive behind it is C++.
printf '%s%s -lstdc++\n' "${libs}" "${threads}"
