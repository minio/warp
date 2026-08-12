# vcpkg overlay triplet for cross-building the libminiocpp dependency tree to
# linux/arm64 from an amd64 host.
#
# vcpkg's own arm64-linux triplet assumes the default compiler can produce arm64
# objects, which on an amd64 host it cannot. This overlay differs from it only by
# chainloading the aarch64 toolchain.

set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)

# libminiocpp is linked statically into warp, so its dependencies must be static
# too; see scripts/rdma-cgo-libs.txt for the resulting link line.
set(VCPKG_LIBRARY_LINKAGE static)

set(VCPKG_CMAKE_SYSTEM_NAME Linux)
set(VCPKG_CHAINLOAD_TOOLCHAIN_FILE "${CMAKE_CURRENT_LIST_DIR}/../aarch64-linux-gnu.cmake")
