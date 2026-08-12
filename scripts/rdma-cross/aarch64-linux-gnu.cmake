# CMake toolchain for cross-building libminiocpp (and its vcpkg dependencies)
# to linux/arm64 from an amd64 release host.
#
# Used by scripts/setup-rdma-release-host.sh, both directly for the minio-cpp
# project and chainloaded by scripts/rdma-cross/triplets/arm64-linux.cmake so
# vcpkg builds the dependency tree with the same compiler.

set(CMAKE_SYSTEM_NAME Linux)

# minio-cpp reads CMAKE_SYSTEM_PROCESSOR to pick vendor/cuobj/lib/<arch>, so
# this is what makes an RDMA build resolve the aarch64 cuObj libraries.
set(CMAKE_SYSTEM_PROCESSOR aarch64)

set(CMAKE_C_COMPILER aarch64-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER aarch64-linux-gnu-g++)

# Debian/Ubuntu foreign-architecture packages -- the arm64 libibverbs, librdmacm
# and libnuma an RDMA build links -- install under /usr/lib/<triplet>, which
# cmake only searches when told the library architecture.
set(CMAKE_LIBRARY_ARCHITECTURE aarch64-linux-gnu)

# Host tools stay executable; nothing else should come from the host.
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
