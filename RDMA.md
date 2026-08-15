# S3 over RDMA

The `--rdma` flag sends PUT and GET payloads over RDMA instead of HTTP. Warp
gives each worker a registered buffer and lets the network card move object data
straight into or out of it.

| Value        | Buffer                   | Use it for                                        |
| ------------ | ------------------------ | ------------------------------------------------- |
| `--rdma=cpu` | Page-aligned host memory | RDMA between the server and host memory           |
| `--rdma=gpu` | CUDA device memory       | GPU-Direct: the NIC transfers straight to the GPU |

Leaving `--rdma` unset keeps the normal HTTP path. Only the `get` and `put`
benchmarks accept `--rdma`; the others reject it rather than report HTTP numbers
as if they were RDMA numbers.

> [!IMPORTANT]
> When an RDMA transfer cannot be set up, the underlying library falls back to
> HTTP and the operation still succeeds. Warp cannot see that this happened, so
> its output never tells you whether RDMA was used. Confirm that from the
> storage server's S3 over RDMA counters before you trust a comparison.

## Install

RDMA support is compiled in, and the standard warp binaries are built without
cgo, so they refuse `--rdma` at startup. Use one of the builds below instead.
They are published for linux/amd64 and linux/arm64.

### Package

Every release artifact ships a `.sha256sum` beside it. Check it first:

```bash
λ sha256sum -c warp-rdma_<version>_amd64.deb.sha256sum
warp-rdma_<version>_amd64.deb: OK
```

Then install:

```bash
λ sudo dnf install warp-rdma-<version>-1.x86_64.rpm     # RHEL, Fedora, Rocky
λ sudo apt install ./warp-rdma_<version>_amd64.deb      # Debian, Ubuntu
λ sudo apk add --allow-untrusted warp-rdma_<version>_x86_64.apk
```

On arm64 hosts the same packages are named `warp-rdma-<version>-1.aarch64.rpm`,
`warp-rdma_<version>_arm64.deb` and `warp-rdma_<version>_aarch64.apk`.

apk needs `--allow-untrusted` because the package is not signed with a key in
the host keyring. Verify the checksum above before using it. The published
binaries also carry `.minisig` and `.asc` signatures if you want to check those
against the MinIO release keys.

The package installs `/usr/local/bin/warp` and the libs3rdma it needs
under `/usr/lib/warp`. It is a **drop-in replacement**: the command is `warp`,
the same as the standard package, so install one or the other rather than both.

### Archive

```bash
λ tar xzf warp-rdma_linux_amd64.tar.gz   # or warp-rdma_linux_arm64.tar.gz
λ ./warp-rdma/warp --version
```

The archive carries its libraries in a `lib` directory beside the binary and
finds them on its own, so it needs no installation and no `LD_LIBRARY_PATH`.
Use it when you cannot install packages, or want several versions side by side.

### Host requirements

The package bundles libs3rdma and links everything else statically, so
it needs only these from the host:

- the base C and C++ runtime, `libc` and `libstdc++`, present on any
  distribution
- `libibverbs`, `librdmacm` and `libnuma`, declared as package dependencies
- the vendor provider for your card, such as `libmlx5`, which must match the
  kernel driver and therefore cannot be bundled

CUDA is **not** required to install or to run `--rdma=cpu`. The runtime is
loaded on demand and only `--rdma=gpu` needs it.

## Configure the client

Nothing to configure. libs3rdma opens every RDMA device whose port is ACTIVE and
spreads transfers across them, so a host with two cards uses both without being
told about either. There is no configuration file to install and no address to
write down.

Confirm the host has usable devices:

```bash
λ ibdev2netdev
mlx5_0 port 1 ==> enp27s0np0 (Up)
mlx5_1 port 1 ==> enp157s0np0 (Up)
```

Both ports `Up` is the whole requirement.

### Restricting to one NIC

Set `S3RDMA_DEVICE` to a device name to use only that one. This is worth doing
when a host has cards on different fabrics and only one of them reaches the
storage server:

```bash
λ export S3RDMA_DEVICE=mlx5_1
```

Unset, every device with an ACTIVE port is used.

### Several NICs

Transfers are spread round-robin over the devices found, and each individual
transfer rides exactly one of them. Aggregate throughput therefore comes from
running concurrent operations, not from striping one object across cards: a run
at `--concurrent=1` exercises a single NIC however many are installed, which is
the usual reason a dual-NIC host appears to be using one.

A card that fails a transfer is taken out of rotation until it recovers, so a
NIC dying mid-run costs throughput rather than ending the run.

## Run

The storage server must have S3 over RDMA enabled and reachable from the client.

```bash
λ warp get --rdma=cpu --host=s3-server:9000 --access-key=minio --secret-key=minio123
λ warp put --rdma=gpu --host=s3-server:9000 --access-key=minio --secret-key=minio123
```

`--rdma=gpu` additionally needs an NVIDIA GPU on the client, with a driver new
enough for the installed CUDA runtime.

Warp prints a warning at startup when it can tell that S3 over RDMA is not
reachable. The probe is optimistic, so the absence of a warning is not proof
that RDMA is working.

`--rdma` works in [distributed mode](README.md#distributed-benchmarking) as
well. The server passes the flag to every client, so each client needs an
RDMA-capable warp binary and its own RDMA path to the storage server.

## Troubleshooting

**`--rdma=cpu requires the RDMA build of warp`**

You are running a standard binary. Install one of the builds above.

**`--rdma=gpu is unavailable: the oldest CUDA runtime on this host is X, but the
NVIDIA driver only supports up to Y`**

The CUDA runtime is newer than the driver can run. Upgrade the driver, or
install a runtime the driver supports. A host can carry several runtimes; warp
reports the closest one so the message names the smallest upgrade that helps.

**`--rdma=gpu is unavailable: no usable CUDA runtime (libcudart) was found`**

No CUDA runtime is installed. `--rdma=cpu` still works.

**`error while loading shared libraries: libs3rdma.so.0`**

The binary cannot find libs3rdma. With the package it lives in
`/usr/lib/warp`; with the archive, run the binary from where you unpacked it so
the bundled `lib` directory sits beside it.

**Throughput looks like plain HTTP**

RDMA failures fall back to HTTP silently, so warp's output looks normal.

Check the fabric first: `ibdev2netdev` should report a port `Up`, and the server
must have S3 over RDMA enabled and be reachable over that fabric. If
`S3RDMA_DEVICE` is set, confirm it names a device that reaches the server rather
than one on another fabric.

A run at `--concurrent=1` uses one NIC by design, so a dual-NIC host reaching
roughly half its expected throughput is usually concurrency rather than a fault.
Confirm from the server's counters rather than from warp.

## Build from source

You need Go (the version in `go.mod`), git, a C++17 compiler, CMake 3.31 or
newer, and the RDMA headers. On Debian or Ubuntu:

```bash
λ sudo apt-get install g++ git libibverbs-dev librdmacm-dev libnuma-dev
```

Distribution packages are often older than 3.31. `build-rdma.sh` checks the
version and stops when it is too old, because vcpkg's port scripts need the
newer one. Install a current CMake from
[Kitware's apt repository](https://apt.kitware.com) or with `pip install cmake`.

Then:

```bash
λ ./scripts/build-rdma.sh    # produces warp-rdma_linux_<arch>.tar.gz
```

The script builds [minio-cpp](https://github.com/minio/minio-cpp) with RDMA
enabled, links warp against it statically, and packages the archive. It is also
how the published archives are built, and it needs no CUDA package even though
the result supports `--rdma=gpu`.

To build the binary alone against an existing libminiocpp, pass its prefix and
the same library list the release uses:

```bash
λ export CGO_ENABLED=1
λ export CGO_CFLAGS="-I/usr/local/include"
λ export CGO_LDFLAGS="-L/usr/local/lib $(cat scripts/rdma-cgo-libs.txt)"
λ go build -tags=kqueue,rdma
```

libminiocpp is linked statically, so cgo, which links with `gcc` rather than
`g++`, has to be told about the C++ runtime and every transitive archive by
name. That list lives in `scripts/rdma-cgo-libs.txt`.

The binary this produces still loads libs3rdma at run time, and nothing tells
it where to find it. Choose one:

```bash
λ sudo ldconfig                                  # if they are in a system path
λ export LD_LIBRARY_PATH=/usr/local/lib          # per shell
λ go build -tags=kqueue,rdma \
    -ldflags "-extldflags=-Wl,-rpath,/usr/local/lib"   # baked into the binary
```

`build-rdma.sh` avoids the question by bundling the libraries in the archive and
baking an `$ORIGIN/lib` rpath.
