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
All of them are linux/amd64: the RDMA build links libminiocpp through cgo, so it
cannot be cross-compiled.

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

apk needs `--allow-untrusted` because the package is not signed with a key in
the host keyring. Verify the checksum above before using it. The published
binaries also carry `.minisig` and `.asc` signatures if you want to check those
against the MinIO release keys.

The package installs `/usr/local/bin/warp` and the cuObj libraries it needs
under `/usr/lib/warp`. It is a **drop-in replacement**: the command is `warp`,
the same as the standard package, so install one or the other rather than both.

### Archive

```bash
λ tar xzf warp-rdma_linux_amd64.tar.gz
λ ./warp-rdma/warp --version
```

The archive carries its libraries in a `lib` directory beside the binary and
finds them on its own, so it needs no installation and no `LD_LIBRARY_PATH`.
Use it when you cannot install packages, or want several versions side by side.

### Host requirements

The package bundles the cuObj libraries and links everything else statically, so
it needs only these from the host:

- the base C and C++ runtime, `libc` and `libstdc++`, present on any
  distribution
- `libibverbs`, `librdmacm` and `libnuma`, declared as package dependencies
- the vendor provider for your card, such as `libmlx5`, which must match the
  kernel driver and therefore cannot be bundled

CUDA is **not** required to install or to run `--rdma=cpu`. The runtime is
loaded on demand and only `--rdma=gpu` needs it.

## Configure the client

The cuObj client reads its settings from `cuobj.json`. Warp does not install
this file, and without one that names your NIC, transfers fall back to HTTP
instead of using RDMA.

Point cuObj at a file with `CUFILE_ENV_PATH_JSON`:

```bash
λ export CUFILE_ENV_PATH_JSON=/etc/warp/cuobj.json
```

### Find your NIC address

`rdma_dev_addr_list` takes the IPv4 address of the RDMA NIC to run over. Map the
RDMA devices to interfaces, then read the address of the one on the same fabric
as the storage server:

```bash
λ ibdev2netdev
mlx5_0 port 1 ==> enp27s0np0 (Up)
mlx5_1 port 1 ==> enp157s0np0 (Up)

λ ip -4 -o addr show enp157s0np0
enp157s0np0    inet 10.0.1.241/24 ...
```

### Write the file

This is the shape a working client uses:

```json
{
  "logging": { "level": "ERROR" },
  "execution": { "parallel_io": false },
  "properties": {
    "allow_compat_mode": true,
    "use_pci_p2pdma": true,
    "rdma_peer_type": "dmabuf",
    "rdma_dev_addr_list": ["10.0.1.241"],
    "rdma_multipath_enabled": false
  }
}
```

| Setting                  | What it does                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------ |
| `rdma_dev_addr_list`     | The NIC to run RDMA over. Required; without it transfers fall back to HTTP           |
| `allow_compat_mode`      | Falls back to a compatible path when the `nvidia-fs` driver is absent                |
| `use_pci_p2pdma`         | Lets the NIC move data across PCIe straight to the GPU, for `--rdma=gpu`             |
| `rdma_peer_type`         | How GPU memory is registered with the NIC; `dmabuf` on current drivers               |
| `rdma_multipath_enabled` | Failover across several NICs. Off when pinning one, which is what benchmark hosts do |
| `logging.level`          | `NOTICE`, `ERROR`, `WARN`, `INFO`, `DEBUG` or `TRACE`                                |
| `logging.dir`            | Where `cufile.log` is written; the current directory when unset                      |

Two notes on the sample that ships with
[minio-cpp](https://github.com/minio/minio-cpp/blob/main/vendor/cuobj/cuobj.json).
Its `rdma_dev_addr_list` is a `<client-nic-ip>` placeholder, so copying it
without editing leaves RDMA unconfigured. It also logs at `TRACE`, which is
useful while bringing a host up and noisy afterwards; deployed clients run at
`ERROR` or `INFO`.

Multipath is worth enabling only when you list several NICs and want failover.
The sample documents `rdma_max_backup_devices`, `rdma_io_retry_count`,
`rdma_failback_enabled` and `rdma_health_check_interval_ms` for that case.

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

**`error while loading shared libraries: libcuobjclient.so.1`**

The binary cannot find the cuObj libraries. With the package they live in
`/usr/lib/warp`; with the archive, run the binary from where you unpacked it so
the bundled `lib` directory sits beside it.

**Throughput looks like plain HTTP**

RDMA failures fall back to HTTP silently, so warp's output looks normal. The
usual cause is client configuration: no `cuobj.json`, or one whose
`rdma_dev_addr_list` still holds the `<client-nic-ip>` placeholder. See
[Configure the client](#configure-the-client). Raise `logging.level` to `DEBUG`
to see what cuObj attempted, and confirm from the server's counters rather than
from warp.

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

The binary this produces still loads the cuObj libraries at run time, and
nothing tells it where they are. Choose one:

```bash
λ sudo ldconfig                                  # if they are in a system path
λ export LD_LIBRARY_PATH=/usr/local/lib          # per shell
λ go build -tags=kqueue,rdma \
    -ldflags "-extldflags=-Wl,-rpath,/usr/local/lib"   # baked into the binary
```

`build-rdma.sh` avoids the question by bundling the libraries in the archive and
baking an `$ORIGIN/lib` rpath.
