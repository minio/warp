TAG    := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")
FULL   := $(shell git rev-parse HEAD 2>/dev/null || echo "dev")
DATE   := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
VER    := $(patsubst v%,%,$(TAG))

LDFLAGS := -s -w \
	-X github.com/minio/warp/pkg.ReleaseTag=$(TAG) \
	-X github.com/minio/warp/pkg.Version=$(VER) \
	-X github.com/minio/warp/pkg.CommitID=$(FULL) \
	-X github.com/minio/warp/pkg.ShortCommitID=$(COMMIT) \
	-X github.com/minio/warp/pkg.ReleaseTime=$(DATE)

.PHONY: build clean test

build:
	go build -o warp-replay -ldflags "$(LDFLAGS)" .

clean:
	rm -f warp-replay

test:
	go test ./...
