FROM golang:1.13.7

ADD go.mod /go/src/github.com/minio/warp/go.mod
ADD go.sum /go/src/github.com/minio/warp/go.sum
WORKDIR /go/src/github.com/minio/warp/
# Get dependencies - will also be cached if we won't change mod/sum
RUN go mod download

ADD . /go/src/github.com/minio/warp/
WORKDIR /go/src/github.com/minio/warp/

ENV CGO_ENABLED=0
ARG warp_tag
ARG warp_full_commit
ARG warp_short_commit
ARG warp_version
ARG warp_date

RUN go build -ldflags '-w -s -X github.com/minio/warp/pkg.ReleaseTag=$warp_tag -X github.com/minio/warp/pkg.CommitID=$warp_full_commit -X github.com/minio/warp/pkg.Version=$warp_version -X github.com/minio/warp/pkg.ShortCommitID=$warp_short_commit -X github.com/minio/warp/pkg.ReleaseTime=$warp_date' -a -o warp .

FROM scratch
MAINTAINER MinIO Development "dev@min.io"

COPY --from=0 /go/src/github.com/minio/warp/warp .

CMD ["/warp"]
