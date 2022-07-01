FROM golang:1.18

ADD go.mod /go/src/github.com/minio/warp/go.mod
ADD go.sum /go/src/github.com/minio/warp/go.sum
WORKDIR /go/src/github.com/minio/warp/
# Get dependencies - will also be cached if we won't change mod/sum
RUN go mod download

ADD . /go/src/github.com/minio/warp/
WORKDIR /go/src/github.com/minio/warp/

ENV CGO_ENABLED=0

RUN go build -ldflags '-w -s' -a -o warp .

FROM alpine
MAINTAINER MinIO Development "dev@min.io"
EXPOSE 7761

COPY --from=0 /go/src/github.com/minio/warp/warp /warp

ENTRYPOINT ["/warp"]
