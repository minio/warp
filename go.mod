module github.com/minio/m3

go 1.13

require (
	github.com/cheggaaa/pb v1.0.28
	github.com/dustin/go-humanize v1.0.0
	github.com/fatih/color v1.7.0
	github.com/inconshreveable/go-update v0.0.0-20160112193335-8152e7eb6ccf
	github.com/mattn/go-isatty v0.0.7
	github.com/minio/cli v1.21.0
	github.com/minio/mc v0.0.0-20190919160124-93297260beaf
	github.com/minio/minio v0.0.0-20190903181048-8a71b0ec5a72
	github.com/minio/sha256-simd v0.1.0
	github.com/pkg/profile v1.3.0
	github.com/posener/complete v1.2.2-0.20190702141536-6ffe496ea953
	github.com/segmentio/go-prompt v1.2.1-0.20161017233205-f0d19b6901ad
)

replace github.com/gorilla/rpc v1.2.0+incompatible => github.com/gorilla/rpc v1.2.0
