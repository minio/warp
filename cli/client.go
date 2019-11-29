package cli

import (
	"errors"
	"strings"
	"sync"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v6"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/warp/pkg"
)

func newClient(ctx *cli.Context) func() *minio.Client {
	hosts := strings.Split(ctx.String("host"), ",")
	switch len(hosts) {
	case 0:
		fatalIf(probe.NewError(errors.New("no host defined")), "Unable to create MinIO client")
	case 1:
		cl, err := minio.New(hosts[0], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
		fatalIf(probe.NewError(err), "Unable to create MinIO client")
		cl.SetAppInfo(appName, pkg.Version)
		return func() *minio.Client {
			return cl
		}
	}
	// Do round-robin.
	var current int
	var mu sync.Mutex
	clients := make([]*minio.Client, len(hosts))
	for i := range hosts {
		cl, err := minio.New(hosts[i], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
		fatalIf(probe.NewError(err), "Unable to create MinIO client")
		cl.SetAppInfo(appName, pkg.Version)
		clients[i] = cl
	}
	return func() *minio.Client {
		mu.Lock()
		now := current % len(clients)
		current++
		mu.Unlock()
		return clients[now]

	}
}

func newAdminClient(ctx *cli.Context) *madmin.AdminClient {
	hosts := strings.Split(ctx.String("host"), ",")
	if len(hosts) == 0 {
		fatalIf(probe.NewError(errors.New("no host defined")), "Unable to create MinIO admin client")
	}
	cl, err := madmin.New(hosts[0], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
	fatalIf(probe.NewError(err), "Unable to create MinIO admin client")
	cl.SetAppInfo(appName, pkg.Version)
	return cl
}
