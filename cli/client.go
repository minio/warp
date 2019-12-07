package cli

import (
	"errors"
	"log"
	"strings"
	"sync"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v6"
	"github.com/minio/minio/pkg/ellipses"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/warp/pkg"
)

// parseHosts will parse the host parameter given.
func parseHosts(h string) []string {
	hosts := strings.Split(h, ",")
	dst := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if !ellipses.HasEllipses(host) {
			dst = append(dst, host)
			continue
		}
		patterns, perr := ellipses.FindEllipsesPatterns(host)
		if perr != nil {
			fatalIf(probe.NewError(perr), "Unable to parse host parameter")

			log.Fatal(perr.Error())
		}
		for _, p := range patterns {
			dst = append(dst, p.Expand()...)
		}
	}
	return dst
}

func newClient(ctx *cli.Context) func() *minio.Client {
	hosts := parseHosts(ctx.String("host"))
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
	hosts := parseHosts(ctx.String("host"))
	if len(hosts) == 0 {
		fatalIf(probe.NewError(errors.New("no host defined")), "Unable to create MinIO admin client")
	}
	cl, err := madmin.New(hosts[0], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
	fatalIf(probe.NewError(err), "Unable to create MinIO admin client")
	cl.SetAppInfo(appName, pkg.Version)
	return cl
}
