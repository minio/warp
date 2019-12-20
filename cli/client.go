package cli

import (
	"errors"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
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

type hostSelectType string

const (
	hostSelectTypeRoundrobin hostSelectType = "roundrobin"
	hostSelectTypeWeighed    hostSelectType = "weighed"
)

func newClient(ctx *cli.Context) func() (cl *minio.Client, done func()) {
	hosts := parseHosts(ctx.String("host"))
	switch len(hosts) {
	case 0:
		fatalIf(probe.NewError(errors.New("no host defined")), "Unable to create MinIO client")
	case 1:
		cl, err := minio.New(hosts[0], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
		fatalIf(probe.NewError(err), "Unable to create MinIO client")
		cl.SetAppInfo(appName, pkg.Version)
		return func() (*minio.Client, func()) {
			return cl, func() {}
		}
	}
	hostSelect := hostSelectType(ctx.String("host-select"))
	switch hostSelect {
	case hostSelectTypeRoundrobin:
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
		return func() (*minio.Client, func()) {
			mu.Lock()
			now := current % len(clients)
			current++
			mu.Unlock()
			return clients[now], func() {}
		}
	case hostSelectTypeWeighed:
		// Keep track of handed out clients.
		// Select random between the clients that have the fewest handed out.
		var mu sync.Mutex
		clients := make([]*minio.Client, len(hosts))
		for i := range hosts {
			cl, err := minio.New(hosts[i], ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
			fatalIf(probe.NewError(err), "Unable to create MinIO client")
			cl.SetAppInfo(appName, pkg.Version)
			clients[i] = cl
		}
		running := make([]int, len(hosts))
		lastFinished := make([]time.Time, len(hosts))
		{
			// Start with a random host
			now := time.Now()
			off := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(hosts))
			for i := range lastFinished {
				t := now
				t.Add(time.Duration(i + off%len(hosts)))
				lastFinished[i] = t
			}
		}
		find := func() int {
			min := math.MaxInt32
			for _, n := range running {
				if n < min {
					min = n
				}
			}
			earliest := time.Now().Add(time.Second)
			earliestIdx := 0
			for i, n := range running {
				if n == min {
					if lastFinished[i].Before(earliest) {
						earliest = lastFinished[i]
						earliestIdx = i
					}
				}
			}
			return earliestIdx
		}
		return func() (*minio.Client, func()) {
			mu.Lock()
			idx := find()
			running[idx]++
			mu.Unlock()
			return clients[idx], func() {
				mu.Lock()
				lastFinished[idx] = time.Now()
				running[idx]--
				if running[idx] < 0 {
					// Will happen if done is called twice.
					panic("client running index < 0")
				}
				mu.Unlock()
			}
		}
	}
	console.Fatalln("unknown host-select:", hostSelect)
	return nil
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
