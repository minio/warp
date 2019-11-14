package cli

import (
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v6"
	"github.com/minio/minio/pkg/madmin"
	"github.com/minio/warp/pkg"
)

func newClient(ctx *cli.Context) *minio.Client {
	cl, err := minio.New(ctx.String("host"), ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
	fatalIf(probe.NewError(err), "Unable to create MinIO client")
	cl.SetAppInfo(appName, pkg.Version)
	return cl
}

func newAdminClient(ctx *cli.Context) *madmin.AdminClient {
	cl, err := madmin.New(ctx.String("host"), ctx.String("access-key"), ctx.String("secret-key"), ctx.Bool("tls"))
	fatalIf(probe.NewError(err), "Unable to create MinIO admin client")
	cl.SetAppInfo(appName, pkg.Version)
	return cl
}
