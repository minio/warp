package cli

import (
	"crypto/rand"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v6/pkg/encrypt"
)

var sseKey encrypt.ServerSide

// newSSE returns a randomly generated key if SSE is requested.
// Only one key will be generated.
func newSSE(ctx *cli.Context) encrypt.ServerSide {
	if !ctx.Bool("encrypt") {
		return nil
	}
	if sseKey != nil {
		return sseKey
	}
	var key [32]byte
	_, err := rand.Read(key[:])
	if err != nil {
		panic(err)
	}
	sseKey, err = encrypt.NewSSEC(key[:])
	if err != nil {
		panic(err)
	}
	return sseKey
}
