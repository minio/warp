package cli

import (
	"flag"
	"testing"

	"github.com/minio/cli"
)

// Issue 460: json, quiet and no-color in a warp run file were accepted but
// ignored, because the globals were read before the file was parsed.
func TestRunFileSetsGlobalFlags(t *testing.T) {
	saved := [4]bool{globalQuiet, globalDebug, globalJSON, globalNoColor}
	t.Cleanup(func() { globalQuiet, globalDebug, globalJSON, globalNoColor = saved[0], saved[1], saved[2], saved[3] })
	globalQuiet, globalDebug, globalJSON, globalNoColor = false, false, false, false

	yFile := []byte(`
warp:
  api: v1
  benchmark: get
  json: true
  quiet: true
  no-color: true
  remote:
    access-key: key
    secret-key: secret
    host:
      - localhost:9000
    bucket: bucket1
  params:
    duration: 10s
    concurrent: 8
    list-existing: true
`)
	app := cli.NewApp()
	parent := cli.NewContext(app, flag.NewFlagSet("run", flag.ContinueOnError), nil)

	ctx2, cmd := benchContextFromYAML(parent, yFile)

	if cmd.Name != "get" {
		t.Fatalf("benchmark = %q, want get", cmd.Name)
	}
	if !ctx2.Bool("json") || !ctx2.Bool("quiet") || !ctx2.Bool("no-color") {
		t.Fatalf("flags not set on the benchmark context: json=%v quiet=%v no-color=%v",
			ctx2.Bool("json"), ctx2.Bool("quiet"), ctx2.Bool("no-color"))
	}
	if !globalJSON {
		t.Error("json: true in the run file did not set globalJSON")
	}
	if !globalQuiet {
		t.Error("quiet: true in the run file did not set globalQuiet")
	}
	if !globalNoColor {
		t.Error("no-color: true in the run file did not set globalNoColor")
	}
	if ctx2.Duration("duration").String() != "10s" || ctx2.Int("concurrent") != 8 {
		t.Errorf("params not carried over: duration=%v concurrent=%v", ctx2.Duration("duration"), ctx2.Int("concurrent"))
	}
}

func TestRunFileWithoutGlobalFlagsLeavesThemOff(t *testing.T) {
	saved := [4]bool{globalQuiet, globalDebug, globalJSON, globalNoColor}
	t.Cleanup(func() { globalQuiet, globalDebug, globalJSON, globalNoColor = saved[0], saved[1], saved[2], saved[3] })
	globalQuiet, globalDebug, globalJSON, globalNoColor = false, false, false, false

	yFile := []byte(`
warp:
  api: v1
  benchmark: get
  remote:
    host:
      - localhost:9000
    bucket: bucket1
`)
	app := cli.NewApp()
	parent := cli.NewContext(app, flag.NewFlagSet("run", flag.ContinueOnError), nil)
	benchContextFromYAML(parent, yFile)

	if globalJSON || globalQuiet || globalNoColor {
		t.Errorf("globals turned on without the keys: json=%v quiet=%v no-color=%v", globalJSON, globalQuiet, globalNoColor)
	}
}
