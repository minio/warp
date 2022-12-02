/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cli

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"time"

	mprofile "github.com/bygui86/multi-profile/v2"
	"github.com/cheggaaa/pb"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/console"
	"github.com/minio/pkg/trie"
	"github.com/minio/pkg/words"
	"github.com/minio/warp/pkg"
	completeinstall "github.com/posener/complete/cmd/install"
)

var (
	globalQuiet   = false // Quiet flag set via command line
	globalJSON    = false // Json flag set via command line
	globalDebug   = false // Debug flag set via command line
	globalNoColor = false // No Color flag set via command line
	// Terminal width
	globalTermWidth int
)

const (
	appName   = "warp"
	appNameUC = "WARP"
)

// Main starts warp
func Main(args []string) {
	// Set system max resources as needed.
	setMaxResources()

	if len(args) > 1 {
		switch args[1] {
		case appName, filepath.Base(args[0]):
			mainComplete()
			return
		}
	}
	rand.Seed(time.Now().UnixNano())

	probe.Init() // Set project's root source path.
	probe.SetAppInfo("Release-Tag", pkg.ReleaseTag)
	probe.SetAppInfo("Commit", pkg.ShortCommitID)

	// Fetch terminal size, if not available, automatically
	// set globalQuiet to true.
	if w, e := pb.GetTerminalWidth(); e != nil {
		globalQuiet = true
	} else {
		globalTermWidth = w
	}

	// Set the warp app name.
	appName := filepath.Base(args[0])

	// Run the app - exit on error.
	if err := registerApp(appName, appCmds).Run(args); err != nil {
		os.Exit(1)
	}
}

func init() {
	a := []cli.Command{
		mixedCmd,
		getCmd,
		putCmd,
		deleteCmd,
		listCmd,
		statCmd,
		selectCmd,
		versionedCmd,
		retentionCmd,
		multipartCmd,
		zipCmd,
	}
	b := []cli.Command{
		analyzeCmd,
		cmpCmd,
		mergeCmd,
		clientCmd,
	}
	appCmds = append(a, b...)
	benchCmds = a
}

var appCmds, benchCmds []cli.Command

func combineFlags(flags ...[]cli.Flag) []cli.Flag {
	var dst []cli.Flag
	for _, fl := range flags {
		dst = append(dst, fl...)
	}
	return dst
}

// Collection of warp commands currently supported
var commands = []cli.Command{}

// Collection of warp commands currently supported in a trie tree
var commandsTree = trie.NewTrie()

// registerCmd registers a cli command
func registerCmd(cmd cli.Command) {
	commands = append(commands, cmd)
	commandsTree.Insert(cmd.Name)
}

func registerApp(name string, appCmds []cli.Command) *cli.App {
	for _, cmd := range appCmds {
		registerCmd(cmd)
	}

	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, h",
		Usage: "show help",
	}

	app := cli.NewApp()
	app.Name = name
	app.Action = func(ctx *cli.Context) {
		if ctx.Bool("autocompletion") || ctx.GlobalBool("autocompletion") {
			// Install shell completions
			installAutoCompletion()
			return
		}

		cli.ShowAppHelp(ctx)
	}
	var afterExec func(ctx *cli.Context) error
	app.After = func(ctx *cli.Context) error {
		if afterExec != nil {
			return afterExec(ctx)
		}
		return nil
	}

	app.Before = func(ctx *cli.Context) error {
		var profiles []*mprofile.Profile
		cfg := &mprofile.Config{
			Path:           ctx.String("pprofdir"),
			UseTempPath:    false,
			Quiet:          false,
			MemProfileRate: 4096,
			MemProfileType: "heap",
			CloserHook:     nil,
			Logger:         nil,
		}
		if ctx.Bool("cpu") {
			profiles = append(profiles, mprofile.CPUProfile(cfg).Start())
		}
		if ctx.Bool("mem") {
			profiles = append(profiles, mprofile.MemProfile(cfg).Start())
		}
		if ctx.Bool("block") {
			profiles = append(profiles, mprofile.BlockProfile(cfg).Start())
		}
		if ctx.Bool("mutex") {
			profiles = append(profiles, mprofile.MutexProfile(cfg).Start())
		}
		if ctx.Bool("trace") {
			profiles = append(profiles, mprofile.TraceProfile(cfg).Start())
		}
		if ctx.Bool("threads") {
			profiles = append(profiles, mprofile.ThreadCreationProfile(cfg).Start())
		}
		if len(profiles) == 0 {
			return nil
		}

		afterExec = func(ctx *cli.Context) error {
			for _, profile := range profiles {
				profile.Stop()
			}
			return nil
		}
		return nil
	}

	app.ExtraInfo = func() map[string]string {
		if globalDebug {
			return getSystemData()
		}
		return make(map[string]string)
	}

	app.HideHelpCommand = true
	app.Usage = "Benchmark tool for S3 compatible object storage systems.\n\tFor usage details see https://github.com/minio/warp"
	app.Commands = commands
	app.Author = "MinIO, Inc."
	app.Version = pkg.Version + " - " + pkg.ShortCommitID
	app.Copyright = "(c) 2020 MinIO, Inc."
	app.Compiled, _ = time.Parse(time.RFC3339, pkg.ReleaseTime)
	app.Flags = append(app.Flags, profileFlags...)
	app.Flags = append(app.Flags, globalFlags...)
	app.CommandNotFound = commandNotFound // handler function declared above.
	app.EnableBashCompletion = true

	return app
}

func installAutoCompletion() {
	if runtime.GOOS == "windows" {
		console.Infoln("autocompletion feature is not available for this operating system")
		return
	}

	if completeinstall.IsInstalled(filepath.Base(os.Args[0])) || completeinstall.IsInstalled(appName) {
		console.Infoln("autocompletion is already enabled in your '$SHELLRC'")
		return
	}

	err := completeinstall.Install(filepath.Base(os.Args[0]))
	if err != nil {
		fatalIf(probe.NewError(err), "Unable to install auto-completion.")
	} else {
		console.Infoln("enabled autocompletion in '$SHELLRC'. Please restart your shell.")
	}
}

// Get os/arch/platform specific information.
// Returns a map of current os/arch/platform/memstats.
func getSystemData() map[string]string {
	host, e := os.Hostname()
	fatalIf(probe.NewError(e), "Unable to determine the hostname.")

	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	mem := fmt.Sprintf("Used: %s | Allocated: %s | UsedHeap: %s | AllocatedHeap: %s",
		pb.Format(int64(memstats.Alloc)).To(pb.U_BYTES),
		pb.Format(int64(memstats.TotalAlloc)).To(pb.U_BYTES),
		pb.Format(int64(memstats.HeapAlloc)).To(pb.U_BYTES),
		pb.Format(int64(memstats.HeapSys)).To(pb.U_BYTES))
	platform := fmt.Sprintf("Host: %s | OS: %s | Arch: %s", host, runtime.GOOS, runtime.GOARCH)
	goruntime := fmt.Sprintf("Version: %s | CPUs: %s", runtime.Version(), strconv.Itoa(runtime.NumCPU()))
	return map[string]string{
		"PLATFORM": platform,
		"RUNTIME":  goruntime,
		"MEM":      mem,
	}
}

// Function invoked when invalid command is passed.
func commandNotFound(ctx *cli.Context, command string) {
	msg := fmt.Sprintf("`%s` is not a %s command. See `m3 --help`.", command, appName)
	closestCommands := findClosestCommands(command)
	if len(closestCommands) > 0 {
		msg += "\n\nDid you mean one of these?\n"
		if len(closestCommands) == 1 {
			cmd := closestCommands[0]
			msg += fmt.Sprintf("        `%s`", cmd)
		} else {
			for _, cmd := range closestCommands {
				msg += fmt.Sprintf("        `%s`\n", cmd)
			}
		}
	}
	fatalIf(errDummy().Trace(), msg)
}

// findClosestCommands to match a given string with commands trie tree.
func findClosestCommands(command string) []string {
	closestCommands := commandsTree.PrefixMatch(command)
	sort.Strings(closestCommands)
	// Suggest other close commands - allow missed, wrongly added and even transposed characters
	for _, value := range commandsTree.Walk(commandsTree.Root()) {
		if sort.SearchStrings(closestCommands, value) < len(closestCommands) {
			continue
		}
		// 2 is arbitrary and represents the max allowed number of typed errors
		if words.DamerauLevenshteinDistance(command, value) < 2 {
			closestCommands = append(closestCommands, value)
		}
	}
	return closestCommands
}
