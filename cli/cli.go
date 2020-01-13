/*
 * Warp (C) 2019-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/minio/cli"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/trie"
	"github.com/minio/minio/pkg/words"
	"github.com/minio/warp/pkg"
	"github.com/pkg/profile"
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

func Main(args []string) {
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

	// Set the mc app name.
	appName := filepath.Base(args[0])

	// Run the app - exit on error.
	if err := registerApp(appName, appCmds).Run(args); err != nil {
		os.Exit(1)
	}
}
func init() {
	a := []cli.Command{
		getCmd,
		putCmd,
		deleteCmd,
		listCmd,
	}
	b := []cli.Command{
		analyzeCmd,
		cmpCmd,
		mergeCmd,
		clientCmd,
		updateCmd,
		versionCmd,
	}
	appCmds = append(a, b...)
	benchCmds = a
}

var appCmds, benchCmds []cli.Command

func combineFlags(flags ...[]cli.Flag) []cli.Flag {
	var dst []cli.Flag
	for _, fl := range flags {
		for _, flag := range fl {
			dst = append(dst, flag)
		}
	}
	return dst
}

// Collection of mc commands currently supported
var commands = []cli.Command{}

// Collection of mc commands currently supported in a trie tree
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
		if strings.HasPrefix(pkg.ReleaseTag, "RELEASE.") {
			// Check for new updates from dl.min.io.
			checkUpdate(ctx)
		}

		if ctx.Bool("autocompletion") || ctx.GlobalBool("autocompletion") {
			// Install shell completions
			installAutoCompletion()
			return
		}

		cli.ShowAppHelp(ctx)
	}

	app.Before = func(ctx *cli.Context) error {
		var after []func()
		if s := ctx.String("cpuprofile"); s != "" {
			after = append(after, profile.Start(profile.CPUProfile, profile.ProfilePath(s)).Stop)
		}
		if s := ctx.String("memprofile"); s != "" {
			after = append(after, profile.Start(profile.MemProfile, profile.ProfilePath(s)).Stop)
		}
		if s := ctx.String("blockprofile"); s != "" {
			after = append(after, profile.Start(profile.BlockProfile, profile.ProfilePath(s)).Stop)
		}
		if s := ctx.String("mutexprofile"); s != "" {
			after = append(after, profile.Start(profile.MutexProfile, profile.ProfilePath(s)).Stop)
		}
		if s := ctx.String("trace"); s != "" {
			after = append(after, profile.Start(profile.TraceProfile, profile.ProfilePath(s)).Stop)
		}
		if len(after) == 0 {
			return nil
		}
		x := app.After
		app.After = func(ctx *cli.Context) error {
			err := x(ctx)
			if err != nil {
				return err
			}
			for _, fn := range after {
				fn()
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
	app.Usage = "MinIO Benchmark tool for S3 compatible systems."
	app.Commands = commands
	app.Author = "MinIO, Inc."
	app.Version = pkg.ReleaseTag
	app.Flags = append(app.Flags, globalFlags...)
	//app.CustomAppHelpTemplate = mcHelpTemplate
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
		msg += fmt.Sprintf("\n\nDid you mean one of these?\n")
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
	var closestCommands []string
	for _, value := range commandsTree.PrefixMatch(command) {
		closestCommands = append(closestCommands, value.(string))
	}
	sort.Strings(closestCommands)
	// Suggest other close commands - allow missed, wrongly added and even transposed characters
	for _, value := range commandsTree.Walk(commandsTree.Root()) {
		if sort.SearchStrings(closestCommands, value.(string)) < len(closestCommands) {
			continue
		}
		// 2 is arbitrary and represents the max allowed number of typed errors
		if words.DamerauLevenshteinDistance(command, value.(string)) < 2 {
			closestCommands = append(closestCommands, value.(string))
		}
	}
	return closestCommands
}
