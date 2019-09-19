/*
 * M3 (C) 2019- MinIO, Inc.
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
	"crypto"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/inconshreveable/go-update"
	"github.com/mattn/go-isatty"
	"github.com/minio/cli"
	"github.com/minio/m3/pkg"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/probe"
	_ "github.com/minio/sha256-simd" // Needed for sha256 hash verifier.
	"github.com/segmentio/go-prompt"
)

// Check for new software updates.
var updateCmd = cli.Command{
	Name:   "update",
	Usage:  "update m3 to latest release",
	Action: mainUpdate,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "quiet, q",
			Usage: "disable any update prompt message",
		},
		cli.BoolFlag{
			Name:  "json",
			Usage: "enable JSON formatted output",
		},
	},
	CustomHelpTemplate: `Name:
   {{.HelpName}} - {{.Usage}}

USAGE:
   {{.HelpName}}{{if .VisibleFlags}} [FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXIT STATUS:
  0 - you are already running the most recent version
  1 - new update was applied successfully
 -1 - error in getting update information

EXAMPLES:
  1. Check and update m3:
     $ {{.HelpName}}
`,
}

const (
	releaseTagTimeLayout = "2006-01-02T15-04-05Z"
	osARCH               = runtime.GOOS + "-" + runtime.GOARCH
	releaseURL           = "https://dl.min.io/client/m3/release/" + osARCH + "/"
)

var (
	// Newer official download info URLs appear earlier below.
	releaseInfoURLs = []string{
		releaseURL + "m3.sha256sum",
		releaseURL + "m3.shasum",
	}

	// For windows our files have .exe additionally.
	releaseWindowsInfoURLs = []string{
		releaseURL + "m3.exe.sha256sum",
		releaseURL + "m3.exe.shasum",
	}
)

// Check for updates and print a notification message
func checkUpdate(ctx *cli.Context) {
	// Do not print update messages, if quiet flag is set.
	if ctx.Bool("quiet") || ctx.GlobalBool("quiet") {
		// Its OK to ignore any errors during doUpdate() here.
		if updateMsg, _, currentReleaseTime, latestReleaseTime, err := getUpdateInfo(2 * time.Second); err == nil {
			printMsg(updateMessage{
				Status:  "success",
				Message: updateMsg,
			})
		} else {
			printMsg(updateMessage{
				Status:  "success",
				Message: prepareUpdateMessage("Run `m3 update`", latestReleaseTime.Sub(currentReleaseTime)),
			})
		}
	}
}

// versionToReleaseTime - parses a standard official release
// mc version string.
//
// An official binary's version string is the release time formatted
// with RFC3339 (in UTC) - e.g. `2017-09-29T19:16:56Z`
func versionToReleaseTime(version string) (releaseTime time.Time, err *probe.Error) {
	var e error
	releaseTime, e = time.Parse(time.RFC3339, version)
	return releaseTime, probe.NewError(e)
}

// releaseTimeToReleaseTag - converts a time to a string formatted as
// an official mc release tag.
//
// An official mc release tag looks like:
// `RELEASE.2017-09-29T19-16-56Z`
func releaseTimeToReleaseTag(releaseTime time.Time) string {
	return "RELEASE." + releaseTime.Format(releaseTagTimeLayout)
}

// releaseTagToReleaseTime - reverse of `releaseTimeToReleaseTag()`
func releaseTagToReleaseTime(releaseTag string) (releaseTime time.Time, err *probe.Error) {
	tagTimePart := strings.TrimPrefix(releaseTag, "RELEASE.")
	if tagTimePart == releaseTag {
		return releaseTime, probe.NewError(fmt.Errorf("%s is not a valid release tag", releaseTag))
	}
	var e error
	releaseTime, e = time.Parse(releaseTagTimeLayout, tagTimePart)
	return releaseTime, probe.NewError(e)
}

// getModTime - get the file modification time of `path`
func getModTime(path string) (t time.Time, err *probe.Error) {
	var e error
	path, e = filepath.EvalSymlinks(path)
	if e != nil {
		return t, probe.NewError(fmt.Errorf("Unable to get absolute path of %s. %s", path, e))
	}

	// Version is mc non-standard, we will use mc binary's
	// ModTime as release time.
	var fi os.FileInfo
	fi, e = os.Stat(path)
	if e != nil {
		return t, probe.NewError(fmt.Errorf("Unable to get ModTime of %s. %s", path, e))
	}

	// Return the ModTime
	return fi.ModTime().UTC(), nil
}

// GetCurrentReleaseTime - returns this process's release time.  If it
// is official mc version, parsed version is returned else mc
// binary's mod time is returned.
func GetCurrentReleaseTime() (releaseTime time.Time, err *probe.Error) {
	if releaseTime, err = versionToReleaseTime(pkg.Version); err == nil {
		return releaseTime, nil
	}

	// Looks like version is mc non-standard, we use mc
	// binary's ModTime as release time:
	path, e := os.Executable()
	if e != nil {
		return releaseTime, probe.NewError(e)
	}
	return getModTime(path)
}

// IsDocker - returns if the environment mc is running in docker or
// not. The check is a simple file existence check.
//
// https://github.com/moby/moby/blob/master/daemon/initlayer/setup_unix.go#L25
//
//     "/.dockerenv":      "file",
//
func IsDocker() bool {
	_, e := os.Stat("/.dockerenv")
	if os.IsNotExist(e) {
		return false
	}

	return e == nil
}

// IsDCOS returns true if mc is running in DCOS.
func IsDCOS() bool {
	// http://mesos.apache.org/documentation/latest/docker-containerizer/
	// Mesos docker containerizer sets this value
	return os.Getenv("MESOS_CONTAINER_NAME") != ""
}

// IsKubernetes returns true if MinIO is running in kubernetes.
func IsKubernetes() bool {
	// Kubernetes env used to validate if we are
	// indeed running inside a kubernetes pod
	// is KUBERNETES_SERVICE_HOST but in future
	// we might need to enhance this.
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

// IsSourceBuild - returns if this binary is a non-official build from
// source code.
func IsSourceBuild() bool {
	_, err := versionToReleaseTime(pkg.Version)
	return err != nil
}

// DO NOT CHANGE USER AGENT STYLE.
// The style should be
//
//   mc (<OS>; <ARCH>[; dcos][; kubernetes][; docker][; source]) mc/<VERSION> mc/<RELEASE-TAG> mc/<COMMIT-ID>
//
// Any change here should be discussed by opening an issue at
// https://github.com/minio/mc/issues.
func getUserAgent() string {
	const name = "m3"
	userAgentParts := []string{}
	// Helper function to concisely append a pair of strings to a
	// the user-agent slice.
	uaAppend := func(p, q string) {
		userAgentParts = append(userAgentParts, p, q)
	}

	uaAppend(name+" (", runtime.GOOS)
	uaAppend("; ", runtime.GOARCH)
	if IsDCOS() {
		uaAppend("; ", "dcos")
	}
	if IsKubernetes() {
		uaAppend("; ", "kubernetes")
	}
	if IsDocker() {
		uaAppend("; ", "docker")
	}
	if IsSourceBuild() {
		uaAppend("; ", "source")
	}

	uaAppend(") "+name+"/", pkg.Version)
	uaAppend(" "+name+"/", pkg.ReleaseTag)
	uaAppend(" "+name+"/", pkg.CommitID)

	return strings.Join(userAgentParts, "")
}

func downloadReleaseURL(releaseChecksumURL string, timeout time.Duration) (content string, err *probe.Error) {
	req, e := http.NewRequest("GET", releaseChecksumURL, nil)
	if e != nil {
		return content, probe.NewError(e)
	}
	req.Header.Set("User-Agent", getUserAgent())

	client := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			// need to close connection after usage.
			DisableKeepAlives: true,
		},
	}

	resp, e := client.Do(req)
	if e != nil {
		return content, probe.NewError(e)
	}
	if resp == nil {
		return content, probe.NewError(fmt.Errorf("No response from server to download URL %s", releaseChecksumURL))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return content, probe.NewError(fmt.Errorf("Error downloading URL %s. Response: %v", releaseChecksumURL, resp.Status))
	}
	contentBytes, e := ioutil.ReadAll(resp.Body)
	if e != nil {
		return content, probe.NewError(fmt.Errorf("Error reading response. %s", err))
	}

	return string(contentBytes), nil
}

// DownloadReleaseData - downloads release data from mc official server.
func DownloadReleaseData(timeout time.Duration) (data string, err *probe.Error) {
	releaseURLs := releaseInfoURLs
	if runtime.GOOS == "windows" {
		releaseURLs = releaseWindowsInfoURLs
	}
	return func() (data string, err *probe.Error) {
		for _, url := range releaseURLs {
			data, err = downloadReleaseURL(url, timeout)
			if err == nil {
				return data, nil
			}
		}
		return data, err.Trace(releaseURLs...)
	}()
}

// parseReleaseData - parses release info file content fetched from
// official mc download server.
//
// The expected format is a single line with two words like:
//
// fbe246edbd382902db9a4035df7dce8cb441357d mc.RELEASE.2016-10-07T01-16-39Z
//
// The second word must be `m3.` appended to a standard release tag.
func parseReleaseData(data string) (sha256Hex string, releaseTime time.Time, err *probe.Error) {
	fields := strings.Fields(data)
	if len(fields) != 2 {
		return sha256Hex, releaseTime, probe.NewError(fmt.Errorf("Unknown release data `%s`", data))
	}

	sha256Hex = fields[0]
	releaseInfo := fields[1]

	fields = strings.SplitN(releaseInfo, ".", 2)
	if len(fields) != 2 {
		return sha256Hex, releaseTime, probe.NewError(fmt.Errorf("Unknown release information `%s`", releaseInfo))
	}
	if fields[0] != "m3" {
		return sha256Hex, releaseTime, probe.NewError(fmt.Errorf("Unknown release `%s`", releaseInfo))
	}

	releaseTime, err = releaseTagToReleaseTime(fields[1])
	if err != nil {
		return sha256Hex, releaseTime, err.Trace(fields...)
	}

	return sha256Hex, releaseTime, nil
}

func getLatestReleaseTime(timeout time.Duration) (sha256Hex string, releaseTime time.Time, err *probe.Error) {
	data, err := DownloadReleaseData(timeout)
	if err != nil {
		return sha256Hex, releaseTime, err.Trace()
	}

	return parseReleaseData(data)
}

func getDownloadURL(releaseTag string) (downloadURL string) {
	// Check if we are docker environment, return docker update command
	const cmd = "m3"
	if IsDocker() {
		// Construct release tag name.
		return fmt.Sprintf("docker pull minio/%s:%s", cmd, releaseTag)
	}

	// For binary only installations, we return link to the latest binary.
	if runtime.GOOS == "windows" {
		return releaseURL + cmd + ".exe"
	}

	return releaseURL + cmd
}

func getUpdateInfo(timeout time.Duration) (updateMsg string, sha256Hex string, currentReleaseTime, latestReleaseTime time.Time, err *probe.Error) {
	currentReleaseTime, err = GetCurrentReleaseTime()
	if err != nil {
		return updateMsg, sha256Hex, currentReleaseTime, latestReleaseTime, err.Trace()
	}

	sha256Hex, latestReleaseTime, err = getLatestReleaseTime(timeout)
	if err != nil {
		return updateMsg, sha256Hex, currentReleaseTime, latestReleaseTime, err.Trace()
	}

	var older time.Duration
	var downloadURL string
	if latestReleaseTime.After(currentReleaseTime) {
		older = latestReleaseTime.Sub(currentReleaseTime)
		downloadURL = getDownloadURL(releaseTimeToReleaseTag(latestReleaseTime))
	}

	return prepareUpdateMessage(downloadURL, older), sha256Hex, currentReleaseTime, latestReleaseTime, nil
}

var (
	// Check if we stderr, stdout are dumb terminals, we do not apply
	// ansi coloring on dumb terminals.
	isTerminal = func() bool {
		return isatty.IsTerminal(os.Stdout.Fd()) && isatty.IsTerminal(os.Stderr.Fd())
	}

	colorCyanBold = func() func(a ...interface{}) string {
		if isTerminal() {
			color.New(color.FgCyan, color.Bold).SprintFunc()
		}
		return fmt.Sprint
	}()

	colorYellowBold = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgYellow, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()

	colorGreenBold = func() func(format string, a ...interface{}) string {
		if isTerminal() {
			return color.New(color.FgGreen, color.Bold).SprintfFunc()
		}
		return fmt.Sprintf
	}()
)

func doUpdate(sha256Hex string, latestReleaseTime time.Time, ok bool) (updateStatusMsg string, err *probe.Error) {
	if !ok {
		updateStatusMsg = colorGreenBold("m3 update to version RELEASE.%s canceled.",
			latestReleaseTime.Format(releaseTagTimeLayout))
		return updateStatusMsg, nil
	}
	var sha256Sum []byte
	var e error
	sha256Sum, e = hex.DecodeString(sha256Hex)
	if e != nil {
		return updateStatusMsg, probe.NewError(e)
	}

	resp, e := http.Get(getDownloadURL(releaseTimeToReleaseTag(latestReleaseTime)))
	if e != nil {
		return updateStatusMsg, probe.NewError(e)
	}
	defer resp.Body.Close()

	// FIXME: add support for gpg verification as well.
	if e = update.Apply(resp.Body,
		update.Options{
			Hash:     crypto.SHA256,
			Checksum: sha256Sum,
		},
	); e != nil {
		return updateStatusMsg, probe.NewError(e)
	}

	return colorGreenBold("m3 updated to version RELEASE.%s successfully.",
		latestReleaseTime.Format(releaseTagTimeLayout)), nil
}

func shouldUpdate(quiet bool, sha256Hex string, latestReleaseTime time.Time) (ok bool) {
	ok = true
	if !quiet {
		ok = prompt.Confirm(colorGreenBold("Update to RELEASE.%s [%s]", latestReleaseTime.Format(releaseTagTimeLayout), "yes"))
	}
	return ok
}

type updateMessage struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// String colorized make bucket message.
func (s updateMessage) String() string {
	return s.Message
}

// JSON jsonified make bucket message.
func (s updateMessage) JSON() string {
	s.Status = "success"
	updateJSONBytes, e := json.MarshalIndent(s, "", " ")
	fatalIf(probe.NewError(e), "Unable to marshal into JSON.")

	return string(updateJSONBytes)
}

func mainUpdate(ctx *cli.Context) {
	const name = "m3"
	if len(ctx.Args()) != 0 {
		cli.ShowCommandHelpAndExit(ctx, "update", -1)
	}

	globalQuiet = ctx.Bool("quiet") || ctx.GlobalBool("quiet")
	globalJSON = ctx.Bool("json") || ctx.GlobalBool("json")

	updateMsg, sha256Hex, _, latestReleaseTime, err := getUpdateInfo(10 * time.Second)
	if err != nil {
		errorIf(err, "Unable to update ‘"+name+"’.")
		os.Exit(-1)
	}

	// Nothing to update running the latest release.
	color.New(color.FgGreen, color.Bold)
	if updateMsg == "" {
		printMsg(updateMessage{
			Status:  "success",
			Message: colorGreenBold("You are already running the most recent version of ‘" + name + "’."),
		})
		os.Exit(0)
	}

	printMsg(updateMessage{
		Status:  "success",
		Message: updateMsg,
	})

	// Avoid updating mc development, source builds.
	if strings.Contains(updateMsg, releaseURL) {
		isUpdate := shouldUpdate(globalQuiet || globalJSON, sha256Hex, latestReleaseTime)
		var updateStatusMsg string
		var err *probe.Error
		updateStatusMsg, err = doUpdate(sha256Hex, latestReleaseTime, isUpdate)
		if err != nil {
			errorIf(err, "Unable to update ‘"+name+"’.")
			os.Exit(-1)
		}
		printMsg(updateMessage{Status: "success", Message: updateStatusMsg})
		os.Exit(1)
	}
}

// prepareUpdateMessage - prepares the update message, only if a
// newer version is available.
func prepareUpdateMessage(downloadURL string, older time.Duration) string {
	if downloadURL == "" || older <= 0 {
		return ""
	}

	// Compute friendly duration string to indicate time
	// difference between newer and current release.
	t := time.Time{}
	newerThan := humanize.RelTime(t, t.Add(older), "ago", "")

	// Return the nicely colored and formatted update message.
	return colorizeUpdateMessage(downloadURL, newerThan)
}

// colorizeUpdateMessage - inspired from Yeoman project npm package https://github.com/yeoman/update-notifier
func colorizeUpdateMessage(updateString string, newerThan string) string {
	msgLine1Fmt := " You are running an older version of m3 released %s "
	msgLine2Fmt := " Update: %s "

	// Calculate length *without* color coding: with ANSI terminal
	// color characters, the result is incorrect.
	line1Length := len(fmt.Sprintf(msgLine1Fmt, newerThan))
	line2Length := len(fmt.Sprintf(msgLine2Fmt, updateString))

	// Populate lines with color coding.
	line1InColor := fmt.Sprintf(msgLine1Fmt, colorYellowBold(newerThan))
	line2InColor := fmt.Sprintf(msgLine2Fmt, colorCyanBold(updateString))

	// calculate the rectangular box size.
	maxContentWidth := int(math.Max(float64(line1Length), float64(line2Length)))

	// termWidth is set to a default one to use when we are
	// not able to calculate terminal width via OS syscalls
	termWidth := 25
	if width, err := pb.GetTerminalWidth(); err == nil {
		termWidth = width
	}

	// Box cannot be printed if terminal width is small than maxContentWidth
	if maxContentWidth > termWidth {
		return "\n" + line1InColor + "\n" + line2InColor + "\n\n"
	}

	topLeftChar := "┏"
	topRightChar := "┓"
	bottomLeftChar := "┗"
	bottomRightChar := "┛"
	horizBarChar := "━"
	vertBarChar := "┃"
	// on windows terminal turn off unicode characters.
	if runtime.GOOS == "windows" {
		topLeftChar = "+"
		topRightChar = "+"
		bottomLeftChar = "+"
		bottomRightChar = "+"
		horizBarChar = "-"
		vertBarChar = "|"
	}

	lines := []string{
		colorYellowBold(topLeftChar + strings.Repeat(horizBarChar, maxContentWidth) + topRightChar),
		vertBarChar + line1InColor + strings.Repeat(" ", maxContentWidth-line1Length) + vertBarChar,
		vertBarChar + line2InColor + strings.Repeat(" ", maxContentWidth-line2Length) + vertBarChar,
		colorYellowBold(bottomLeftChar + strings.Repeat(horizBarChar, maxContentWidth) + bottomRightChar),
	}
	return "\n" + strings.Join(lines, "\n") + "\n"
}
