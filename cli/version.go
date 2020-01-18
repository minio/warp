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

	"github.com/minio/warp/pkg"

	"github.com/fatih/color"
	"github.com/minio/cli"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/console"
	"github.com/minio/mc/pkg/probe"
)

// Print version.
var versionCmd = cli.Command{
	Name:   "version",
	Usage:  "show version info",
	Action: mainVersion,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "quiet, q",
			Usage: "suppress chatty console output",
		},
		cli.BoolFlag{
			Name:  "json",
			Usage: "enable JSON formatted output",
		},
	},
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}}{{if .VisibleFlags}} [FLAGS]{{end}}
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
EXAMPLES:
  1. Prints the MinIO Client version:
     $ {{.HelpName}}
`,
}

// Structured message depending on the type of console.
type versionMessage struct {
	Status      string `json:"status"`
	Version     string `json:"version"`
	ReleaseTime string `json:"release_time"`
	ReleaseTag  string `json:"release_tag"`
	CommitID    string `json:"commit_id"`
}

// Colorized message for console printing.
func (v versionMessage) String() string {
	return console.Colorize("Version", fmt.Sprintf("Version: %s\n", v.Version)) +
		console.Colorize("ReleaseTag", fmt.Sprintf("Release tag: %s\n", v.ReleaseTag)) +
		console.Colorize("ReleaseTime", fmt.Sprintf("Release time: %s\n", v.ReleaseTime)) +
		console.Colorize("CommitID", fmt.Sprintf("Commit-id: %s", v.CommitID))
}

// JSON'ified message for scripting.
func (v versionMessage) JSON() string {
	v.Status = "success"
	msgBytes, e := json.MarshalIndent(v, "", " ")
	fatalIf(probe.NewError(e), "Unable to marshal into JSON.")
	return string(msgBytes)
}

func mainVersion(ctx *cli.Context) error {
	// Additional command specific theme customization.
	console.SetColor("Version", color.New(color.FgGreen, color.Bold))
	console.SetColor("ReleaseTag", color.New(color.FgGreen))
	console.SetColor("ReleaseTime", color.New(color.FgGreen))
	console.SetColor("CommitID", color.New(color.FgGreen))

	verMsg := versionMessage{}
	verMsg.CommitID = pkg.CommitID
	verMsg.ReleaseTag = pkg.ReleaseTag
	verMsg.ReleaseTime = pkg.ReleaseTime
	verMsg.Version = pkg.Version

	if !globalQuiet {
		if ctx.Bool("json") {
			console.Println(verMsg.JSON())
		} else {
			console.Println(verMsg)
		}
	}
	return nil
}
