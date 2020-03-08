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

	"github.com/minio/warp/pkg"

	"github.com/fatih/color"
	"github.com/minio/cli"
	json "github.com/minio/mc/pkg/colorjson"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio/pkg/console"
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
  1. Prints warp version:
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
