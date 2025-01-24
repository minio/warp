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
	"errors"
	"fmt"
	"github.com/minio/cli"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/bench"
	"strconv"
	"strings"
)

var listFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "objects",
		Value: 10000,
		Usage: "Number of objects to upload. Rounded up to have equal concurrent objects.",
	},
	cli.IntFlag{
		Name:  "versions",
		Value: 1,
		Usage: "Number of versions to upload. If more than 1, versioned listing will be benchmarked",
	},
	cli.StringFlag{
		Name:  "obj.size",
		Value: "1KB",
		Usage: "Size of each generated object. Can be a number or 10KB/MB/GB. All sizes are base 2 binary.",
	},
	cli.BoolFlag{
		Name:  "metadata",
		Usage: "Enable extended MinIO ListObjects with metadata, by default this benchmarking uses ListObjectsV2 API.",
	},
	cli.BoolFlag{
		Name:  "nested",
		Usage: "Enable nested",
	},
	cli.StringFlag{
		Name:  "branchingFactors",
		Value: "2/2",
		Usage: "branchingFactors, specifying the branching factor at each level separated by /. eg: depth=3 branchingFactors=/12/30/1000 makes sense only when --nested",
	},
}

var ListCombinedFlags = combineFlags(globalFlags, ioFlags, listFlags, genFlags, benchFlags, analyzeFlags)

var listCmd = cli.Command{
	Name:   "list",
	Usage:  "benchmark list objects",
	Action: mainList,
	Before: setGlobalsFromContext,
	Flags:  ListCombinedFlags,
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} [FLAGS]
  -> see https://github.com/minio/warp#list

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainDelete is the entry point for get command.
func mainList(ctx *cli.Context) error {
	checkListSyntax(ctx)
	branchingFactors, _ := parseBranchingFactors(ctx.String("branchingFactors"))
	b := bench.List{
		Common:           getCommon(ctx, newGenSource(ctx, "obj.size")),
		Versions:         ctx.Int("versions"),
		Metadata:         ctx.Bool("metadata"),
		CreateObjects:    ctx.Int("objects"),
		NoPrefix:         ctx.Bool("noprefix"),
		Nested:           ctx.Bool("nested"),
		BranchingFactors: branchingFactors,
		FixedPrefix:      strings.Trim(ctx.String("prefix"), "/"), // reuse this as a fixed prefix when nesting enabled
	}
	return runBench(ctx, &b)
}

func checkListSyntax(ctx *cli.Context) {
	if ctx.NArg() > 0 {
		console.Fatal("Command takes no arguments")
	}
	if ctx.Int("versions") < 1 {
		console.Fatal("At least one version must be tested")
	}
	if ctx.Int("objects") < 1 {
		console.Fatal("At least one object must be tested")
	}

	checkAnalyze(ctx)
	checkBenchmark(ctx)
}

func parseBranchingFactors(branchingFactors string) ([]int, error) {
	strNumbers := strings.Split(strings.Trim(branchingFactors, "/"), "/")
	if len(strNumbers) == 1 && strNumbers[0] == "" {
		return nil, errors.New("branchingFactors string is empty or contains only slashes")
	}
	parsedNumbers := make([]int, 0, len(strNumbers))
	for _, strNum := range strNumbers {
		num, err := strconv.Atoi(strNum)
		if err != nil {
			return nil, fmt.Errorf("invalid number '%s' in branchingFactors: %v", strNum, err)
		}
		parsedNumbers = append(parsedNumbers, num)
	}
	return parsedNumbers, nil
}
