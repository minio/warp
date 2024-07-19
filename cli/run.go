/*
 * Warp (C) 2019-2024 MinIO, Inc.
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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"gopkg.in/yaml.v3"
)

var runCmd = cli.Command{
	Name:   "run",
	Usage:  "run benchmark defined in YAML file",
	Action: mainExec,
	Before: setGlobalsFromContext,
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:   "debug",
			Usage:  "enable debug logging before executing",
			Hidden: true,
		},
		cli.StringSliceFlag{
			Name:  "var",
			Usage: "Set variables for template replacement. Can be used multiple times. Example: ObjSize=1KB",
		},
	},
	CustomHelpTemplate: `NAME:
  {{.HelpName}} - {{.Usage}}

  Execute the benchmark as defined in YAML file. 
USAGE:
  {{.HelpName}} <file.yaml>
    -> see https://github.com/minio/warp#run

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}`,
}

// mainExec is the entry point for exe command.
func mainExec(ctx *cli.Context) error {
	var yFile []byte
	switch ctx.NArg() {
	case 1:
		b, err := os.ReadFile(ctx.Args()[0])
		if err != nil {
			fatal(probe.NewError(err), "error reading input file")
		}
		yFile = b
	default:
		fatal(errInvalidArgument(), "No YAML file specified")
	}

	// Do template replacements
	if vars := ctx.StringSlice("var"); len(vars) > 0 {
		replacements := make(map[string]string)
		for _, v := range vars {
			idx := strings.Index(v, "=")
			name := v[:idx]
			replacements[name] = v[idx+1:]
		}
		t, err := template.New("benchmark").Parse(string(yFile))
		if err != nil {
			fatal(probe.NewError(err), "error parsing template")
		}
		dst := new(bytes.Buffer)
		err = t.ExecuteTemplate(dst, "benchmark", replacements)
		if err != nil {
			fatal(probe.NewError(err), "error parsing template")
		}
		yFile = dst.Bytes()
	}

	// Unmarshal into map.
	var doc map[string]any
	err := yaml.Unmarshal(yFile, &doc)
	if err != nil {
		fatal(probe.NewError(err), "error parsing YAML file")
	}
	doc, ok := doc["warp"].(map[string]any)
	if !ok {
		fatal(probe.NewError(err), "Expected top level 'warp' element could not be found")
	}
	switch ver := mustGetString(doc, "api"); ver {
	case "v1":
	default:
		fatal(probe.NewError(fmt.Errorf("unsupported api: %s", ver)), "Incompatible API version")
	}
	delete(doc, "api")
	op := mustGetString(doc, "benchmark")
	var benchCmd *cli.Command
	for i, cmd := range benchCmds {
		if cmd.Name == op {
			benchCmd = &benchCmds[i]
			break
		}
	}
	if benchCmd == nil {
		fatal(probe.NewError(fmt.Errorf("unknown benchmark: %s", op)), "Unknown benchmark")
	}
	delete(doc, "benchmark")

	// Rename input fields to commandline params:
	rename := map[string]string{
		"sse-c-encrypt":            "encrypt",
		"analyze.verbose":          "analyze.v",
		"obj.part-size":            "part.size",
		"analyze.skip-duration":    "analyze.skip",
		"analyze.segment-duration": "analyze.dur",
		"analyze.filter-op":        "analyze.op",
		"server-profile":           "serverprof",
		"bench-data":               "benchdata",
		"autoterm.enabled":         "autoterm",
		"obj.versions":             "versions",
		"obj.rand-size":            "obj.randsize",
		"no-prefix":                "noprefix",
		"no-clear":                 "noclear",
		"distribution.get":         "get-distrib",
		"distribution.stat":        "stat-distrib",
		"distribution.put":         "put-distrib",
		"distribution.delete":      "delete-distrib",
		"obj.parts":                "parts",
	}

	// Allow some fields to be string lists
	commaListed := map[string]bool{
		"server-profile": true,
		"remote.host":    true,
		"warp-client":    true,
	}

	var prefixStack []string
	var currDept []string
	var flags = map[string]string{}
	setFlag := func(key string, value any) {
		name := strings.Join(append(prefixStack, key), ".")
		ogName := strings.Join(append(currDept, key), ".")

		if alt := rename[name]; alt != "" {
			name = alt
		}
		var flag cli.Flag
		for _, f := range benchCmd.Flags {
			if strings.Split(f.GetName(), ",")[0] == name {
				flag = f
				break
			}
		}
		if value == nil {
			if globalDebug {
				fmt.Printf("%s => --%s=(default)\n", ogName, name)
			}
			// Ignore unset values
			return
		}
		if flag == nil {
			section := strings.Join(currDept, ".")
			fatal(probe.NewError(fmt.Errorf("unknown key: %q in section %q", name, section)), "Unknown benchmark flag")
		}

		var err error
		var setFlag []byte
		switch flag.(type) {
		case cli.BoolFlag:
			if v, ok := value.(bool); !ok {
				err = fmt.Errorf("value of %s must be a bool", ogName)
			} else {
				setFlag, err = json.Marshal(v)
			}
		case cli.StringFlag, cli.DurationFlag:
			var wasList bool
			if commaListed[ogName] {
				if v, ok := value.([]any); ok {
					wasList = true
					var all []string
					for i, v := range v {
						value, ok := v.(string)
						if !ok {
							err = fmt.Errorf("value of %s item %d must be a string", ogName, i+1)
							break
						}
						all = append(all, value)
					}
					setFlag = []byte(strings.Join(all, ","))
				}
			}
			if !wasList {
				if v, ok := value.(string); !ok {
					err = fmt.Errorf("value of %s must be a string, was %T", ogName, value)
				} else {
					setFlag = []byte(v)
				}
			}
		case cli.IntFlag, cli.Float64Flag, cli.UintFlag, cli.Uint64Flag:
			switch v := value.(type) {
			case float64, int:
				setFlag, err = json.Marshal(v)
			default:
				err = fmt.Errorf("value of %s must be a number, was %T", ogName, value)
			}

		default:
			err = fmt.Errorf("unknown flag type %T for key %s", flag, ogName)
		}
		if err != nil {
			fatal(probe.NewError(err), "error parsing config")
			return
		}
		if _, ok := flags[name]; ok {
			fatal(probe.NewError(fmt.Errorf("duplicate benchmark flag: %s", ogName)), "duplicate benchmark flag")
		}
		flags[name] = string(setFlag)
		if globalDebug {
			fmt.Printf("%s => --%s=%s\n", ogName, name, string(setFlag))
		}
	}
	parseDoc(doc, &prefixStack, &currDept, setFlag)

	// Reconstruct command
	app := registerApp("warp", benchCmds)
	fs, err := flagSet(benchCmd.Name, benchCmd.Flags, nil)
	if err != nil {
		fatal(probe.NewError(err), "error setting flags")
	}
	ctx2 := cli.NewContext(app, fs, ctx)
	ctx2.Command = *benchCmd
	for k, v := range flags {
		err := ctx2.Set(k, v)
		if err != nil {
			err := fmt.Errorf("parsing parameters (%v:%v): %w", k, v, err)
			fatal(probe.NewError(err), "error setting flags")
		}
	}

	return runCommand(ctx2, benchCmd)
}

func parseDoc(doc map[string]any, prefixStack, printStack *[]string, setFlag func(key string, value any)) {
	push := func(stack *[]string, s string) func() {
		v := *stack
		v = append(v, s)
		*stack = v
		return func() {
			v := *stack
			v = v[:len(v)-1]
			*stack = v
		}
	}
	for k, v := range doc {
		switch k {
		case "analyze", "obj", "autoterm", "distribution":
			// These automatically adds the prefix to the flag name.
			pop := push(prefixStack, k)
			pop2 := push(printStack, k)
			for k, v := range v.(map[string]any) {
				setFlag(k, v)
			}
			pop()
			pop2()

		case "io", "remote", "params", "advanced":
			// These are just added to the top level.
			pop := push(printStack, k)
			parseDoc(v.(map[string]any), prefixStack, printStack, setFlag)
			pop()
		default:
			setFlag(k, v)
		}
	}
}

func mustGetString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		fatal(probe.NewError(fmt.Errorf("value of '%s' not found", key)), "Missing key")
	}
	val, ok := v.(string)
	if !ok {
		fatal(probe.NewError(fmt.Errorf("value of '%s' must be a string", key)), "Invalid type")
	}
	return val
}
