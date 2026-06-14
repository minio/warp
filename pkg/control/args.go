/*
 * Warp (C) 2019-2026 MinIO, Inc.
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

package control

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// validMethods lists the benchmark commands the control plane can launch.
var validMethods = map[string]bool{
	"get": true, "put": true, "mixed": true,
	"delete": true, "stat": true, "list": true,
}

// objectsFlagMethods are the commands that accept --objects (a count of
// pre-created objects to operate on). put creates objects, so it has no such flag.
var objectsFlagMethods = map[string]bool{
	"get": true, "mixed": true, "delete": true, "stat": true, "list": true,
}

// buildArgs maps a scenario spec + target + client set onto a warp command line.
// It returns the argument slice (command first) and any environment overrides.
//
// Credentials are passed as --access-key/--secret-key flags, not via env, because
// in distributed mode warp only forwards a flag to its clients when ctx.IsSet is
// true, and minio/cli reports env-derived values as "not set". Without this the
// load-generating clients would connect anonymously and get access denied. The
// trade-off is that credentials are visible in the coordinator's process args,
// acceptable for a single-user local control plane.
func buildArgs(spec ScenarioSpec, t *Target, clientAddrs []string, benchDataBase string) ([]string, []string, error) {
	method := strings.ToLower(strings.TrimSpace(spec.Method))
	if !validMethods[method] {
		return nil, nil, fmt.Errorf("unsupported method %q", spec.Method)
	}
	if len(clientAddrs) == 0 {
		return nil, nil, fmt.Errorf("no clients selected")
	}
	if t.Endpoint == "" || t.Bucket == "" {
		return nil, nil, fmt.Errorf("target endpoint and bucket are required")
	}

	args := []string{
		method,
		"--warp-client", strings.Join(clientAddrs, ","),
		"--host", t.Endpoint,
		"--bucket", t.Bucket,
		"--access-key", t.AccessKey,
		"--secret-key", t.SecretKey,
		"--benchdata", benchDataBase,
		"--no-color",
	}
	if t.TLS {
		args = append(args, "--tls")
	}
	if t.Region != "" {
		args = append(args, "--region", t.Region)
	}
	if spec.Duration != "" {
		args = append(args, "--duration", spec.Duration)
	}
	if spec.ObjSize != "" {
		args = append(args, "--obj.size", spec.ObjSize)
	}
	if spec.Concurrent > 0 {
		args = append(args, "--concurrent", strconv.Itoa(spec.Concurrent))
	}
	if spec.Objects > 0 && objectsFlagMethods[method] {
		args = append(args, "--objects", strconv.Itoa(spec.Objects))
	}
	if method == "mixed" {
		args = append(args,
			"--get-distrib", strconv.Itoa(spec.GetDistrib),
			"--put-distrib", strconv.Itoa(spec.PutDistrib),
			"--stat-distrib", strconv.Itoa(spec.StatDistrib),
			"--delete-distrib", strconv.Itoa(spec.DeleteDistrib),
		)
	}

	// Pass-through extra flags, sorted for deterministic command lines. A flag
	// with an empty value is emitted bare (e.g. --noclear) so boolean flags work;
	// the leading "--" is trimmed if the user already typed it.
	keys := make([]string, 0, len(spec.ExtraFlags))
	for k := range spec.ExtraFlags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		name := "--" + strings.TrimLeft(strings.TrimSpace(k), "-")
		if v := spec.ExtraFlags[k]; v != "" {
			args = append(args, name, v)
		} else {
			args = append(args, name)
		}
	}

	// No env overrides needed; credentials are passed as flags above.
	return args, nil, nil
}
