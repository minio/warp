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
	"encoding/json"
	"fmt"
	"time"
	"strings"
	"sync"
	"os/exec"
	"unicode"

	"github.com/cheggaaa/pb"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/pkg/v2/console"
)

// causeMessage container for golang error messages
type causeMessage struct {
	Error   error  `json:"error"`
	Message string `json:"message"`
}

// errorMessage container for error messages
type errorMessage struct {
	Cause     causeMessage       `json:"cause"`
	SysInfo   map[string]string  `json:"sysinfo"`
	Message   string             `json:"message"`
	Type      string             `json:"type"`
	CallTrace []probe.TracePoint `json:"trace,omitempty"`
}

var printMu sync.Mutex

func printInfo(data ...interface{}) {
	printMu.Lock()
	defer printMu.Unlock()
	w, _ := pb.GetTerminalWidth()
	if w > 0 {
		fmt.Print("\r", strings.Repeat(" ", w), "\r")
	} else {
		data = append(data, "\n")
	}
	console.Info(data...)
}

func printError(data ...interface{}) {
	printMu.Lock()
	defer printMu.Unlock()
	w, _ := pb.GetTerminalWidth()
	if w > 0 {
		fmt.Print("\r", strings.Repeat(" ", w), "\r")
	} else {
		data = append(data, "\n")
	}
	//put the timestamp in an interface so we can combine it with the actual error message
	timestamp := []interface{}{time.Now().Format(time.DateTime)}
	data = append(timestamp, data)
	console.Errorln( data...)
	if len(globalFailCmd) > 0 {
		console.Info(fmt.Sprintf("Executing %s\r", globalFailCmd))

		//cmd := exec.Command( globalExitOnFailCmd)
		//err := cmd.Run()
		out, err := exec.Command( globalFailCmd).Output()
		if err != nil {
			console.Info(fmt.Sprintf("Unable to run %s: err %s\r", globalFailCmd, err))
		} else {
			console.Info(fmt.Sprintf("Successfully ran %s: output\n %s\r", globalFailCmd, out))
		}

	}

	if globalExitOnFailure {
		console.Fatalln(fmt.Sprintf("Exiting on first error due to cli request"))
	}

}

// fatalIf wrapper function which takes error and selectively prints stack frames if available on debug
func fatalIf(err *probe.Error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	fatal(err, msg, data...)
}

func fatal(err *probe.Error, msg string, data ...interface{}) {
	if globalJSON {
		errorMsg := errorMessage{
			Message: msg,
			Type:    "fatal",
			Cause: causeMessage{
				Message: err.ToGoError().Error(),
				Error:   err.ToGoError(),
			},
			SysInfo: err.SysInfo,
		}
		if globalDebug {
			errorMsg.CallTrace = err.CallTrace
		}
		json, e := json.MarshalIndent(struct {
			Error  errorMessage `json:"error"`
			Status string       `json:"status"`
		}{
			Status: "error",
			Error:  errorMsg,
		}, "", " ")
		if e != nil {
			console.Fatalln(probe.NewError(e))
		}
		console.Infoln(string(json))
		console.Fatalln()
	}

	msg = fmt.Sprintf(msg, data...)
	errmsg := err.String()
	if !globalDebug {
		errmsg = err.ToGoError().Error()
	}

	// Remove unnecessary leading spaces in generic/detailed error messages
	msg = strings.TrimSpace(msg)
	errmsg = strings.TrimSpace(errmsg)

	// Add punctuations when needed
	if len(errmsg) > 0 && len(msg) > 0 {
		if msg[len(msg)-1] != ':' && msg[len(msg)-1] != '.' {
			// The detailed error message starts with a capital letter,
			// we should then add '.', otherwise add ':'.
			if unicode.IsUpper(rune(errmsg[0])) {
				msg += "."
			} else {
				msg += ":"
			}
		}
		// Add '.' to the detail error if not found
		if errmsg[len(errmsg)-1] != '.' {
			errmsg += "."
		}
	}
	fmt.Println("")
	console.Fatalln(fmt.Sprintf("%s %s", msg, errmsg))
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func errorIf(err *probe.Error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	if globalJSON {
		errorMsg := errorMessage{
			Message: fmt.Sprintf(msg, data...),
			Type:    "error",
			Cause: causeMessage{
				Message: err.ToGoError().Error(),
				Error:   err.ToGoError(),
			},
			SysInfo: err.SysInfo,
		}
		if globalDebug {
			errorMsg.CallTrace = err.CallTrace
		}
		json, e := json.MarshalIndent(struct {
			Error  errorMessage `json:"error"`
			Status string       `json:"status"`
		}{
			Status: "error",
			Error:  errorMsg,
		}, "", " ")
		if e != nil {
			console.Fatalln(probe.NewError(e))
		}
		console.Infoln(string(json))
		return
	}
	msg = fmt.Sprintf(msg, data...)
	if !globalDebug {
		console.Errorln(fmt.Sprintf("%s %s", msg, err.ToGoError()))
		return
	}
	fmt.Println("")
	console.Errorln(fmt.Sprintf("%s %s", msg, err))
}
