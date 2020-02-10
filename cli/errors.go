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

	"github.com/minio/mc/pkg/probe"
)

type dummyErr error

var errDummy = func() *probe.Error {
	msg := ""
	return probe.NewError(dummyErr(errors.New(msg))).Untrace()
}

type invalidArgumentErr error

var errInvalidArgument = func() *probe.Error {
	msg := "Invalid arguments provided, please refer " + "`" + appName + " <command> -h` for relevant documentation."
	return probe.NewError(invalidArgumentErr(errors.New(msg))).Untrace()
}
