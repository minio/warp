/*
 * MinIO Client (C) 2014, 2015 MinIO, Inc.
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

package pkg

var (
	// Version - the version being released (v prefix stripped)
	Version = "(dev)"
	// ReleaseTag - the current git tag
	ReleaseTag = "(no tag)"
	// ReleaseTime - current UTC date in RFC3339 format.
	ReleaseTime = "(no release)"
	// CommitID - latest commit id.
	CommitID = "(dev)"
	// ShortCommitID - first 12 characters from CommitID.
	ShortCommitID = "(dev)"
)
