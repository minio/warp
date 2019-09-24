package sse

import (
	"path/filepath"
	"runtime"
	"strings"
)

// url2Alias separates alias and path from the URL. Aliased URL is of
// the form alias/path/to/blah.
func url2Alias(aliasedURL string) (alias, path string) {
	// Save aliased url.
	urlStr := aliasedURL

	// Convert '/' on windows to filepath.Separator.
	urlStr = filepath.FromSlash(urlStr)

	if runtime.GOOS == "windows" {
		// Remove '/' prefix before alias if any to support '\\home' alias
		// style under Windows
		urlStr = strings.TrimPrefix(urlStr, string(filepath.Separator))
	}

	// Remove everything after alias (i.e. after '/').
	urlParts := strings.SplitN(urlStr, string(filepath.Separator), 2)
	if len(urlParts) == 2 {
		// Convert windows style path separator to Unix style.
		return urlParts[0], urlParts[1]
	}
	return urlParts[0], ""
}
