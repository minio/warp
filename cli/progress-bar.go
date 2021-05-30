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
	"runtime"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
	"github.com/minio/pkg/console"
)

// progress extender.
type progressBar struct {
	*pb.ProgressBar
}

// newProgressBar - instantiate a progress bar.
func newProgressBar(total int64, units pb.Units) *progressBar {
	// Progress bar specific theme customization.
	console.SetColor("Bar", color.New(color.FgGreen, color.Bold))

	pgbar := progressBar{}

	// get the new original progress bar.
	bar := pb.New64(total)

	// Set new human friendly print units.
	bar.SetUnits(pb.U_DURATION)

	// Refresh rate for progress bar is set to 125 milliseconds.
	bar.SetRefreshRate(time.Millisecond * 125)

	// Do not print a newline by default handled, it is handled manually.
	bar.NotPrint = true

	// Show current speed is true.
	bar.ShowSpeed = false
	bar.ShowTimeLeft = false
	bar.ManualUpdate = true

	// Custom callback with colorized bar.
	bar.Callback = func(s string) {
		printMu.Lock()
		defer printMu.Unlock()
		console.Print(console.Colorize("Bar", "\r"+s+"\r"))
	}

	// Use different unicodes for Linux, OS X and Windows.
	switch runtime.GOOS {
	case "linux", "windows":
		// Need to add '\x00' as delimiter for unicode characters.
		bar.Format("┃\x00▓\x00█\x00░\x00┃")
	case "darwin":
		// Need to add '\x00' as delimiter for unicode characters.
		bar.Format(" \x00▓\x00 \x00░\x00 ")
	default:
		// Default to non unicode characters.
		bar.Format("[=> ]")
	}

	// Start the progress bar.
	if bar.Total > 0 {
		bar.Start()
	}

	// Copy for future
	pgbar.ProgressBar = bar

	// Return new progress bar here.
	return &pgbar
}

// Set caption.
func (p *progressBar) SetCaption(caption string) *progressBar {
	caption = fixateBarCaption(caption, getFixedWidth(p.ProgressBar.GetWidth(), 18))
	p.ProgressBar.Prefix(caption)
	return p
}

func (p *progressBar) Set64(length int64) *progressBar {
	p.ProgressBar = p.ProgressBar.Set64(length)
	return p
}

func (p *progressBar) SetTotal(total int64) *progressBar {
	p.ProgressBar.Total = total
	return p
}

// fixateBarCaption - fancify bar caption based on the terminal width.
func fixateBarCaption(caption string, width int) string {
	switch {
	case len(caption) > width:
		// Trim caption to fit within the screen
		trimSize := len(caption) - width + 3
		if trimSize < len(caption) {
			caption = "..." + caption[trimSize:]
		}
	case len(caption) < width:
		caption += strings.Repeat(" ", width-len(caption))
	}
	return caption
}

// getFixedWidth - get a fixed width based for a given percentage.
func getFixedWidth(width, percent int) int {
	return width * percent / 100
}
