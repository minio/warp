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
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minio/pkg/v3/console"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
	"github.com/muesli/termenv"
)

type ui struct {
	progress     progress.Model
	pct          atomic.Pointer[float64]
	phase        atomic.Pointer[string]
	phaseTxt     atomic.Pointer[string]
	updates      atomic.Pointer[chan<- aggregate.UpdateReq]
	start, end   atomic.Pointer[time.Time]
	pause        atomic.Bool
	quitPls      atomic.Bool
	showProgress bool
	cancelFn     atomic.Pointer[context.CancelFunc]
	quitCh       chan struct{}
}

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second/2, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (u *ui) Init() tea.Cmd {
	u.progress = progress.New(progress.WithScaledGradient("#c72e49", "#edf7f7"), progress.WithSolidFill("#c72e49"))
	u.quitCh = make(chan struct{})
	return tea.Batch(tickCmd())
}

func (u *ui) Run() {
	p := tea.NewProgram(u)

	if _, err := p.Run(); err != nil {
		fmt.Printf("UI: %v", err)
	}
	close(u.quitCh)
	if c := u.cancelFn.Load(); c != nil {
		cancel := *c
		cancel()
	}
	u.quitPls.Store(true)
}

func (u *ui) Wait() {
	if u.quitCh != nil {
		<-u.quitCh
	}
}

const (
	padding  = 2
	maxWidth = 80
)

func (u *ui) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if u.quitPls.Load() {
		return u, tea.Quit
	}
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		case "ctrl+c", "q":
			return u, tea.Quit
		}
	case tea.QuitMsg:
		u.quitPls.Store(true)
		return u, tea.Quit
	case tea.WindowSizeMsg:
		u.progress.Width = msg.Width - 4
		if u.progress.Width > maxWidth-padding {
			u.progress.Width = maxWidth - padding
		}
	case tickMsg:
		batch := []tea.Cmd{tickCmd()}
		u.showProgress = false
		if p := u.pct.Load(); p != nil {
			u.showProgress = true
			batch = append(batch, u.progress.SetPercent(*p))
		} else if start := u.start.Load(); start != nil {
			u.showProgress = true
			end := u.end.Load()
			now := time.Now()
			switch {
			case now.Before(*start):
				batch = append(batch, u.progress.SetPercent(0))
			case now.After(*end):
				batch = append(batch, u.progress.SetPercent(1))
			default:
				a, b := start.UnixNano(), end.UnixNano()
				pct := float64(now.UnixNano()-a) / float64(b-a)
				batch = append(batch, u.progress.SetPercent(pct))
				u.progress.SetPercent(pct)
			}
		}
		if u.quitPls.Load() {
			batch = append(batch, tea.Quit)
		}

		return u, tea.Batch(batch...)

	case progress.FrameMsg:
		// FrameMsg is sent when the progress bar wants to animate itself
		progressModel, cmd := u.progress.Update(msg)
		u.progress = progressModel.(progress.Model)
		return u, cmd
	}

	return u, nil
}

func (u *ui) View() string {
	res := titleStyle.Render("WARP S3 Benchmark Tool by MinIO")
	res += "\n"

	if ph := u.phase.Load(); ph != nil {
		status := "\n" + *ph
		if ph := u.phaseTxt.Load(); ph != nil {
			status += ": " + *ph
		}
		status += "...\n\n"
		res += statusStyle.Render(status)
	}

	res += defaultStyle.Render("\r λ ")
	if u.showProgress {
		res += u.progress.View() + "\n"
	} else {
		res += "\n"
	}
	if up := u.updates.Load(); up != nil {
		reqCh := *up
		respCh := make(chan *aggregate.Realtime, 1)
		reqCh <- aggregate.UpdateReq{C: respCh}
		var resp *aggregate.Realtime
		select {
		case resp = <-respCh:
		case <-time.After(time.Second):
		}
		if resp != nil {
			nBytes := strings.TrimSuffix(bench.Throughput(resp.Total.TotalBytes).String(), "/s")
			stats := fmt.Sprintf("\nReqs: %d, Errs:%d, Objs:%d, Bytes: %s\n", resp.Total.TotalRequests, resp.Total.TotalErrors, resp.Total.TotalObjects, nBytes)
			ops := stringKeysSorted(resp.ByOpType)
			for _, op := range ops {
				tp := resp.ByOpType[op].Throughput
				segs := tp.Segmented
				if segs == nil || len(segs.Segments) == 0 {
					continue
				}
				stats += fmt.Sprintf(" -%10s Average: %.0f Obj/s, %s", op, tp.ObjectsPS(), tp.BytesPS().String())
				segs.Segments.SortByStartTime()
				lastOps := segs.Segments[len(segs.Segments)-1]
				if time.Since(lastOps.Start) > 15*time.Second {
					stats += "\n"
					continue
				}
				stats += fmt.Sprintf("; Current %.0f Obj/s, %s", lastOps.OPS, bench.Throughput(lastOps.BPS))
				if len(resp.ByOpType[op].Requests) == 0 {
					stats += ".\n"
					continue
				}

				var totalDur float64
				var totalTTFB float64
				var totalRequests int
				for _, reqs := range resp.ByOpType[op].Requests {
					if len(reqs) == 0 {
						continue
					}
					lastReq := reqs[len(reqs)-1]
					if time.Since(lastReq.EndTime) > 30*time.Second {
						continue
					}
					if lastReq.Single != nil {
						totalDur += lastReq.Single.DurAvgMillis
						if lastReq.Single.FirstByte != nil {
							totalTTFB += lastReq.Single.FirstByte.AverageMillis
						}
						totalRequests += lastReq.Single.MergedEntries
					}
					if lastReq.Multi != nil {
						for _, reqs := range lastReq.Multi.ByHost {
							totalDur += reqs.AvgDurationMillis
							if reqs.FirstByte != nil {
								totalTTFB += reqs.FirstByte.AverageMillis
							}
							totalRequests += reqs.MergedEntries
						}
					}
				}
				if totalRequests > 0 {
					stats += fmt.Sprintf(", %.1f ms/req", totalDur/float64(totalRequests))
					if totalTTFB > 0 {
						stats += fmt.Sprintf(", TTFB: %.1fms", totalTTFB/float64(totalRequests))
					}
				}
				stats += "\n"
			}
			res += statsStyle.Render(stats)
		}
	}
	return res + "\n"
}

func (u *ui) SetSubText(caption string) {
	if u.quitPls.Load() == true {
		u.Wait()
		console.Printf("\r%-80s", caption)
		return
	}
	u.phaseTxt.Store(&caption)
}

func (u *ui) SetPhase(caption string) {
	if u.quitPls.Load() == true {
		u.Wait()
		console.Println("\n" + caption)
		return
	}
	u.phase.Store(&caption)
	u.phaseTxt.Store(nil)
}

func (u *ui) StartPrepare(caption string, progress <-chan float64, ur chan<- aggregate.UpdateReq) {
	u.phase.Store(&caption)
	u.phaseTxt.Store(nil)
	if ur != nil {
		u.updates.Store(&ur)
	}
	if progress != nil {
		go func() {
			for p := range progress {
				u.pct.Store(&p)
			}
			u.pct.Store(nil)
		}()
	} else {
		u.pct.Store(nil)
	}
}

func (u *ui) StartBenchmark(caption string, start, end time.Time, ur chan<- aggregate.UpdateReq) {
	u.phase.Store(&caption)
	u.phaseTxt.Store(nil)
	u.end.Store(&end)
	u.start.Store(&start)
	if ur != nil {
		u.updates.Store(&ur)
	}
}

func (u *ui) Pause(b bool) {
	u.pause.Store(b)
}

const borderCol = lipgloss.Color("#c72e49")

var (
	titleStyle = func() lipgloss.Style {
		b := lipgloss.RoundedBorder()
		return lipgloss.NewStyle().BorderStyle(b).Padding(0, 1).Foreground(lipgloss.ANSIColor(termenv.ANSIBrightWhite)).BorderForeground(borderCol)
	}()

	defaultStyle = func() lipgloss.Style {
		return lipgloss.NewStyle().Foreground(lipgloss.ANSIColor(termenv.ANSIWhite))
	}()
	statusStyle = func() lipgloss.Style {
		return lipgloss.NewStyle().Foreground(lipgloss.ANSIColor(termenv.ANSIBrightBlue))
	}()
	statsStyle = func() lipgloss.Style {
		return lipgloss.NewStyle().Foreground(lipgloss.ANSIColor(termenv.ANSIWhite))
	}()
)
