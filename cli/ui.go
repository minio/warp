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
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/minio/warp/pkg/aggregate"
	"github.com/minio/warp/pkg/bench"
)

type ui struct {
	progress      progress.Model
	pct           atomic.Pointer[float64]
	phase         atomic.Pointer[string]
	phaseTxt      atomic.Pointer[string]
	updates       atomic.Pointer[chan<- aggregate.UpdateReq]
	start, end    atomic.Pointer[time.Time]
	pause         atomic.Bool
	txtOnly       atomic.Bool
	reportBuf     atomic.Pointer[bytes.Buffer]
	showProgress  bool
	gotWindowSize bool
	quit          bool
	viewport      viewport.Model
}

const useHighPerformanceRenderer = false

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second/2, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (u *ui) Init() tea.Cmd {
	u.progress = progress.New(progress.WithScaledGradient("#c72e49", "#edf7f7"), progress.WithSolidFill("#c72e49"))
	return tickCmd()
}

func (u *ui) Run() {
	p := tea.NewProgram(u)
	if _, err := p.Run(); err != nil {
		fmt.Printf("UI: %v", err)
	}
	os.Exit(0)
}

const (
	padding  = 2
	maxWidth = 80
)

func (m *ui) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		case "ctrl+c", "q":
			m.quit = true
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		m.progress.Width = msg.Width - padding*2 - 4
		if m.progress.Width > maxWidth {
			m.progress.Width = maxWidth
		}
		headerHeight := lipgloss.Height(m.headerView())
		footerHeight := lipgloss.Height(m.footerView())
		verticalMarginHeight := headerHeight + footerHeight

		if !m.gotWindowSize {
			// Since this program is using the full size of the viewport we
			// need to wait until we've received the window dimensions before
			// we can initialize the viewport. The initial dimensions come in
			// quickly, though asynchronously, which is why we wait for them
			// here.
			m.viewport = viewport.New(msg.Width, msg.Height-verticalMarginHeight)
			m.viewport.YPosition = headerHeight
			m.viewport.HighPerformanceRendering = useHighPerformanceRenderer
			m.gotWindowSize = true

			// This is only necessary for high performance rendering, which in
			// most cases you won't need.
			//
			// Render the viewport one line below the header.
			m.viewport.YPosition = headerHeight + 1
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = msg.Height - verticalMarginHeight
		}
		return m, viewport.Sync(m.viewport)
	case tickMsg:
		batch := []tea.Cmd{tickCmd(), viewport.Sync(m.viewport)}
		m.showProgress = false
		if m.reportBuf.Load() != nil {
			return m, nil
		}
		if p := m.pct.Load(); p != nil {
			m.showProgress = true
			batch = append(batch, m.progress.SetPercent(*p))
		} else if start := m.start.Load(); start != nil {
			m.showProgress = true
			end := m.end.Load()
			now := time.Now()
			if now.Before(*start) {
				batch = append(batch, m.progress.SetPercent(0))
			} else if now.After(*end) {
				batch = append(batch, m.progress.SetPercent(1))
			} else {
				a, b := start.UnixNano(), end.UnixNano()
				pct := float64(now.UnixNano()-a) / float64(b-a)
				batch = append(batch, m.progress.SetPercent(pct))
				m.progress.SetPercent(pct)
			}
		}

		return m, tea.Batch(batch...)

	// FrameMsg is sent when the progress bar wants to animate itself
	case progress.FrameMsg:
		progressModel, cmd := m.progress.Update(msg)
		m.progress = progressModel.(progress.Model)
		return m, cmd
	}
	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (u *ui) View() string {
	if u.pause.Load() {
		return ""
	}
	if rep := u.reportBuf.Load(); rep != nil {
		if u.quit {
			return rep.String()
		}
		return fmt.Sprintf("%s\n%s\n%s", u.headerView(), u.viewport.View(), u.footerView())
	}

	pad := strings.Repeat(" ", padding)
	res := "\nWarp S3 Benchmark Tool by MinIO\n"
	if ph := u.phase.Load(); ph != nil {
		res += "\n" + *ph
		if ph := u.phaseTxt.Load(); ph != nil {
			res += ": " + *ph
		}
		res += "...\n\n"
	}

	if u.showProgress {
		res += pad + u.progress.View() + "\n"
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
			res += fmt.Sprintf("\nReqs: %d, Errs:%d, Bytes: %d\n", resp.Total.TotalRequests, resp.Total.TotalErrors, resp.Total.TotalBytes)
			ops := stringKeysSorted(resp.ByOpType)
			for _, op := range ops {
				tp := resp.ByOpType[op].Throughput
				segs := tp.Segmented
				if segs == nil || len(segs.Segments) == 0 {
					continue
				}
				res += fmt.Sprintf(" -%10s Average: %.0f Obj/s, %s; ", op, tp.ObjectsPS(), tp.BytesPS().String())
				lastOps := segs.Segments[len(segs.Segments)-1]
				res += fmt.Sprintf("Current %.0f Obj/s, %s", lastOps.OPS, bench.Throughput(lastOps.BPS))
				if len(resp.ByOpType[op].Requests) > 0 {
					reqs := resp.ByOpType[op].Requests[len(resp.ByOpType[op].Requests)-1]
					if reqs.Single != nil {
						res += fmt.Sprintf(", %d ms/req", reqs.Single.DurAvgMillis)
						if reqs.Single.FirstByte != nil {
							res += fmt.Sprintf(", ttfb: %vms", reqs.Single.FirstByte.AverageMillis)
						}
					}
					res += ".\n"
				} else {
					res += "\n"
				}
			}
		}
	}
	return res + "\n"
}

func (u *ui) SetSubText(caption string) {
	u.phaseTxt.Store(&caption)
}

func (u *ui) SetPhase(caption string) {
	u.phase.Store(&caption)
	u.phaseTxt.Store(nil)
}

func (u *ui) ShowReport(buf *bytes.Buffer) {
	u.viewport.SetContent(buf.String())
	u.reportBuf.Store(buf)
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

func (m *ui) Pause(b bool) {
	m.pause.Store(b)
}

var (
	titleStyle = func() lipgloss.Style {
		b := lipgloss.RoundedBorder()
		b.Right = "├"
		return lipgloss.NewStyle().BorderStyle(b).Padding(0, 1)
	}()

	infoStyle = func() lipgloss.Style {
		b := lipgloss.RoundedBorder()
		b.Left = "┤"
		return titleStyle.BorderStyle(b)
	}()
)

func (m *ui) headerView() string {
	title := titleStyle.Render("Warp S3 Benchmark Tool by MinIO - Result View")
	line := strings.Repeat("─", max(0, m.viewport.Width-lipgloss.Width(title)))
	return lipgloss.JoinHorizontal(lipgloss.Center, title, line)
}

func (m *ui) footerView() string {
	info := infoStyle.Render(fmt.Sprintf("%3.f%%", m.viewport.ScrollPercent()*100))
	line := strings.Repeat("─", max(0, m.viewport.Width-lipgloss.Width(info)))
	joined := lipgloss.JoinHorizontal(lipgloss.Center, line, info)
	if ph := m.phase.Load(); ph != nil {
		joined += "\n" + *ph
		if ph := m.phaseTxt.Load(); ph != nil {
			joined += ": " + *ph
		}
	}
	return joined
}
