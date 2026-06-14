/*
 * Warp comparison view.
 * Fetches /api/compare and renders before/after tables and bar charts.
 */

// --- Theme (mirrors app.js) ---
function getTheme() { return localStorage.getItem('warp-theme') || 'dark'; }

function setTheme(theme) {
    localStorage.setItem('warp-theme', theme);
    document.body.classList.toggle('light', theme === 'light');
    updateThemeButton(theme);
    if (window._charts) restyleCharts();
}

function updateThemeButton(theme) {
    const dark = document.getElementById('theme-icon-dark');
    const light = document.getElementById('theme-icon-light');
    if (!dark || !light) return;
    dark.style.display = theme === 'dark' ? '' : 'none';
    light.style.display = theme === 'dark' ? 'none' : '';
}

document.getElementById('theme-toggle').addEventListener('click', () => {
    setTheme(getTheme() === 'dark' ? 'light' : 'dark');
});

// Print palette (fixed, so the PDF reads on white regardless of UI theme).
const PDF = {
    accent: [232, 74, 107],   // pink (after)
    before: [154, 160, 170],  // grey (before)
    slate: [35, 41, 51],
    good: [31, 157, 87],
    bad: [210, 59, 48],
    muted: [110, 116, 128],
};

// exportValueLabelPlugin draws each bar's exact value above it (dark, for print).
const exportValueLabelPlugin = {
    id: 'exportValueLabels',
    afterDatasetsDraw(chart) {
        const ds = chart.data.datasets[0];
        if (!ds || !ds._labels) return;
        const meta = chart.getDatasetMeta(0);
        const { ctx } = chart;
        ctx.save();
        ctx.font = "600 16px 'General Sans', Arial, sans-serif";
        ctx.fillStyle = '#222';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'bottom';
        meta.data.forEach((bar, i) => {
            const l = ds._labels[i];
            if (l != null) ctx.fillText(l, bar.x, bar.y - 5);
        });
        ctx.restore();
    },
};

// whiteBgPlugin fills the canvas white so JPEG export (no alpha) isn't black.
const whiteBgPlugin = {
    id: 'whiteBg',
    beforeDraw(chart) {
        const { ctx } = chart;
        ctx.save();
        ctx.globalCompositeOperation = 'destination-over';
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, chart.width, chart.height);
        ctx.restore();
    },
};

// renderExportChart renders one metric chart offscreen at 2x DPI with fixed
// print colors and returns a JPEG data URL. JPEG (vs PNG) and 2x DPI keep the
// charts crisp while keeping the PDF file size small.
function renderExportChart(spec) {
    const holder = document.createElement('div');
    holder.style.cssText = 'position:fixed;left:-99999px;top:0;width:460px;height:300px;';
    const canvas = document.createElement('canvas');
    holder.appendChild(canvas);
    document.body.appendChild(holder);
    const chart = new Chart(canvas, {
        type: 'bar',
        data: {
            labels: [LBL.before, LBL.after],
            datasets: [{
                data: [spec.beforeVal, spec.afterVal],
                backgroundColor: ['#9aa0aa', '#e84a6b'],
                borderRadius: 4,
                _labels: [spec.beforeStr, spec.afterStr],
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            devicePixelRatio: 2,
            layout: { padding: { top: 26 } },
            plugins: { legend: { display: false }, tooltip: { enabled: false } },
            scales: {
                x: { grid: { display: false }, ticks: { display: false } },
                y: { beginAtZero: true, grace: '18%', grid: { color: '#e3e6ea' }, ticks: { color: '#555', font: { size: 15 }, maxTicksLimit: 4 } },
            },
        },
        plugins: [whiteBgPlugin, exportValueLabelPlugin],
    });
    const url = chart.toBase64Image('image/jpeg', 0.9);
    chart.destroy();
    holder.remove();
    return url;
}

// Export the comparison to a polished, report-grade PDF (jsPDF + autotable) that
// mirrors the UI's styling. Charts are rendered offscreen at high DPI; no
// html2canvas (which fails inconsistently across browsers).
async function exportPdf() {
    const btn = document.getElementById('export-pdf');
    if (!window.jspdf || !window.jspdf.jsPDF) {
        alert('PDF library failed to load (no network access to the CDN?).');
        return;
    }
    const data = window._cmpData;
    if (!data || !data.ops || !data.ops.length) { alert('Nothing to export yet.'); return; }

    if (btn) { btn.disabled = true; btn.textContent = 'Generating…'; }
    await new Promise((r) => setTimeout(r, 30)); // let the button repaint

    try {
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF({ unit: 'mm', format: 'a4', orientation: 'portrait' });
        const pageW = doc.internal.pageSize.getWidth();
        const pageH = doc.internal.pageSize.getHeight();
        const margin = 12;
        const contentW = pageW - 2 * margin;
        let y = margin;
        const ensure = (h) => { if (y + h > pageH - margin) { doc.addPage(); y = margin; } };

        // Legend: grey "before" + pink "after" swatches with run labels.
        const drawLegend = (x, yy) => {
            doc.setFontSize(8);
            doc.setFillColor(...PDF.before); doc.rect(x, yy - 2.6, 3, 3, 'F');
            doc.setTextColor(...PDF.muted); doc.text(LBL.before, x + 4, yy);
            const x2 = x + 4 + doc.getTextWidth(LBL.before) + 6;
            doc.setFillColor(...PDF.accent); doc.rect(x2, yy - 2.6, 3, 3, 'F');
            doc.text(LBL.after, x2 + 4, yy);
            doc.setTextColor(0);
        };

        // Title band.
        doc.setFillColor(...PDF.accent); doc.rect(0, 0, pageW, 3, 'F');
        doc.setFontSize(18); doc.setTextColor(...PDF.slate);
        doc.text('Warp Benchmark Comparison', margin, y + 4);
        y += 10;
        doc.setFontSize(9); doc.setTextColor(...PDF.muted);
        doc.text(`Before:  ${LBL.before}`, margin, y); y += 4.5;
        doc.text(`After:   ${LBL.after}`, margin, y); y += 4.5;
        doc.text(`Generated ${new Date().toLocaleString()}`, margin, y); y += 7;
        doc.setTextColor(0);

        data.ops.forEach((op, i) => {
            ensure(16);
            // Operation header bar (slate, like the UI's op header).
            doc.setFillColor(...PDF.slate);
            doc.roundedRect(margin, y, contentW, 8, 1.5, 1.5, 'F');
            doc.setTextColor(255); doc.setFontSize(12);
            doc.text(op.op, margin + 3, y + 5.6);
            doc.setTextColor(0);
            y += 12;

            if (op.error) {
                doc.setFontSize(9); doc.setTextColor(...PDF.bad);
                doc.text('Cannot compare: ' + op.error, margin, y);
                doc.setTextColor(0); y += 8;
                return;
            }

            // Chart sections: per-metric mini bar charts (high-DPI), 3 per row.
            const chartSections = [
                [`chart-${i}-tput`, 'Throughput (MiB/s)'],
                [`chart-${i}-req`, 'Request duration (ms)'],
                [`chart-${i}-ttfb`, 'Time to first byte (ms)'],
            ];
            const perRow = 3, gap = 5;
            const cw = (contentW - (perRow - 1) * gap) / perRow;
            const imgH = cw * 0.66;
            const cellH = 5 + imgH;
            for (const [prefix, secTitle] of chartSections) {
                const specs = chartSpecs.filter((s) => s.id.startsWith(prefix + '-'));
                if (!specs.length) continue;
                ensure(10);
                doc.setFontSize(10); doc.setTextColor(...PDF.slate);
                doc.text(secTitle, margin, y);
                drawLegend(margin + doc.getTextWidth(secTitle) + 8, y);
                doc.setTextColor(0);
                y += 4;
                for (let k = 0; k < specs.length; k++) {
                    const col = k % perRow;
                    if (col === 0) ensure(cellH + 2);
                    const x = margin + col * (cw + gap);
                    doc.setFontSize(8); doc.setTextColor(...PDF.muted);
                    doc.text(specs[k].name || '', x + cw / 2, y + 3.2, { align: 'center', maxWidth: cw });
                    doc.setTextColor(0);
                    try {
                        doc.addImage(renderExportChart(specs[k]), 'JPEG', x, y + 5, cw, imgH, undefined, 'FAST');
                    } catch (e) { /* skip a chart that fails to render */ }
                    if (col === perRow - 1 || k === specs.length - 1) y += cellH + 2;
                }
                y += 2;
            }

            // Tables, with green/red "Change" cells matching the UI.
            const sections = [
                ['Overview', op.info], ['Throughput', op.throughput],
                ['Request duration', op.requests], ['Time to first byte', op.ttfb],
            ];
            sections.forEach(([title, rows]) => {
                if (!rows || !rows.length) return;
                ensure(16);
                doc.setFontSize(10); doc.setTextColor(...PDF.slate);
                doc.text(title, margin, y); doc.setTextColor(0);
                y += 2;
                doc.autoTable({
                    startY: y,
                    margin: { left: margin, right: margin },
                    head: [['Metric', LBL.before, LBL.after, 'Change']],
                    body: rows.map((r) => [r.name, r.before, r.after, r.change || '—']),
                    styles: { fontSize: 8, cellPadding: 1.6 },
                    headStyles: { fillColor: PDF.slate },
                    alternateRowStyles: { fillColor: [245, 247, 249] },
                    columnStyles: { 0: { fontStyle: 'bold' }, 3: { halign: 'right' } },
                    didParseCell: (d) => {
                        if (d.section === 'body' && d.column.index === 3) {
                            const imp = rows[d.row.index].improved;
                            if (imp === true) d.cell.styles.textColor = PDF.good;
                            else if (imp === false) d.cell.styles.textColor = PDF.bad;
                        }
                    },
                });
                y = doc.lastAutoTable.finalY + 5;
            });
            y += 3;
        });

        const stamp = new Date().toISOString().slice(0, 16).replace(/[:T]/g, '-');
        doc.save(`warp-compare-${stamp}.pdf`);
    } catch (err) {
        console.error('PDF export failed:', err);
        alert('PDF export failed: ' + (err && err.message ? err.message : err));
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = 'Export PDF'; }
    }
}

document.getElementById('export-pdf')?.addEventListener('click', exportPdf);

document.addEventListener('keydown', (e) => {
    if (e.target.matches('input, textarea')) return;
    if (e.key === 't' || e.key === 'T') {
        setTheme(getTheme() === 'dark' ? 'light' : 'dark');
    }
});

setTheme(getTheme());

// --- Colors ---
const C_BEFORE = '#9aa0aa';
const C_AFTER = '#e84a6b';
function gridColor() { return getTheme() === 'light' ? 'rgba(0,0,0,0.08)' : 'rgba(255,255,255,0.08)'; }
function tickColor() { return getTheme() === 'light' ? '#52525b' : '#a1a1aa'; }

// --- Helpers ---
function escapeHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
}

// Run labels derived from the input filenames, set in load().
const LBL = { before: 'Before', after: 'After' };

// shortName turns a path into a readable run label: basename without the
// .csv.zst / .json.zst / .zst suffix.
function shortName(path) {
    let n = String(path ?? '').split(/[\\/]/).pop();
    n = n.replace(/\.(csv|json)?\.?zst$/i, '');
    return n || path || '?';
}

function changeCell(row) {
    if (!row.change) return '<span class="cmp-change neutral">—</span>';
    let cls = 'neutral';
    if (row.improved === true) cls = 'good';
    else if (row.improved === false) cls = 'bad';
    return `<span class="cmp-change ${cls}">${escapeHtml(row.change)}</span>`;
}

function renderTable(title, rows) {
    if (!rows || rows.length === 0) return '';
    const body = rows.map((r) => `
        <tr>
            <td class="cmp-metric">${escapeHtml(r.name)}</td>
            <td class="cmp-num">${escapeHtml(r.before)}</td>
            <td class="cmp-num">${escapeHtml(r.after)}</td>
            <td class="cmp-num">${changeCell(r)}</td>
        </tr>`).join('');
    return `
        <div class="cmp-table-block">
            <h4 class="cmp-table-title">${escapeHtml(title)}</h4>
            <div class="table-wrap">
                <table class="cmp-table">
                    <thead>
                        <tr><th>Metric</th><th class="cmp-run-col">${escapeHtml(LBL.before)}</th><th class="cmp-run-col">${escapeHtml(LBL.after)}</th><th>Change</th></tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        </div>`;
}

// chartSpecs collects charts to instantiate after the DOM is in place.
let chartSpecs = [];

// metricSection renders a titled group with one small Before/After chart PER
// metric, so a large metric (e.g. Worst) never squashes the others. Each bar is
// labelled with its exact value for reports/presentations.
function metricSection(prefix, title, unit, rows, labelFn) {
    if (!rows || rows.length === 0) return '';
    const cards = rows.map((r, j) => {
        const id = `${prefix}-${j}`;
        const metric = labelFn ? labelFn(r.name) : r.name;
        chartSpecs.push({
            id,
            name: metric,
            unit,
            beforeVal: r.before_num,
            afterVal: r.after_num,
            beforeStr: r.before,
            afterStr: r.after,
            change: r.change || '',
            improved: r.improved,
        });
        return `
            <div class="cmp-metric-card">
                <div class="cmp-metric-title">${escapeHtml(metric)}</div>
                <div class="cmp-mini-wrap"><canvas id="${id}"></canvas></div>
            </div>`;
    }).join('');
    return `
        <div class="cmp-section">
            <div class="cmp-section-head">
                <h4 class="cmp-table-title">${escapeHtml(title)}${unit ? ` <span class="cmp-unit">(${escapeHtml(unit)})</span>` : ''}</h4>
                <div class="cmp-legend">
                    <span class="cmp-leg"><span class="cmp-swatch before"></span>${escapeHtml(LBL.before)}</span>
                    <span class="cmp-leg"><span class="cmp-swatch after"></span>${escapeHtml(LBL.after)}</span>
                </div>
            </div>
            <div class="cmp-metric-grid">${cards}</div>
        </div>`;
}

function renderOp(op, i) {
    if (op.error) {
        return `
        <section class="card">
            <header class="card-header">
                <div><h2 class="card-title">${escapeHtml(op.op)}</h2></div>
                <span class="op-pill">${escapeHtml(op.op)}</span>
            </header>
            <div class="card-content"><div class="empty">Cannot compare: ${escapeHtml(op.error)}</div></div>
        </section>`;
    }

    const tputRows = (op.throughput || []).filter((r) => r.unit === 'MiB/s');
    const reqRows = (op.requests || []).filter((r) => r.unit === 'ms');
    const ttfbRows = (op.ttfb || []).filter((r) => r.unit === 'ms');

    const charts = `
        ${metricSection(`chart-${i}-tput`, 'Throughput', 'MiB/s', tputRows, (n) => n.replace(' throughput', ''))}
        ${metricSection(`chart-${i}-req`, 'Request duration', 'ms', reqRows)}
        ${metricSection(`chart-${i}-ttfb`, 'Time to first byte', 'ms', ttfbRows)}`;

    return `
    <section class="card">
        <header class="card-header">
            <div>
                <h2 class="card-title">${escapeHtml(op.op)}</h2>
                <p class="card-description">Before vs after for ${escapeHtml(op.op)} operations</p>
            </div>
            <span class="op-pill">${escapeHtml(op.op)}</span>
        </header>
        <div class="card-content">
            ${charts}
            ${renderTable('Overview', op.info)}
            ${renderTable('Throughput', op.throughput)}
            ${renderTable('Request duration', op.requests)}
            ${renderTable('Time to first byte', op.ttfb)}
        </div>
    </section>`;
}

// valueLabelPlugin prints each bar's exact value above it (for reports/slides).
// The label text comes from dataset._labels (already unit-formatted strings).
const valueLabelPlugin = {
    id: 'valueLabels',
    afterDatasetsDraw(chart) {
        const ds = chart.data.datasets[0];
        if (!ds || !ds._labels) return;
        const meta = chart.getDatasetMeta(0);
        const { ctx } = chart;
        ctx.save();
        ctx.font = "600 11px 'General Sans', sans-serif";
        ctx.fillStyle = tickColor();
        ctx.textAlign = 'center';
        ctx.textBaseline = 'bottom';
        meta.data.forEach((bar, i) => {
            const label = ds._labels[i];
            if (label != null) ctx.fillText(label, bar.x, bar.y - 4);
        });
        ctx.restore();
    },
};

function buildCharts() {
    window._charts = [];
    for (const spec of chartSpecs) {
        const el = document.getElementById(spec.id);
        if (!el) continue;
        // One small chart per metric: two same-metric bars (before/after), so
        // each chart auto-scales to its own values and stays readable.
        const chart = new Chart(el, {
            type: 'bar',
            data: {
                labels: [LBL.before, LBL.after],
                datasets: [{
                    data: [spec.beforeVal, spec.afterVal],
                    backgroundColor: [C_BEFORE, C_AFTER],
                    borderRadius: 4,
                    _labels: [spec.beforeStr, spec.afterStr],
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { top: 18 } }, // room for the value labels
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (c) => c.dataset._labels[c.dataIndex] } },
                },
                scales: {
                    x: { grid: { display: false }, ticks: { display: false } },
                    y: { beginAtZero: true, grace: '15%', grid: { color: gridColor() }, ticks: { color: tickColor(), maxTicksLimit: 4 } },
                },
            },
            plugins: [valueLabelPlugin],
        });
        window._charts.push(chart);
    }
}

function restyleCharts() {
    for (const chart of window._charts) {
        const s = chart.options.scales;
        if (s.y?.ticks) s.y.ticks.color = tickColor();
        if (s.y?.grid) s.y.grid.color = gridColor();
        chart.update('none');
    }
}

function renderFiles(data) {
    const el = document.getElementById('cmp-files');
    el.innerHTML = `
        <div class="cmp-file"><span class="cmp-swatch before"></span><span class="cmp-file-name">${escapeHtml(LBL.before)}</span><code>${escapeHtml(data.before_file)}</code></div>
        <div class="cmp-file"><span class="cmp-swatch after"></span><span class="cmp-file-name">${escapeHtml(LBL.after)}</span><code>${escapeHtml(data.after_file)}</code></div>`;
}

async function load() {
    const opsEl = document.getElementById('cmp-ops');
    try {
        // Relative path + query so this works both standalone and mounted
        // under a run-scoped prefix (e.g. /dash/compare.html?before=A&after=B).
        const resp = await fetch('api/compare' + location.search);
        if (!resp.ok) throw new Error(`server returned ${resp.status}`);
        const data = await resp.json();
        window._cmpData = data; // used by the PDF export
        LBL.before = shortName(data.before_file);
        LBL.after = shortName(data.after_file);
        renderFiles(data);
        if (!data.ops || data.ops.length === 0) {
            opsEl.innerHTML = '<section class="card"><div class="card-content"><div class="empty">No comparable operations found.</div></div></section>';
            return;
        }
        chartSpecs = [];
        opsEl.innerHTML = data.ops.map(renderOp).join('');
        buildCharts();
    } catch (err) {
        opsEl.innerHTML = `<section class="card"><div class="card-content"><div class="empty">Failed to load comparison: ${escapeHtml(err.message)}</div></div></section>`;
    }
}

load();
