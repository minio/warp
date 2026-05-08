// Warp Web UI — refined performance dashboard

const palette = {
    fg:      '#f1f3f5',
    fgDim:   '#9298a0',
    fgFaint: '#5e636b',
    accent:  '#ea4869',
    accent2: '#ff6e87',
    ok:      '#27c08c',
    warn:    '#f5b323',
    danger:  '#dd4949',
    info:    '#5b9aef',
    cyan:    '#22d3ee',
    purple:  '#a78bfa',
    pink:    '#f472b6',
};

const paletteLight = {
    fg:      '#181d27',
    fgDim:   '#666c78',
    fgFaint: '#9aa0aa',
    accent:  '#c43056',
    accent2: '#dc4768',
    ok:      '#1a7a47',
    warn:    '#a26710',
    danger:  '#c5292c',
    info:    '#1f6cce',
    cyan:    '#0e7490',
    purple:  '#6d28d9',
    pink:    '#be185d',
};

const opColors = {
    GET:       'ok',
    PUT:       'info',
    DELETE:    'accent',
    LIST:      'purple',
    STAT:      'cyan',
    MULTIPART: 'pink',
};

let p = palette;
function pc(name) { return p[name] || p.accent; }

// ----- Theme -----
function getTheme() { return localStorage.getItem('warp-theme') || 'dark'; }

function setTheme(theme) {
    localStorage.setItem('warp-theme', theme);
    document.body.classList.add('theme-fading');
    document.body.classList.toggle('light', theme === 'light');
    p = theme === 'light' ? paletteLight : palette;
    updateThemeButton(theme);
    updateChartDefaults();
    destroyAllCharts();
    if (data) renderDashboard();
    setTimeout(() => document.body.classList.remove('theme-fading'), 280);
}

function updateThemeButton(theme) {
    const dark = document.getElementById('theme-icon-dark');
    const light = document.getElementById('theme-icon-light');
    if (theme === 'dark') {
        dark.style.display = 'block';
        light.style.display = 'none';
    } else {
        dark.style.display = 'none';
        light.style.display = 'block';
    }
}

let chartGridColor = 'rgba(255,255,255,0.05)';

function updateChartDefaults() {
    const isLight = document.body.classList.contains('light');
    Chart.defaults.color = p.fgFaint;
    Chart.defaults.borderColor = isLight ? 'rgba(20,20,30,0.07)' : 'rgba(255,255,255,0.06)';
    chartGridColor = isLight ? 'rgba(20,20,30,0.06)' : 'rgba(255,255,255,0.05)';

    Chart.defaults.font.family = "'General Sans', 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
    Chart.defaults.font.size = 11;
    Chart.defaults.font.weight = '500';

    Chart.defaults.plugins.tooltip.backgroundColor = isLight ? '#14151a' : '#0a0c10';
    Chart.defaults.plugins.tooltip.borderColor = isLight ? 'rgba(20,20,30,0.12)' : 'rgba(255,255,255,0.10)';
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.plugins.tooltip.cornerRadius = 8;
    Chart.defaults.plugins.tooltip.titleColor = isLight ? '#faf8f4' : '#e8ecf3';
    Chart.defaults.plugins.tooltip.bodyColor = isLight ? '#d8d6d0' : '#b6bdcb';
    Chart.defaults.plugins.tooltip.padding = { top: 10, bottom: 10, left: 12, right: 12 };
    Chart.defaults.plugins.tooltip.titleFont = { size: 11, weight: '600', family: "'General Sans', sans-serif" };
    Chart.defaults.plugins.tooltip.bodyFont = { size: 11.5, family: "'JetBrains Mono', monospace" };
    Chart.defaults.plugins.tooltip.titleMarginBottom = 6;
    Chart.defaults.plugins.tooltip.boxPadding = 4;
    Chart.defaults.plugins.tooltip.usePointStyle = true;

    Chart.defaults.elements.line.borderWidth = 1.75;
    Chart.defaults.elements.line.tension = 0.35;
    Chart.defaults.elements.point.radius = 0;
    Chart.defaults.elements.point.hoverRadius = 4;
    Chart.defaults.elements.point.hoverBorderWidth = 2;
}

const initialTheme = getTheme();
document.body.classList.toggle('light', initialTheme === 'light');
p = initialTheme === 'light' ? paletteLight : palette;
updateChartDefaults();
updateThemeButton(initialTheme);

document.getElementById('theme-toggle').addEventListener('click', () => {
    setTheme(getTheme() === 'dark' ? 'light' : 'dark');
});

// ----- Toast -----
function showToast(message, opts = {}) {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const toast = document.createElement('div');
    toast.className = 'toast';
    if (opts.icon !== false) {
        const iconWrap = document.createElement('span');
        iconWrap.className = 'toast-icon';
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', '16');
        svg.setAttribute('height', '16');
        svg.setAttribute('viewBox', '0 0 24 24');
        svg.setAttribute('fill', 'none');
        svg.setAttribute('stroke', 'currentColor');
        svg.setAttribute('stroke-width', '2.5');
        svg.setAttribute('stroke-linecap', 'round');
        svg.setAttribute('stroke-linejoin', 'round');
        const poly = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
        poly.setAttribute('points', '20 6 9 17 4 12');
        svg.appendChild(poly);
        iconWrap.appendChild(svg);
        toast.appendChild(iconWrap);
    }
    const text = document.createElement('span');
    text.textContent = message;
    toast.appendChild(text);
    container.appendChild(toast);
    const ttl = opts.ttl ?? 2000;
    setTimeout(() => {
        toast.classList.add('leaving');
        setTimeout(() => toast.remove(), 220);
    }, ttl);
}

// ----- Copy command -----
const copyBtn = document.getElementById('cmd-copy');
const copyIcon = document.getElementById('copy-icon');
const copyIconDone = document.getElementById('copy-icon-done');
let copyResetTimer = null;
copyBtn?.addEventListener('click', async () => {
    const cmd = data?.commandline || document.getElementById('cmd-line-code')?.textContent || '';
    if (!cmd) return;
    try {
        await navigator.clipboard.writeText(cmd);
        copyBtn.classList.add('copied');
        copyIcon.style.display = 'none';
        copyIconDone.style.display = 'block';
        if (copyResetTimer) clearTimeout(copyResetTimer);
        copyResetTimer = setTimeout(() => {
            copyBtn.classList.remove('copied');
            copyIcon.style.display = 'block';
            copyIconDone.style.display = 'none';
        }, 1500);
        showToast('Command copied to clipboard');
    } catch (err) {
        console.error('clipboard write failed', err);
        showToast('Copy failed', { icon: false });
    }
});

// ----- Number animation -----
function parseValueWithSuffix(v) {
    const s = String(v);
    const m = s.match(/^(-?[\d.]+)(.*)$/);
    if (!m) return { num: 0, suffix: s };
    return { num: parseFloat(m[1]), suffix: m[2] };
}

function animateNum(el, target, formatter, duration = 750) {
    if (!el) return;
    const startTime = performance.now();
    const ease = (t) => 1 - Math.pow(1 - t, 3);
    let raf;
    function tick(now) {
        const t = Math.min(1, (now - startTime) / duration);
        const v = target * ease(t);
        el.textContent = formatter(v);
        if (t < 1) raf = requestAnimationFrame(tick);
        else el.textContent = formatter(target);
    }
    raf = requestAnimationFrame(tick);
}

// ----- Sparkline -----
function drawSparkline(canvas, values, opts = {}) {
    if (!canvas || !values || values.length < 2) return;
    const dpr = window.devicePixelRatio || 1;
    const W = canvas.offsetWidth;
    const H = canvas.offsetHeight;
    if (W <= 0 || H <= 0) return;
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    const ctx = canvas.getContext('2d');
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, W, H);

    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = (max - min) || 1;
    const stepX = W / Math.max(1, values.length - 1);
    const padTop = 6, padBot = 2;
    const yFor = (v) => H - padBot - ((v - min) / range) * (H - padTop - padBot);
    const color = opts.color || p.accent;

    if (opts.fill !== false) {
        const grad = ctx.createLinearGradient(0, 0, 0, H);
        grad.addColorStop(0, hexToRgba(color, 0.20));
        grad.addColorStop(1, hexToRgba(color, 0));
        ctx.fillStyle = grad;
        ctx.beginPath();
        ctx.moveTo(0, H);
        ctx.lineTo(0, yFor(values[0]));
        for (let i = 1; i < values.length; i++) ctx.lineTo(i * stepX, yFor(values[i]));
        ctx.lineTo(W, H);
        ctx.closePath();
        ctx.fill();
    }

    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.lineCap = 'round';
    ctx.lineJoin = 'round';
    ctx.beginPath();
    for (let i = 0; i < values.length; i++) {
        const x = i * stepX;
        const y = yFor(values[i]);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    }
    ctx.stroke();

    const lastX = (values.length - 1) * stepX;
    const lastY = yFor(values[values.length - 1]);
    ctx.fillStyle = color;
    ctx.beginPath();
    ctx.arc(lastX - 1, lastY, 2.5, 0, Math.PI * 2);
    ctx.fill();
}

function getSparklineSeries(realtime) {
    const tp = realtime?.total?.throughput;
    const segs = tp?.segmented?.segments;
    if (!segs || segs.length === 0) return null;
    const sorted = [...segs].sort((a, b) => new Date(a.start) - new Date(b.start));
    const segDurMs = tp.segmented.segment_duration_millis || 5000;
    const segDurS = segDurMs / 1000;
    const peak = sorted.map(s => s.bytes_per_sec || s.obj_per_sec || 0);
    let opsRunning = 0, bytesRunning = 0;
    const opsCum = [], dataCum = [];
    for (const s of sorted) {
        opsRunning += (s.obj_per_sec || 0) * segDurS;
        bytesRunning += (s.bytes_per_sec || 0) * segDurS;
        opsCum.push(opsRunning);
        dataCum.push(bytesRunning);
    }
    return { peak, opsCum, dataCum };
}

// ----- Formatters -----
function formatBytes(bytes) {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatThroughput(bps, ops) {
    if ((!bps || bps === 0) && ops > 0) {
        return { value: parseFloat(ops.toFixed(1)), unit: 'obj/s' };
    }
    if (!bps || bps === 0) return { value: '0', unit: 'B/s' };
    const k = 1024;
    const sizes = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s', 'TiB/s'];
    const i = Math.floor(Math.log(bps) / Math.log(k));
    return {
        value: parseFloat((bps / Math.pow(k, i)).toFixed(1)),
        unit: sizes[i],
    };
}

function formatDuration(ms) {
    if (!ms) return '0ms';
    if (ms < 1000) return ms + 'ms';
    const s = ms / 1000;
    if (s < 60) return s.toFixed(1) + 's';
    const m = Math.floor(s / 60);
    const rs = Math.floor(s % 60);
    if (m < 60) return m + 'm ' + rs + 's';
    const h = Math.floor(m / 60);
    const rm = m % 60;
    return h + 'h ' + rm + 'm';
}

function formatNumber(n) {
    if (!n) return '0';
    if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
    if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return n.toFixed(0);
}

function formatLatency(ms) {
    if (ms == null) return '—';
    if (ms < 1) return ms.toFixed(2) + 'ms';
    if (ms < 1000) return ms.toFixed(0) + 'ms';
    return (ms / 1000).toFixed(2) + 's';
}

function formatTime(d) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function getOpColor(opType) {
    return pc(opColors[opType.toUpperCase()] || 'accent');
}

function hasTtfb(opType, opData) {
    const uploadOps = ['PUT', 'POST'];
    if (uploadOps.includes(opType.toUpperCase())) return false;
    if (opData !== undefined) return getTtfbStats(opData) != null;
    return true;
}

function sampleData(arr, maxPoints = 200) {
    if (!arr || arr.length <= maxPoints) return arr;
    const step = arr.length / maxPoints;
    const sampled = [];
    for (let i = 0; i < arr.length; i += step) sampled.push(arr[Math.floor(i)]);
    if (sampled[sampled.length - 1] !== arr[arr.length - 1]) sampled.push(arr[arr.length - 1]);
    return sampled;
}

// ----- Tabs -----
document.querySelectorAll('.seg-tab').forEach(tab => {
    tab.addEventListener('click', () => activateTab(tab.dataset.tab));
});

function activateTab(name) {
    document.querySelectorAll('.seg-tab').forEach(t => {
        const isActive = t.dataset.tab === name;
        t.classList.toggle('active', isActive);
        t.setAttribute('aria-selected', isActive ? 'true' : 'false');
        t.setAttribute('tabindex', isActive ? '0' : '-1');
    });
    document.querySelectorAll('.tab-content').forEach(c => {
        const isActive = c.id === 'tab-' + name;
        c.classList.toggle('active', isActive);
        if (isActive) c.removeAttribute('hidden');
        else c.setAttribute('hidden', '');
    });
}

// ----- Keyboard shortcuts -----
document.addEventListener('keydown', (e) => {
    if (e.target.matches('input, textarea, [contenteditable]')) return;
    const k = e.key.toLowerCase();
    if (k === 't') { e.preventDefault(); setTheme(getTheme() === 'dark' ? 'light' : 'dark'); }
    if (k === 'r') { e.preventDefault(); loadData(); }
    if (k === '1') { e.preventDefault(); activateTab('hosts'); }
    if (k === '2') { e.preventDefault(); activateTab('clients'); }
});

// ----- App -----
let data = null;
let charts = {};
let pollTimer = null;
let firstRender = true;
const POLL_INTERVAL_MS = 2000;

async function loadData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error('Failed to load data');
        const apiResponse = await response.json();
        data = apiResponse.data || apiResponse;
        const autoUpdate = !!apiResponse.auto_update;
        updateLiveStatus(autoUpdate);
        renderDashboard();
        schedulePoll(autoUpdate);
    } catch (error) {
        console.error('Error loading data:', error);
        schedulePoll(false);
        const el = document.querySelector('main');
        if (el) {
            el.replaceChildren();
            const empty = document.createElement('div');
            empty.className = 'empty';
            empty.textContent = 'Failed to load benchmark data — ' + error.message;
            el.appendChild(empty);
        }
    }
}

function schedulePoll(autoUpdate) {
    if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; }
    if (autoUpdate) pollTimer = setTimeout(loadData, POLL_INTERVAL_MS);
}

function updateLiveStatus(autoUpdate) {
    const pill = document.getElementById('live-pill');
    const label = document.getElementById('live-label');
    const tick = document.getElementById('last-tick');
    pill.classList.remove('live', 'final');
    if (autoUpdate) {
        pill.classList.add('live');
        label.textContent = 'Live';
    } else if (data && (data.final || (data.total && data.total.total_requests > 0))) {
        pill.classList.add('final');
        label.textContent = 'Final';
    } else {
        label.textContent = 'Ready';
    }
    tick.textContent = 'Updated ' + formatTime(new Date());
}

function destroyAllCharts() {
    for (const id in Chart.instances) {
        if (Chart.instances[id]) Chart.instances[id].destroy();
    }
    charts = {};
}

function renderDashboard() {
    destroyAllCharts();
    renderConfig();
    renderHeroStats();
    renderOperationCards();
    renderHostsTable();
    renderClientsTable();
    firstRender = false;
}

// ----- Config / Execution -----
function parseCommandLine(cmdline) {
    if (!cmdline) return { op: 'benchmark', params: {} };
    const parts = cmdline.split(/\s+/);
    let op = 'benchmark';
    const params = {};
    for (let i = 0; i < parts.length; i++) {
        const part = parts[i];
        if (part === 'warp') continue;
        if (!part.startsWith('-') && !op.match(/get|put|delete|list|stat|mixed|multipart|retention|versioned|select|fanout|snowball|zip/i)) {
            op = part;
            continue;
        }
        if (part.startsWith('--')) {
            const [key, ...valueParts] = part.slice(2).split('=');
            params[key] = valueParts.join('=') || 'true';
        }
    }
    return { op, params };
}

function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function highlightCmd(cmdline) {
    if (!cmdline) return 'warp';
    const parts = cmdline.trim().split(/\s+(?=--)/);
    const cont = '<span class="cont"> \\</span>\n    ';
    const fmtFlag = (s) => {
        const m = s.match(/^(--[\w.-]+)(=)?([\s\S]*)?$/);
        if (!m) return escapeHtml(s);
        const [, flag, eq, val] = m;
        if (!eq) return `<span class="arg-flag">${escapeHtml(flag)}</span>`;
        return `<span class="arg-flag">${escapeHtml(flag)}</span>=<span class="arg-val">${escapeHtml(val || '')}</span>`;
    };
    if (parts.length <= 1) return escapeHtml(parts[0] || cmdline);
    const head = escapeHtml(parts[0]);
    const flags = parts.slice(1).map(fmtFlag);
    return head + cont + flags.join(cont);
}

function renderConfig() {
    const cmdline = data.commandline || '';
    const { op, params } = parseCommandLine(cmdline);
    const total = data.total || {};
    const hosts = total.hosts || [];
    const clients = total.clients || [];

    document.getElementById('benchmark-type').textContent = `S3 Benchmark · ${op}`;
    document.getElementById('config-op').textContent = op;
    document.getElementById('cmd-line-code').innerHTML = highlightCmd(cmdline || 'warp');

    const cells = [
        { label: 'Hosts',       value: hosts.length || (params.host ? params.host.split(',').length : '—') },
        { label: 'Bucket',      value: params.bucket || '—' },
        { label: 'Clients',     value: clients.length || params['warp-client'] || '—' },
        { label: 'Concurrent',  value: total.concurrency || params.concurrent || '—' },
        { label: 'Object size', value: params['obj.size'] || '—' },
        { label: 'Duration',    value: params.duration || '—' },
        { label: 'Multipart',   value: params['disable-multipart'] === 'true' ? 'Off' : 'On' },
        { label: 'Auto-term',   value: params.autoterm === 'true' ? 'On' : 'Off' },
    ];

    const hasErrors = (total.total_errors || 0) > 0;
    if (hasErrors) cells.push({ label: 'Errors', value: total.total_errors, error: true });

    document.getElementById('config-grid').innerHTML = cells.map(c => `
        <div class="config-cell${c.error ? ' error' : ''}">
            <span class="label">${c.label}</span>
            <span class="value">${c.value}</span>
        </div>
    `).join('');
}

// ----- Hero -----
function renderHeroStats() {
    const total = data.total || {};
    const tp = total.throughput;

    let bps = 0, opsPerSec = 0;
    if (tp && tp.measure_duration_millis > 0) {
        bps = (tp.bytes * 1000) / tp.measure_duration_millis;
        opsPerSec = (tp.ops * 1000) / tp.measure_duration_millis;
    }
    const tput = formatThroughput(bps, opsPerSec);
    const bytesParts = formatBytes(total.total_bytes || 0).split(' ');

    const durMs = tp?.measure_duration_millis || 0;
    let durVal = '0', durUnit = 's';
    if (durMs > 0 && durMs < 1000) { durVal = String(durMs); durUnit = 'ms'; }
    else if (durMs < 60_000) { durVal = (durMs / 1000).toFixed(1); durUnit = 's'; }
    else if (durMs < 3_600_000) { durVal = (durMs / 60_000).toFixed(1); durUnit = 'min'; }
    else { durVal = (durMs / 3_600_000).toFixed(2); durUnit = 'hr'; }

    const series = getSparklineSeries(data);

    const stats = [
        { key: 'peak',  label: 'Peak throughput', value: String(tput.value), unit: tput.unit, peak: true,
          sparkline: series?.peak, sparkColor: p.accent },
        { key: 'ops',   label: 'Operations',       value: formatNumber(total.total_requests || 0), unit: 'reqs',
          sparkline: series?.opsCum, sparkColor: p.info },
        { key: 'data',  label: 'Data transferred', value: bytesParts[0] || '0', unit: bytesParts[1] || 'B',
          sparkline: series?.dataCum, sparkColor: p.ok },
        { key: 'dur',   label: 'Duration',         value: durVal, unit: durUnit, progress: 100 },
    ];

    document.getElementById('hero-stats').innerHTML = stats.map(s => {
        const tail = s.sparkline && s.sparkline.length > 1
            ? `<div class="sparkline-wrap"><canvas></canvas></div>`
            : (s.progress != null
                ? `<div class="progress-bar" style="--progress: ${s.progress}%;"></div>`
                : '');
        return `
        <div class="hero-stat${s.peak ? ' peak' : ''}" data-key="${s.key}">
            <span class="label">${s.label}</span>
            <span class="value"><span class="num">${s.value}</span><span class="unit">${s.unit}</span></span>
            ${tail}
        </div>`;
    }).join('');

    // Animate counters on first render
    if (firstRender) {
        document.querySelectorAll('.hero-stat .num').forEach(el => {
            const original = el.textContent;
            const { num, suffix } = parseValueWithSuffix(original);
            const decimals = (original.split('.')[1]?.match(/^\d+/) || [''])[0].length;
            animateNum(el, num, (v) => v.toFixed(decimals) + suffix, 800);
        });
    }

    // Draw sparklines
    requestAnimationFrame(() => {
        document.querySelectorAll('.hero-stat').forEach(card => {
            const canvas = card.querySelector('.sparkline-wrap canvas');
            if (!canvas) return;
            const key = card.dataset.key;
            const stat = stats.find(s => s.key === key);
            if (!stat?.sparkline) return;
            drawSparkline(canvas, stat.sparkline, { color: stat.sparkColor });
        });
    });
}

// ----- Operation cards -----
function renderOperationCards() {
    const container = document.getElementById('operation-cards');
    const opTypes = Object.keys(data.by_op_type || {});
    document.getElementById('op-count').textContent = `${opTypes.length} operation${opTypes.length === 1 ? '' : 's'}`;

    if (opTypes.length === 0) {
        container.innerHTML = '<div class="empty">No operation data available</div>';
        return;
    }

    container.innerHTML = opTypes.map((opType, idx) => {
        const opData = data.by_op_type[opType];
        const tp = opData?.throughput;
        const color = getOpColor(opType);

        let bps = 0, opsPerSec = 0;
        if (tp && tp.measure_duration_millis > 0) {
            bps = (tp.bytes * 1000) / tp.measure_duration_millis;
            opsPerSec = (tp.ops * 1000) / tp.measure_duration_millis;
        }
        const tput = formatThroughput(bps, opsPerSec);
        const lat = getLatencyStats(opData);
        const ttfb = getTtfbStats(opData);

        return `
            <div class="op-card" style="--op-tint:${color};">
                <div class="op-card-bar">
                    <span class="op-tag">${opType}</span>
                    <div class="bar-stat primary">
                        <span class="label">Throughput</span>
                        <span class="value">${tput.value} ${tput.unit}</span>
                    </div>
                    <div class="bar-stat">
                        <span class="label">Requests</span>
                        <span class="value">${formatNumber(opData?.total_requests || 0)}</span>
                    </div>
                    <div class="bar-stat">
                        <span class="label">Data</span>
                        <span class="value">${formatBytes(opData?.total_bytes || 0)}</span>
                    </div>
                    <div class="bar-stat">
                        <span class="label">Ops/sec</span>
                        <span class="value">${opsPerSec.toFixed(1)}</span>
                    </div>
                    <div class="bar-stat">
                        <span class="label">Concurrency</span>
                        <span class="value">${opData?.concurrency || '—'}</span>
                    </div>
                    <div class="bar-stat">
                        <span class="label">Duration</span>
                        <span class="value">${formatDuration(tp?.measure_duration_millis || 0)}</span>
                    </div>
                </div>
                <div class="op-card-body">
                    <div class="chart-panel">
                        <div class="chart-panel-head">
                            <span class="ttl">Throughput over time</span>
                            <span class="axis-note">${useOpsAxis(opData) ? 'obj/s' : 'MiB/s'}</span>
                        </div>
                        <div class="chart-panel-canvas"><canvas id="op-throughput-${idx}"></canvas></div>
                        ${tp?.segmented ? `
                        <div class="chart-panel-stats three-col">
                            ${makeStatCell('Median', formatThroughputCompact(tp.segmented.median_bps, tp.segmented.median_ops), '')}
                            ${makeStatCell('Fastest', formatThroughputCompact(tp.segmented.fastest_bps, tp.segmented.fastest_ops), 'green')}
                            ${makeStatCell('Slowest', formatThroughputCompact(tp.segmented.slowest_bps, tp.segmented.slowest_ops), 'red')}
                        </div>` : ''}
                    </div>
                    ${ttfb ? `
                    <div class="chart-panel">
                        <div class="chart-panel-head">
                            <span class="ttl">Time to first byte</span>
                            <span class="axis-note">milliseconds</span>
                        </div>
                        <div class="chart-panel-canvas"><canvas id="op-ttfb-${idx}"></canvas></div>
                        ${ttfb ? `
                        <div class="chart-panel-stats">
                            ${makeStatCell('Avg', formatLatency(ttfb.avg), 'accent')}
                            ${makeStatCell('p50', formatLatency(ttfb.p50), 'green')}
                            ${makeStatCell('p90', formatLatency(ttfb.p90), 'amber')}
                            ${makeStatCell('p99', formatLatency(ttfb.p99), 'red')}
                        </div>` : ''}
                    </div>` : ''}
                    <div class="chart-panel">
                        <div class="chart-panel-head">
                            <span class="ttl">Response time</span>
                            <span class="axis-note">milliseconds</span>
                        </div>
                        <div class="chart-panel-canvas"><canvas id="op-latency-${idx}"></canvas></div>
                        ${lat ? `
                        <div class="chart-panel-stats">
                            ${makeStatCell('Avg', formatLatency(lat.avg), 'accent')}
                            ${makeStatCell('p50', formatLatency(lat.p50), 'green')}
                            ${makeStatCell('p90', formatLatency(lat.p90), 'amber')}
                            ${makeStatCell('p99', formatLatency(lat.p99), 'red')}
                        </div>` : ''}
                    </div>
                </div>
            </div>
        `;
    }).join('');

    opTypes.forEach((opType, idx) => {
        renderOpThroughputChart(opType, idx);
        renderOpLatencyChart(opType, idx);
        renderOpTtfbChart(opType, idx);
    });
}

function makeStatCell(pct, val, cls) {
    return `<div class="stat-cell"><span class="pct">${pct}</span><span class="v ${cls || ''}">${val}</span></div>`;
}

function formatThroughputCompact(bps, ops) {
    const t = formatThroughput(bps, ops);
    return `${t.value} ${t.unit}`;
}

function useOpsAxis(opData) {
    const segs = opData?.throughput?.segmented?.segments;
    if (!segs || segs.length === 0) return false;
    return segs.every(s => !s.bytes_per_sec || s.bytes_per_sec === 0);
}

function getLatencyStats(opData) {
    if (!opData?.requests_by_client) return null;
    let totalAvg = 0, totalP50 = 0, totalP90 = 0, totalP99 = 0, count = 0;
    for (const clientReqs of Object.values(opData.requests_by_client)) {
        for (const seg of clientReqs) {
            const single = seg.single_sized_requests;
            if (single && !single.skipped) {
                totalAvg += single.dur_avg_millis || 0;
                totalP50 += single.dur_median_millis || 0;
                totalP90 += single.dur_90_millis || 0;
                totalP99 += single.dur_99_millis || 0;
                count++;
            }
        }
    }
    if (count === 0) return null;
    return { avg: totalAvg / count, p50: totalP50 / count, p90: totalP90 / count, p99: totalP99 / count };
}

function getTtfbStats(opData) {
    if (!opData?.requests_by_client) return null;
    let totalAvg = 0, totalP50 = 0, totalP90 = 0, totalP99 = 0, count = 0;
    for (const clientReqs of Object.values(opData.requests_by_client)) {
        for (const seg of clientReqs) {
            const single = seg.single_sized_requests;
            const multi = seg.multi_sized_requests;
            const ttfb = single?.first_byte || (multi?.by_size?.[0]?.first_byte);
            if (ttfb) {
                totalAvg += ttfb.average_millis || 0;
                totalP50 += ttfb.median_millis || 0;
                totalP90 += ttfb.p90_millis || 0;
                totalP99 += ttfb.p99_millis || 0;
                count++;
            }
        }
    }
    if (count === 0) return null;
    return { avg: totalAvg / count, p50: totalP50 / count, p90: totalP90 / count, p99: totalP99 / count };
}

function mergeTimeSeriesByTimestamp(segments) {
    if (segments.length === 0) return [];
    segments.sort((a, b) => a.time - b.time);
    const merged = [];
    let i = 0;
    while (i < segments.length) {
        const ts = segments[i].time.getTime();
        let avg = 0, p50 = 0, p90 = 0, p99 = 0, count = 0;
        while (i < segments.length && segments[i].time.getTime() === ts) {
            avg += segments[i].avg;
            p50 += segments[i].p50;
            p90 += segments[i].p90;
            p99 += segments[i].p99;
            count++;
            i++;
        }
        merged.push({ time: new Date(ts), avg: avg/count, p50: p50/count, p90: p90/count, p99: p99/count });
    }
    return merged;
}

function getLatencyTimeSeries(opData) {
    if (!opData?.requests_by_client) return null;
    const segments = [];
    for (const clientReqs of Object.values(opData.requests_by_client)) {
        for (const seg of clientReqs) {
            const single = seg.single_sized_requests;
            if (single && !single.skipped && seg.start_time) {
                segments.push({
                    time: new Date(seg.start_time),
                    avg: single.dur_avg_millis,
                    p50: single.dur_median_millis,
                    p90: single.dur_90_millis,
                    p99: single.dur_99_millis,
                });
            }
        }
    }
    if (segments.length === 0) return null;
    return mergeTimeSeriesByTimestamp(segments);
}

function getTtfbTimeSeries(opData) {
    if (!opData?.requests_by_client) return null;
    const segments = [];
    for (const clientReqs of Object.values(opData.requests_by_client)) {
        for (const seg of clientReqs) {
            const single = seg.single_sized_requests;
            const multi = seg.multi_sized_requests;
            const ttfb = single?.first_byte || (multi?.by_size?.[0]?.first_byte);
            if (ttfb && seg.start_time) {
                segments.push({
                    time: new Date(seg.start_time),
                    avg: ttfb.average_millis || 0,
                    p50: ttfb.median_millis || 0,
                    p90: ttfb.p90_millis || 0,
                    p99: ttfb.p99_millis || 0,
                });
            }
        }
    }
    if (segments.length === 0) return null;
    return mergeTimeSeriesByTimestamp(segments);
}

// ----- Chart helpers -----
function chartScales(opts) {
    return {
        x: {
            type: 'time',
            time: { displayFormats: { second: 'HH:mm:ss', minute: 'HH:mm' } },
            grid: { display: false },
            ticks: { font: { size: 10 }, color: p.fgFaint, maxRotation: 0, autoSkipPadding: 14 },
            border: { display: false },
        },
        y: {
            beginAtZero: true,
            title: opts?.yTitle ? { display: false } : undefined,
            grid: { color: chartGridColor, lineWidth: 1, drawTicks: false },
            ticks: { font: { size: 10 }, color: p.fgFaint, padding: 8 },
            border: { display: false },
        },
    };
}

const chartLegend = {
    display: true,
    position: 'top',
    align: 'end',
    labels: {
        boxWidth: 8,
        boxHeight: 8,
        usePointStyle: true,
        pointStyle: 'circle',
        padding: 14,
        font: { size: 11, weight: '500', family: "'General Sans', sans-serif" },
        color: p.fgDim,
    },
};

function createGradientFill(ctx, color, opacity) {
    const g = ctx.createLinearGradient(0, 0, 0, ctx.canvas.height);
    g.addColorStop(0, hexToRgba(color, opacity || 0.18));
    g.addColorStop(1, hexToRgba(color, 0));
    return g;
}

function renderOpThroughputChart(opType, idx) {
    const opData = data.by_op_type[opType];
    const segments = sampleData(opData?.throughput?.segmented?.segments);
    if (!segments) return;
    const canvas = document.getElementById(`op-throughput-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const color = getOpColor(opType);
    const useOps = useOpsAxis(opData);

    new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: opType,
                data: segments.map(s => ({
                    x: new Date(s.start),
                    y: useOps ? s.obj_per_sec : s.bytes_per_sec / (1024 * 1024),
                })),
                borderColor: color,
                backgroundColor: createGradientFill(ctx, color, 0.18),
                fill: true,
                pointRadius: 0,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            plugins: { legend: { display: false } },
            scales: chartScales({ yTitle: useOps ? 'obj/s' : 'MiB/s' }),
        },
    });
}

function renderOpLatencyChart(opType, idx) {
    const opData = data.by_op_type[opType];
    const ts = sampleData(getLatencyTimeSeries(opData));
    if (!ts) return;
    const canvas = document.getElementById(`op-latency-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                lineDS('p50', ts.map(s => ({ x: s.time, y: s.p50 })), p.ok, ctx, 0.16, true),
                lineDS('p90', ts.map(s => ({ x: s.time, y: s.p90 })), p.warn, ctx, 0, false),
                lineDS('p99', ts.map(s => ({ x: s.time, y: s.p99 })), p.danger, ctx, 0, false),
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: chartLegend },
            scales: chartScales({ yTitle: 'ms' }),
        },
    });
}

function renderOpTtfbChart(opType, idx) {
    const opData = data.by_op_type[opType];
    if (!hasTtfb(opType, opData)) return;
    const ts = sampleData(getTtfbTimeSeries(opData));
    if (!ts) return;
    const canvas = document.getElementById(`op-ttfb-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                lineDS('p50', ts.map(s => ({ x: s.time, y: s.p50 })), p.cyan, ctx, 0.16, true),
                lineDS('p90', ts.map(s => ({ x: s.time, y: s.p90 })), p.warn, ctx, 0, false),
                lineDS('p99', ts.map(s => ({ x: s.time, y: s.p99 })), p.purple, ctx, 0, false),
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: chartLegend },
            scales: chartScales({ yTitle: 'ms' }),
        },
    });
}

function lineDS(label, dataset, color, ctx, fillOpacity, fill) {
    return {
        label,
        data: dataset,
        borderColor: color,
        backgroundColor: fillOpacity ? createGradientFill(ctx, color, fillOpacity) : 'transparent',
        fill: !!fill,
        pointRadius: 0,
    };
}

function hexToRgba(hex, a) {
    const h = hex.replace('#', '');
    const r = parseInt(h.substr(0, 2), 16);
    const g = parseInt(h.substr(2, 2), 16);
    const b = parseInt(h.substr(4, 2), 16);
    return `rgba(${r},${g},${b},${a})`;
}

// ----- Hosts -----
function renderHostsTable() {
    const container = document.getElementById('hosts-container');
    const opTypes = Object.keys(data.by_op_type || {}).sort();
    const hosts = [...(data.total?.hosts || [])].sort();

    if (hosts.length === 0 || opTypes.length === 0) {
        container.innerHTML = '<div class="empty">No server data available</div>';
        return;
    }

    let chartIdx = 0;
    container.innerHTML = opTypes.map((opType) => {
        const opData = data.by_op_type[opType];
        const byHost = opData?.throughput_by_host || {};
        const color = getOpColor(opType);
        const ci = chartIdx++;
        return `
            <div class="op-table-section">
                <h3 class="section-label">
                    <span class="badge-op" style="background:${color};">${opType}</span>
                    <span class="sep">·</span> <span class="scope">By server</span>
                </h3>
                <div class="breakdown-chart-container"><canvas id="host-chart-${ci}"></canvas></div>
                ${hasTtfb(opType, opData) ? `<div class="breakdown-chart-container"><canvas id="host-ttfb-chart-${ci}"></canvas></div>` : ''}
                <table>
                    <thead>
                        <tr><th>Server</th><th>Operations</th><th>Data</th><th>Throughput</th><th>Errors</th></tr>
                    </thead>
                    <tbody>
                        ${hosts.map(host => {
                            const h = byHost[host] || {};
                            const bps = h.measure_duration_millis > 0 ? (h.bytes * 1000) / h.measure_duration_millis : 0;
                            const ops = h.measure_duration_millis > 0 ? (h.ops * 1000) / h.measure_duration_millis : 0;
                            const tp = formatThroughput(bps, ops);
                            return `<tr>
                                <td>${host}</td>
                                <td>${formatNumber(h.ops || 0)}</td>
                                <td>${formatBytes(h.bytes || 0)}</td>
                                <td>${tp.value} ${tp.unit}</td>
                                <td style="color:${(h.errors || 0) > 0 ? p.danger : p.fgFaint}">${h.errors || 0}</td>
                            </tr>`;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    chartIdx = 0;
    opTypes.forEach((opType) => {
        const ci = chartIdx++;
        renderHostThroughputChart(opType, ci, hosts);
        renderHostTtfbChart(opType, ci, hosts);
    });
}

function renderClientsTable() {
    const container = document.getElementById('clients-container');
    const opTypes = Object.keys(data.by_op_type || {}).sort();
    const clients = [...(data.total?.clients || [])].sort();

    if (clients.length === 0 || opTypes.length === 0) {
        container.innerHTML = '<div class="empty">No client data available</div>';
        return;
    }

    let chartIdx = 0;
    container.innerHTML = opTypes.map((opType) => {
        const opData = data.by_op_type[opType];
        const byClient = opData?.throughput_by_client || {};
        const color = getOpColor(opType);
        const ci = chartIdx++;
        return `
            <div class="op-table-section">
                <h3 class="section-label">
                    <span class="badge-op" style="background:${color};">${opType}</span>
                    <span class="sep">·</span> <span class="scope">By client</span>
                </h3>
                <div class="breakdown-chart-container"><canvas id="client-chart-${ci}"></canvas></div>
                ${hasTtfb(opType, opData) ? `<div class="breakdown-chart-container"><canvas id="client-ttfb-chart-${ci}"></canvas></div>` : ''}
                <table>
                    <thead>
                        <tr><th>Client</th><th>Operations</th><th>Data</th><th>Throughput</th><th>Errors</th></tr>
                    </thead>
                    <tbody>
                        ${clients.map(client => {
                            const c = byClient[client] || {};
                            const bps = c.measure_duration_millis > 0 ? (c.bytes * 1000) / c.measure_duration_millis : 0;
                            const ops = c.measure_duration_millis > 0 ? (c.ops * 1000) / c.measure_duration_millis : 0;
                            const tp = formatThroughput(bps, ops);
                            return `<tr>
                                <td>${client}</td>
                                <td>${formatNumber(c.ops || 0)}</td>
                                <td>${formatBytes(c.bytes || 0)}</td>
                                <td>${tp.value} ${tp.unit}</td>
                                <td style="color:${(c.errors || 0) > 0 ? p.danger : p.fgFaint}">${c.errors || 0}</td>
                            </tr>`;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    chartIdx = 0;
    opTypes.forEach((opType) => {
        const ci = chartIdx++;
        renderClientThroughputChart(opType, ci, clients);
        renderClientTtfbChart(opType, ci, clients);
    });
}

const seriesColors = ['accent', 'info', 'ok', 'cyan', 'purple', 'pink', 'warn', 'danger'];
function getSeriesColor(i) { return pc(seriesColors[i % seriesColors.length]); }

function renderHostThroughputChart(opType, idx, hosts) {
    const opData = data.by_op_type[opType];
    const byHostData = data.by_host || {};
    const segments = sampleData(opData?.throughput?.segmented?.segments);
    const useOps = useOpsAxis(opData);
    const canvas = document.getElementById(`host-chart-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const datasets = [];
    hosts.forEach((host, i) => {
        const hostAgg = byHostData[host];
        const segs = sampleData(hostAgg?.throughput?.segmented?.segments);
        if (segs && segs.length > 0) {
            datasets.push({
                label: host,
                data: segs.map(s => ({
                    x: new Date(s.start),
                    y: useOps ? s.obj_per_sec : s.bytes_per_sec / (1024 * 1024),
                })),
                borderColor: getSeriesColor(i),
                backgroundColor: 'transparent',
                fill: false,
                pointRadius: 0,
            });
        }
    });

    if (datasets.length === 0 && segments) {
        const color = getOpColor(opType);
        datasets.push({
            label: 'All servers',
            data: segments.map(s => ({
                x: new Date(s.start),
                y: useOps ? s.obj_per_sec : s.bytes_per_sec / (1024 * 1024),
            })),
            borderColor: color,
            backgroundColor: createGradientFill(ctx, color, 0.18),
            fill: true,
            pointRadius: 0,
        });
    }
    if (datasets.length === 0) return;

    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { ...chartLegend, display: datasets.length > 1 } },
            scales: chartScales({ yTitle: useOps ? 'obj/s' : 'MiB/s' }),
        },
    });
}

function renderClientThroughputChart(opType, idx, clients) {
    const opData = data.by_op_type[opType];
    const byClientData = data.by_client || {};
    const segments = sampleData(opData?.throughput?.segmented?.segments);
    const useOps = useOpsAxis(opData);
    const canvas = document.getElementById(`client-chart-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const datasets = [];
    clients.forEach((client, i) => {
        const clientAgg = byClientData[client];
        const segs = sampleData(clientAgg?.throughput?.segmented?.segments);
        if (segs && segs.length > 0) {
            datasets.push({
                label: client,
                data: segs.map(s => ({
                    x: new Date(s.start),
                    y: useOps ? s.obj_per_sec : s.bytes_per_sec / (1024 * 1024),
                })),
                borderColor: getSeriesColor(i),
                backgroundColor: 'transparent',
                fill: false,
                pointRadius: 0,
            });
        }
    });

    if (datasets.length === 0 && segments) {
        const color = getOpColor(opType);
        datasets.push({
            label: 'All clients',
            data: segments.map(s => ({
                x: new Date(s.start),
                y: useOps ? s.obj_per_sec : s.bytes_per_sec / (1024 * 1024),
            })),
            borderColor: color,
            backgroundColor: createGradientFill(ctx, color, 0.18),
            fill: true,
            pointRadius: 0,
        });
    }
    if (datasets.length === 0) return;

    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { ...chartLegend, display: datasets.length > 1 } },
            scales: chartScales({ yTitle: useOps ? 'obj/s' : 'MiB/s' }),
        },
    });
}

function renderHostTtfbChart(opType, idx, hosts) {
    const opData = data.by_op_type[opType];
    if (!hasTtfb(opType, opData)) return;
    const byHostData = data.by_host || {};
    const canvas = document.getElementById(`host-ttfb-chart-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const datasets = [];
    hosts.forEach((host, i) => {
        const hostAgg = byHostData[host];
        const ts = sampleData(getTtfbTimeSeries(hostAgg));
        if (ts && ts.length > 0) {
            datasets.push({
                label: host + ' p50',
                data: ts.map(s => ({ x: s.time, y: s.p50 })),
                borderColor: getSeriesColor(i),
                backgroundColor: 'transparent',
                fill: false,
                pointRadius: 0,
            });
        }
    });
    if (datasets.length === 0) return;

    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                title: { display: true, text: 'TTFB · p50', font: { size: 11, weight: '600' }, color: p.fgDim, align: 'start' },
                legend: { ...chartLegend, display: datasets.length > 1 },
            },
            scales: chartScales({ yTitle: 'ms' }),
        },
    });
}

function renderClientTtfbChart(opType, idx, clients) {
    const opData = data.by_op_type[opType];
    if (!hasTtfb(opType, opData)) return;
    const byClientData = data.by_client || {};
    const canvas = document.getElementById(`client-ttfb-chart-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    const datasets = [];
    clients.forEach((client, i) => {
        const clientAgg = byClientData[client];
        const ts = sampleData(getTtfbTimeSeries(clientAgg));
        if (ts && ts.length > 0) {
            datasets.push({
                label: client + ' p50',
                data: ts.map(s => ({ x: s.time, y: s.p50 })),
                borderColor: getSeriesColor(i),
                backgroundColor: 'transparent',
                fill: false,
                pointRadius: 0,
            });
        }
    });
    if (datasets.length === 0) return;

    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                title: { display: true, text: 'TTFB · p50', font: { size: 11, weight: '600' }, color: p.fgDim, align: 'start' },
                legend: { ...chartLegend, display: datasets.length > 1 },
            },
            scales: chartScales({ yTitle: 'ms' }),
        },
    });
}

// Redraw sparklines on resize (debounced)
let _resizeTimer = null;
window.addEventListener('resize', () => {
    clearTimeout(_resizeTimer);
    _resizeTimer = setTimeout(() => {
        if (!data) return;
        const series = getSparklineSeries(data);
        if (!series) return;
        const map = { peak: series.peak, ops: series.opsCum, data: series.dataCum };
        const colorMap = { peak: p.accent, ops: p.info, data: p.ok };
        document.querySelectorAll('.hero-stat[data-key]').forEach(card => {
            const canvas = card.querySelector('.sparkline-wrap canvas');
            if (!canvas) return;
            const key = card.dataset.key;
            if (map[key]) drawSparkline(canvas, map[key], { color: colorMap[key] });
        });
    }, 200);
});

loadData();
