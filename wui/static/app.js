// Warp Web UI Application - Redesigned

// Color palette
const chartColors = {
    blue: 'hsl(217, 91%, 60%)',
    cyan: 'hsl(187, 95%, 42%)',
    amber: 'hsl(38, 92%, 50%)',
    purple: 'hsl(280, 65%, 60%)',
    pink: 'hsl(330, 81%, 60%)',
    success: 'hsl(142, 71%, 45%)',
    error: 'hsl(0, 84%, 60%)',
    primary: 'hsl(348, 83%, 47%)',
};

const opColors = {
    'GET': chartColors.success,
    'PUT': chartColors.blue,
    'DELETE': chartColors.amber,
    'LIST': chartColors.purple,
    'STAT': chartColors.cyan,
    'MULTIPART': chartColors.pink
};

// Theme management
function getTheme() {
    return localStorage.getItem('warp-theme') || 'dark';
}

function setTheme(theme) {
    localStorage.setItem('warp-theme', theme);
    document.body.classList.toggle('light', theme === 'light');
    updateThemeButton(theme);
    updateChartDefaults(theme);
    destroyAllCharts();
    if (data) renderDashboard();
}

function updateThemeButton(theme) {
    const iconDark = document.getElementById('theme-icon-dark');
    const iconLight = document.getElementById('theme-icon-light');
    const label = document.getElementById('theme-label');
    if (theme === 'dark') {
        iconDark.style.display = 'block';
        iconLight.style.display = 'none';
        label.textContent = 'Light';
    } else {
        iconDark.style.display = 'none';
        iconLight.style.display = 'block';
        label.textContent = 'Dark';
    }
}

function updateChartDefaults(theme) {
    if (theme === 'light') {
        Chart.defaults.color = 'hsl(0, 0%, 45.1%)';
        Chart.defaults.borderColor = 'hsl(0, 0%, 89.8%)';
    } else {
        Chart.defaults.color = 'hsl(0, 0%, 64%)';
        Chart.defaults.borderColor = 'hsl(0, 0%, 15%)';
    }
}

// Initialize theme
const initialTheme = getTheme();
document.body.classList.toggle('light', initialTheme === 'light');
updateChartDefaults(initialTheme);
Chart.defaults.font.family = "'Geist', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";

document.getElementById('theme-toggle').addEventListener('click', () => {
    setTheme(getTheme() === 'dark' ? 'light' : 'dark');
});
updateThemeButton(initialTheme);

// Utility functions
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatBytesPerSec(bps) {
    if (bps === 0) return '0 B/s';
    const k = 1024;
    const sizes = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s', 'TiB/s'];
    const i = Math.floor(Math.log(bps) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatThroughput(bps) {
    if (bps === 0) return { value: '0', unit: 'B/s' };
    const k = 1024;
    const sizes = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s', 'TiB/s'];
    const i = Math.floor(Math.log(bps) / Math.log(k));
    return {
        value: parseFloat((bps / Math.pow(k, i)).toFixed(1)),
        unit: sizes[i]
    };
}

function formatDuration(ms) {
    if (ms < 1000) return ms + 'ms';
    const seconds = ms / 1000;
    if (seconds < 60) return seconds.toFixed(1) + 's';
    const minutes = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    if (minutes < 60) return minutes + 'm ' + secs + 's';
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    return hours + 'h ' + mins + 'm';
}

function formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toFixed(0);
}

function formatLatency(ms) {
    if (ms < 1) return ms.toFixed(2) + 'ms';
    if (ms < 1000) return ms.toFixed(0) + 'ms';
    return (ms / 1000).toFixed(2) + 's';
}

function getOpColor(opType) {
    return opColors[opType.toUpperCase()] || chartColors.primary;
}

// Check if operation should show TTFB
// Only PUT and POST (uploads) don't have TTFB
function hasTtfb(opType) {
    const uploadOps = ['PUT', 'POST'];
    return !uploadOps.includes(opType.toUpperCase());
}

// Sample data to reduce points for smoother charts
// Keeps ~maxPoints evenly distributed samples
function sampleData(arr, maxPoints = 100) {
    if (!arr || arr.length <= maxPoints) return arr;
    const step = arr.length / maxPoints;
    const sampled = [];
    for (let i = 0; i < arr.length; i += step) {
        sampled.push(arr[Math.floor(i)]);
    }
    // Always include the last point
    if (sampled[sampled.length - 1] !== arr[arr.length - 1]) {
        sampled.push(arr[arr.length - 1]);
    }
    return sampled;
}

// Tab handling
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    });
});

// Main application
let data = null;
let charts = {};
let chartCounter = 0;

async function loadData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error('Failed to load data');
        const apiResponse = await response.json();
        data = apiResponse.data || apiResponse;
        renderDashboard();
    } catch (error) {
        console.error('Error loading data:', error);
        document.body.innerHTML = '<div style="padding: 2rem; text-align: center; color: var(--error);">Error loading benchmark data: ' + error.message + '</div>';
    }
}

function destroyAllCharts() {
    for (const id in Chart.instances) {
        if (Chart.instances[id]) Chart.instances[id].destroy();
    }
    charts = {};
    chartCounter = 0;
}

function renderDashboard() {
    destroyAllCharts();
    renderConfigCard();
    renderHeroStats();
    renderOperationCards();
    renderHostsTable();
    renderClientsTable();
}

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
            const value = valueParts.join('=') || 'true';
            params[key] = value;
        }
    }

    return { op, params };
}

function renderConfigCard() {
    const container = document.getElementById('config-card');
    const cmdline = data.commandline || '';
    const { op, params } = parseCommandLine(cmdline);
    const total = data.total || {};
    const hosts = total.hosts || [];
    const clients = total.clients || [];

    document.getElementById('benchmark-type').textContent = `S3 Benchmark`;

    const displayParams = [
        { label: 'Hosts', value: params.host || hosts.join(', ') || '-', show: true },
        { label: 'Bucket', value: params.bucket || '-', show: !!params.bucket },
        { label: 'Clients', value: clients.length || params['warp-client'] || '-', show: clients.length > 0 || !!params['warp-client'] },
        { label: 'Concurrent', value: total.concurrency || params.concurrent || '-', show: true },
        { label: 'Object Size', value: params['obj.size'] || '-', show: !!params['obj.size'] },
        { label: 'Duration', value: params.duration || '-', show: !!params.duration },
        { label: 'Multipart', value: params['disable-multipart'] === 'true' ? 'Disabled' : 'Enabled', show: params['disable-multipart'] !== undefined },
        { label: 'Auto-term', value: params.autoterm === 'true' ? 'On' : 'Off', show: params.autoterm !== undefined },
    ].filter(p => p.show);

    const hasErrors = (total.total_errors || 0) > 0;

    container.innerHTML = `
        <div class="config-header">
            <span class="op-badge">${op.toUpperCase()}</span>
        </div>
        <div class="config-params">
            ${displayParams.map(p => `
                <div class="config-param">
                    <span class="label">${p.label}</span>
                    <span class="value">${p.value}</span>
                </div>
            `).join('')}
            ${hasErrors ? `
                <div class="config-param">
                    <span class="label">Errors</span>
                    <span class="value" style="color: var(--error);">${total.total_errors}</span>
                </div>
            ` : ''}
        </div>
        <button class="config-toggle" onclick="toggleFullCmd()">Show Command</button>
        <div class="config-full-cmd" id="full-cmd">
            <code>${cmdline}</code>
        </div>
    `;
}

function toggleFullCmd() {
    const el = document.getElementById('full-cmd');
    const btn = document.querySelector('.config-toggle');
    el.classList.toggle('show');
    btn.textContent = el.classList.contains('show') ? 'Hide Command' : 'Show Command';
}

function renderHeroStats() {
    const container = document.getElementById('hero-stats');
    const total = data.total || {};
    const tp = total.throughput;

    let bps = 0, opsPerSec = 0;
    if (tp && tp.measure_duration_millis > 0) {
        bps = (tp.bytes * 1000) / tp.measure_duration_millis;
        opsPerSec = (tp.ops * 1000) / tp.measure_duration_millis;
    }

    const throughput = formatThroughput(bps);

    container.innerHTML = `
        <div class="hero-stat primary">
            <div class="label">Peak Throughput</div>
            <div class="value">${throughput.value}<span class="unit"> ${throughput.unit}</span></div>
        </div>
        <div class="hero-stat success">
            <div class="label">Operations</div>
            <div class="value">${formatNumber(total.total_requests || 0)}</div>
        </div>
        <div class="hero-stat info">
            <div class="label">Data Transferred</div>
            <div class="value">${formatBytes(total.total_bytes || 0)}</div>
        </div>
        <div class="hero-stat">
            <div class="label">Duration</div>
            <div class="value">${formatDuration(tp?.measure_duration_millis || 0)}</div>
        </div>
    `;
}

function renderOperationCards() {
    const container = document.getElementById('operation-cards');
    const opTypes = Object.keys(data.by_op_type || {});

    if (opTypes.length === 0) {
        container.innerHTML = '<p style="color: var(--muted-foreground); text-align: center; padding: 2rem;">No operation data available.</p>';
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

        const throughput = formatThroughput(bps);
        const latencyData = getLatencyStats(opData);

        return `
            <div class="op-card" style="--op-color: ${color};">
                <div class="op-card-header">
                    <div class="op-name">
                        <span class="op-badge">${opType}</span>
                        <span class="op-duration">${formatDuration(tp?.measure_duration_millis || 0)}</span>
                    </div>
                    <div class="op-throughput">
                        <div class="value">${throughput.value} <span class="unit">${throughput.unit}</span></div>
                        <div class="label">Throughput</div>
                    </div>
                </div>
                <div class="op-card-body">
                    <div class="op-metrics">
                        <div class="op-metric">
                            <div class="label">Requests</div>
                            <div class="value">${formatNumber(opData?.total_requests || 0)}</div>
                        </div>
                        <div class="op-metric">
                            <div class="label">Data</div>
                            <div class="value">${formatBytes(opData?.total_bytes || 0)}</div>
                        </div>
                        <div class="op-metric">
                            <div class="label">Ops/sec</div>
                            <div class="value">${opsPerSec.toFixed(1)}</div>
                        </div>
                        <div class="op-metric">
                            <div class="label">Concurrency</div>
                            <div class="value">${opData?.concurrency || '-'}</div>
                        </div>
                    </div>
                    <div class="op-charts">
                        <div class="op-chart-section">
                            <h4>Throughput Over Time</h4>
                            <canvas id="op-throughput-${idx}"></canvas>
                            ${tp?.segmented ? `
                            <div class="latency-grid">
                                <div class="latency-item">
                                    <div class="percentile">Median</div>
                                    <div class="time">${formatThroughput(tp.segmented.median_bps).value} ${formatThroughput(tp.segmented.median_bps).unit}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">Fastest</div>
                                    <div class="time" style="color: ${chartColors.success};">${formatThroughput(tp.segmented.fastest_bps).value} ${formatThroughput(tp.segmented.fastest_bps).unit}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">Slowest</div>
                                    <div class="time" style="color: ${chartColors.error};">${formatThroughput(tp.segmented.slowest_bps).value} ${formatThroughput(tp.segmented.slowest_bps).unit}</div>
                                </div>
                            </div>
                            ` : ''}
                        </div>
                        ${hasTtfb(opType) ? `
                        <div class="op-chart-section">
                            <h4>Time To First Byte</h4>
                            <canvas id="op-ttfb-${idx}"></canvas>
                            ${(() => { const ttfbStats = getTtfbStats(opData); return ttfbStats ? `
                            <div class="latency-grid">
                                <div class="latency-item">
                                    <div class="percentile">Avg</div>
                                    <div class="time">${formatLatency(ttfbStats.avg)}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">P50</div>
                                    <div class="time" style="color: ${chartColors.cyan};">${formatLatency(ttfbStats.p50)}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">P90</div>
                                    <div class="time" style="color: ${chartColors.amber};">${formatLatency(ttfbStats.p90)}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">P99</div>
                                    <div class="time" style="color: ${chartColors.purple};">${formatLatency(ttfbStats.p99)}</div>
                                </div>
                            </div>
                            ` : ''; })()}
                        </div>
                        ` : ''}
                        <div class="op-chart-section">
                            <h4>Response Time Distribution</h4>
                            <canvas id="op-latency-${idx}"></canvas>
                            ${latencyData ? `
                            <div class="latency-grid">
                                <div class="latency-item">
                                    <div class="percentile">Avg</div>
                                    <div class="time">${formatLatency(latencyData.avg)}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">P50</div>
                                    <div class="time" style="color: ${chartColors.success};">${formatLatency(latencyData.p50)}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">P90</div>
                                    <div class="time" style="color: ${chartColors.amber};">${formatLatency(latencyData.p90)}</div>
                                </div>
                                <div class="latency-item">
                                    <div class="percentile">P99</div>
                                    <div class="time" style="color: ${chartColors.error};">${formatLatency(latencyData.p99)}</div>
                                </div>
                            </div>
                            ` : ''}
                        </div>
                    </div>
                </div>
            </div>
        `;
    }).join('');

    // Render charts
    opTypes.forEach((opType, idx) => {
        renderOpThroughputChart(opType, idx);
        renderOpLatencyChart(opType, idx);
        renderOpTtfbChart(opType, idx);
    });
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

    return {
        avg: totalAvg / count,
        p50: totalP50 / count,
        p90: totalP90 / count,
        p99: totalP99 / count
    };
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

    return {
        avg: totalAvg / count,
        p50: totalP50 / count,
        p90: totalP90 / count,
        p99: totalP99 / count
    };
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
                    p99: single.dur_99_millis
                });
            }
        }
    }

    if (segments.length === 0) return null;
    segments.sort((a, b) => a.time - b.time);
    return segments;
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
                    p99: ttfb.p99_millis || 0
                });
            }
        }
    }

    if (segments.length === 0) return null;
    segments.sort((a, b) => a.time - b.time);
    return segments;
}

function renderOpThroughputChart(opType, idx) {
    const opData = data.by_op_type[opType];
    const segments = sampleData(opData?.throughput?.segmented?.segments);
    if (!segments) return;

    const canvas = document.getElementById(`op-throughput-${idx}`);
    const ctx = canvas.getContext('2d');
    const color = getOpColor(opType);

    new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [{
                label: 'Throughput',
                data: segments.map(s => ({
                    x: new Date(s.start),
                    y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                })),
                borderColor: color,
                backgroundColor: color + '15',
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'HH:mm:ss' } },
                    grid: { display: false }
                },
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255,255,255,0.05)' }
                }
            }
        }
    });
}

function renderOpLatencyChart(opType, idx) {
    const opData = data.by_op_type[opType];
    const latencyTs = sampleData(getLatencyTimeSeries(opData));
    if (!latencyTs) return;

    const canvas = document.getElementById(`op-latency-${idx}`);
    const ctx = canvas.getContext('2d');

    new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'P50',
                    data: latencyTs.map(s => ({ x: s.time, y: s.p50 })),
                    borderColor: chartColors.success,
                    backgroundColor: chartColors.success + '15',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                    borderWidth: 2
                },
                {
                    label: 'P90',
                    data: latencyTs.map(s => ({ x: s.time, y: s.p90 })),
                    borderColor: chartColors.amber,
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    borderWidth: 2
                },
                {
                    label: 'P99',
                    data: latencyTs.map(s => ({ x: s.time, y: s.p99 })),
                    borderColor: chartColors.error,
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    borderWidth: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { boxWidth: 12, padding: 8, font: { size: 10 } }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'HH:mm:ss' } },
                    grid: { display: false }
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'ms', font: { size: 10 } },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                }
            }
        }
    });
}

function renderOpTtfbChart(opType, idx) {
    // Skip TTFB chart for write operations (PUT, POST - no TTFB concept)
    if (!hasTtfb(opType)) return;

    const opData = data.by_op_type[opType];
    const ttfbTs = sampleData(getTtfbTimeSeries(opData));
    if (!ttfbTs) return;

    const canvas = document.getElementById(`op-ttfb-${idx}`);
    const ctx = canvas.getContext('2d');

    new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'P50',
                    data: ttfbTs.map(s => ({ x: s.time, y: s.p50 })),
                    borderColor: chartColors.cyan,
                    backgroundColor: chartColors.cyan + '15',
                    fill: true,
                    tension: 0.4,
                    pointRadius: 0,
                    borderWidth: 2
                },
                {
                    label: 'P90',
                    data: ttfbTs.map(s => ({ x: s.time, y: s.p90 })),
                    borderColor: chartColors.amber,
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    borderWidth: 2
                },
                {
                    label: 'P99',
                    data: ttfbTs.map(s => ({ x: s.time, y: s.p99 })),
                    borderColor: chartColors.purple,
                    backgroundColor: 'transparent',
                    fill: false,
                    tension: 0.4,
                    pointRadius: 0,
                    borderWidth: 2
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { boxWidth: 12, padding: 8, font: { size: 10 } }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'HH:mm:ss' } },
                    grid: { display: false }
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'ms', font: { size: 10 } },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                }
            }
        }
    });
}

function renderHostsTable() {
    const container = document.getElementById('hosts-container');

    const opTypes = Object.keys(data.by_op_type || {}).sort();
    const hosts = [...(data.total?.hosts || [])].sort();

    if (hosts.length === 0 || opTypes.length === 0) {
        container.innerHTML = '<p style="color: var(--muted-foreground);">No server data available</p>';
        return;
    }

    let chartIdx = 0;
    container.innerHTML = opTypes.map((opType) => {
        const opData = data.by_op_type[opType];
        const byHost = opData?.throughput_by_host || {};
        const color = getOpColor(opType);
        const currentChartIdx = chartIdx++;

        return `
            <div class="op-table-section">
                <h3><span class="badge" style="background: ${color};">${opType}</span> by Server</h3>
                <div class="breakdown-chart-container">
                    <canvas id="host-chart-${currentChartIdx}"></canvas>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Server</th>
                            <th>Operations</th>
                            <th>Data</th>
                            <th>Throughput</th>
                            <th>Errors</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${hosts.map(host => {
                            const hostData = byHost[host] || {};
                            const bps = hostData.measure_duration_millis > 0
                                ? (hostData.bytes * 1000) / hostData.measure_duration_millis
                                : 0;
                            const tp = formatThroughput(bps);
                            return `
                                <tr>
                                    <td>${host}</td>
                                    <td>${formatNumber(hostData.ops || 0)}</td>
                                    <td>${formatBytes(hostData.bytes || 0)}</td>
                                    <td>${tp.value} ${tp.unit}</td>
                                    <td>${hostData.errors || 0}</td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    // Render charts for each op type
    chartIdx = 0;
    opTypes.forEach((opType) => {
        renderHostThroughputChart(opType, chartIdx++, hosts);
    });
}

function renderClientsTable() {
    const container = document.getElementById('clients-container');

    const opTypes = Object.keys(data.by_op_type || {}).sort();
    const clients = [...(data.total?.clients || [])].sort();

    if (clients.length === 0 || opTypes.length === 0) {
        container.innerHTML = '<p style="color: var(--muted-foreground);">No client data available</p>';
        return;
    }

    let chartIdx = 0;
    container.innerHTML = opTypes.map((opType) => {
        const opData = data.by_op_type[opType];
        const byClient = opData?.throughput_by_client || {};
        const color = getOpColor(opType);
        const currentChartIdx = chartIdx++;

        return `
            <div class="op-table-section">
                <h3><span class="badge" style="background: ${color};">${opType}</span> by Client</h3>
                <div class="breakdown-chart-container">
                    <canvas id="client-chart-${currentChartIdx}"></canvas>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Client</th>
                            <th>Operations</th>
                            <th>Data</th>
                            <th>Throughput</th>
                            <th>Errors</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${clients.map(client => {
                            const clientData = byClient[client] || {};
                            const bps = clientData.measure_duration_millis > 0
                                ? (clientData.bytes * 1000) / clientData.measure_duration_millis
                                : 0;
                            const tp = formatThroughput(bps);
                            return `
                                <tr>
                                    <td>${client}</td>
                                    <td>${formatNumber(clientData.ops || 0)}</td>
                                    <td>${formatBytes(clientData.bytes || 0)}</td>
                                    <td>${tp.value} ${tp.unit}</td>
                                    <td>${clientData.errors || 0}</td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    // Render charts for each op type
    chartIdx = 0;
    opTypes.forEach((opType) => {
        renderClientThroughputChart(opType, chartIdx++, clients);
    });
}

// Chart colors for multiple series
const seriesColors = [
    chartColors.success,
    chartColors.blue,
    chartColors.amber,
    chartColors.purple,
    chartColors.pink,
    chartColors.cyan,
    chartColors.error,
    chartColors.primary
];

function getSeriesColor(index) {
    return seriesColors[index % seriesColors.length];
}

function renderHostThroughputChart(opType, idx, hosts) {
    const opData = data.by_op_type[opType];
    const byHostData = data.by_host || {};
    const segments = sampleData(opData?.throughput?.segmented?.segments);

    const canvas = document.getElementById(`host-chart-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    // Try to get per-host segmented data from by_host
    const datasets = [];
    hosts.forEach((host, hostIdx) => {
        const hostAgg = byHostData[host];
        const hostSegments = sampleData(hostAgg?.throughput?.segmented?.segments);
        if (hostSegments && hostSegments.length > 0) {
            datasets.push({
                label: host,
                data: hostSegments.map(s => ({
                    x: new Date(s.start),
                    y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                })),
                borderColor: getSeriesColor(hostIdx),
                backgroundColor: 'transparent',
                fill: false,
                tension: 0.4,
                pointRadius: 0,
                borderWidth: 2
            });
        }
    });

    // Fall back to aggregate throughput if no per-host time series
    if (datasets.length === 0 && segments) {
        datasets.push({
            label: 'All Servers',
            data: segments.map(s => ({
                x: new Date(s.start),
                y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
            })),
            borderColor: getOpColor(opType),
            backgroundColor: getOpColor(opType) + '15',
            fill: true,
            tension: 0.4,
            pointRadius: 0,
            borderWidth: 2
        });
    }

    if (datasets.length === 0) return;

    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: datasets.length > 1,
                    position: 'top',
                    labels: { boxWidth: 12, padding: 8, font: { size: 10 } }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'HH:mm:ss' } },
                    grid: { display: false }
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'MiB/s', font: { size: 10 } },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                }
            }
        }
    });
}

function renderClientThroughputChart(opType, idx, clients) {
    const opData = data.by_op_type[opType];
    const byClientData = data.by_client || {};
    const segments = sampleData(opData?.throughput?.segmented?.segments);

    const canvas = document.getElementById(`client-chart-${idx}`);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    // Try to get per-client segmented data from by_client
    const datasets = [];
    clients.forEach((client, clientIdx) => {
        const clientAgg = byClientData[client];
        const clientSegments = sampleData(clientAgg?.throughput?.segmented?.segments);
        if (clientSegments && clientSegments.length > 0) {
            datasets.push({
                label: client,
                data: clientSegments.map(s => ({
                    x: new Date(s.start),
                    y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                })),
                borderColor: getSeriesColor(clientIdx),
                backgroundColor: 'transparent',
                fill: false,
                tension: 0.4,
                pointRadius: 0,
                borderWidth: 2
            });
        }
    });

    // Fall back to aggregate throughput if no per-client time series
    if (datasets.length === 0 && segments) {
        datasets.push({
            label: 'All Clients',
            data: segments.map(s => ({
                x: new Date(s.start),
                y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
            })),
            borderColor: getOpColor(opType),
            backgroundColor: getOpColor(opType) + '15',
            fill: true,
            tension: 0.4,
            pointRadius: 0,
            borderWidth: 2
        });
    }

    if (datasets.length === 0) return;

    new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: datasets.length > 1,
                    position: 'top',
                    labels: { boxWidth: 12, padding: 8, font: { size: 10 } }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'HH:mm:ss' } },
                    grid: { display: false }
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'MiB/s', font: { size: 10 } },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                }
            }
        }
    });
}

// Initialize
loadData();
