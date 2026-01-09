// Warp Web UI Application

// Chart.js default configuration
Chart.defaults.color = '#6b7280';
Chart.defaults.borderColor = '#e5e7eb';
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';

// Color palette for charts
const colors = {
    primary: '#c72c48',
    get: '#4caf50',
    put: '#2196f3',
    delete: '#ff9800',
    list: '#9c27b0',
    stat: '#00bcd4',
    multipart: '#e91e63',
    default: '#607d8b'
};

const opColors = {
    'GET': colors.get,
    'PUT': colors.put,
    'DELETE': colors.delete,
    'LIST': colors.list,
    'STAT': colors.stat,
    'MULTIPART': colors.multipart
};

// Utility functions
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatBytesPerSec(bps) {
    if (bps === 0) return '0 B/s';
    const k = 1024;
    const sizes = ['B/s', 'KiB/s', 'MiB/s', 'GiB/s'];
    const i = Math.floor(Math.log(bps) / Math.log(k));
    return parseFloat((bps / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
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
    if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
    return num.toFixed(0);
}

function getOpColor(opType) {
    return opColors[opType.toUpperCase()] || colors.default;
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
let autoUpdateInterval = null;
let autoUpdateEnabled = false;

async function loadData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error('Failed to load data');
        const apiResponse = await response.json();

        // Handle wrapped response structure
        data = apiResponse.data || apiResponse;

        // Setup auto-update if enabled by server
        if (apiResponse.auto_update && !autoUpdateEnabled) {
            autoUpdateEnabled = true;
            startAutoUpdate();
        } else if (!apiResponse.auto_update && autoUpdateEnabled) {
            autoUpdateEnabled = false;
            stopAutoUpdate();
        }

        renderDashboard();
        updateAutoUpdateIndicator();
    } catch (error) {
        console.error('Error loading data:', error);
        document.body.innerHTML = '<div style="padding: 2rem; text-align: center; color: #ff5252;">Error loading benchmark data: ' + error.message + '</div>';
    }
}

function startAutoUpdate() {
    if (autoUpdateInterval) return;
    autoUpdateInterval = setInterval(refreshData, 5000);
    console.log('Auto-update enabled (5s interval)');
}

function stopAutoUpdate() {
    if (autoUpdateInterval) {
        clearInterval(autoUpdateInterval);
        autoUpdateInterval = null;
        console.log('Auto-update disabled');
    }
}

async function refreshData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error('Failed to load data');
        const apiResponse = await response.json();

        // Handle wrapped response structure
        data = apiResponse.data || apiResponse;

        // Check if auto-update should continue
        if (!apiResponse.auto_update && autoUpdateEnabled) {
            autoUpdateEnabled = false;
            stopAutoUpdate();
        }

        // Destroy existing charts before re-rendering
        destroyAllCharts();

        // Reset chart counters
        hostChartCounter = 0;
        clientChartCounter = 0;

        renderDashboard();
        updateAutoUpdateIndicator();
    } catch (error) {
        console.error('Error refreshing data:', error);
    }
}

function destroyAllCharts() {
    // Destroy charts stored in the charts object
    for (const key in charts) {
        if (charts[key] && typeof charts[key].destroy === 'function') {
            charts[key].destroy();
        }
    }
    charts = {};

    // Destroy all Chart.js instances
    const chartInstances = Chart.instances;
    for (const id in chartInstances) {
        if (chartInstances[id]) {
            chartInstances[id].destroy();
        }
    }
}

function updateAutoUpdateIndicator() {
    let indicator = document.getElementById('auto-update-indicator');
    if (!indicator) {
        // Create indicator if it doesn't exist
        indicator = document.createElement('div');
        indicator.id = 'auto-update-indicator';
        indicator.style.cssText = 'position: fixed; bottom: 1rem; right: 1rem; padding: 0.5rem 1rem; border-radius: 4px; font-size: 0.875rem; z-index: 1000;';
        document.body.appendChild(indicator);
    }

    if (autoUpdateEnabled) {
        indicator.style.display = 'block';
        indicator.style.background = '#10b981';
        indicator.style.color = 'white';
        indicator.innerHTML = '&#x21bb; Live (5s)';
    } else {
        indicator.style.display = 'none';
    }
}

function renderDashboard() {
    renderCommandline();
    renderSummaryCards();
    renderThroughputChart();
    renderOpsChart();
    renderLatencyChart();
    renderOperationsDetail();
    renderHostsTable();
    renderClientsTable();
}

function renderCommandline() {
    const el = document.getElementById('commandline');
    el.textContent = data.commandline || '';
}

function renderSummaryCards() {
    const total = data.total || {};

    document.getElementById('total-ops').textContent = formatNumber(total.total_requests || 0);
    document.getElementById('total-bytes').textContent = formatBytes(total.total_bytes || 0);

    // Calculate throughput and ops/s
    const tp = total.throughput;
    if (tp && tp.measure_duration_millis > 0) {
        const bps = (tp.bytes * 1000) / tp.measure_duration_millis;
        const ops = (tp.ops * 1000) / tp.measure_duration_millis;
        if (bps > 0) {
            document.getElementById('throughput').textContent = formatBytesPerSec(bps);
        } else {
            document.getElementById('throughput').textContent = '-';
        }
        document.getElementById('ops-per-sec').textContent = ops.toFixed(2);
        document.getElementById('duration').textContent = formatDuration(tp.measure_duration_millis);
    } else {
        document.getElementById('throughput').textContent = '-';
        document.getElementById('ops-per-sec').textContent = '-';
        document.getElementById('duration').textContent = '-';
    }

    document.getElementById('concurrency').textContent = total.concurrency || '-';

    // Client count
    const clients = total.clients || [];
    document.getElementById('client-count').textContent = clients.length > 0 ? clients.length : '-';

    // Show/hide errors
    if (total.total_errors > 0) {
        document.getElementById('error-card').style.display = 'block';
        document.getElementById('errors').textContent = total.total_errors;
    } else {
        document.getElementById('error-card').style.display = 'none';
    }
}

function renderThroughputChart() {
    const canvas = document.getElementById('throughput-chart');
    const ctx = canvas.getContext('2d');

    const datasets = [];
    const opTypes = Object.keys(data.by_op_type || {});

    if (opTypes.length === 0 && data.total?.throughput?.segmented?.segments) {
        // Single operation type - use total
        const segments = data.total.throughput.segmented.segments;
        datasets.push({
            label: 'Throughput',
            data: segments.map(s => ({
                x: new Date(s.start),
                y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
            })),
            borderColor: colors.primary,
            backgroundColor: colors.primary + '20',
            fill: true,
            tension: 0.3
        });
    } else {
        // Multiple operation types
        opTypes.forEach(opType => {
            const opData = data.by_op_type[opType];
            const segments = opData?.throughput?.segmented?.segments;
            if (!segments) return;

            datasets.push({
                label: opType,
                data: segments.map(s => ({
                    x: new Date(s.start),
                    y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                })),
                borderColor: getOpColor(opType),
                backgroundColor: getOpColor(opType) + '20',
                fill: false,
                tension: 0.3
            });
        });
    }

    if (datasets.length === 0) {
        canvas.parentElement.style.display = 'none';
        return;
    }

    // Show the chart container (may have been hidden previously)
    canvas.parentElement.style.display = '';

    // Determine if we're showing BPS or OPS
    const hasBPS = datasets.some(ds => ds.data.some(d => d.y > 1));
    const yLabel = hasBPS ? 'MiB/s' : 'ops/s';

    charts.throughput = new Chart(ctx, {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        displayFormats: {
                            second: 'HH:mm:ss',
                            minute: 'HH:mm'
                        }
                    },
                    title: {
                        display: true,
                        text: 'Time'
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: yLabel
                    }
                }
            },
            plugins: {
                legend: {
                    display: datasets.length > 1
                }
            }
        }
    });
}

function renderOpsChart() {
    const canvas = document.getElementById('ops-chart');
    const ctx = canvas.getContext('2d');

    const opTypes = Object.keys(data.by_op_type || {});
    if (opTypes.length === 0) {
        canvas.parentElement.style.display = 'none';
        return;
    }

    // Show the chart container (may have been hidden previously)
    canvas.parentElement.style.display = '';

    const labels = [];
    const values = [];
    const bgColors = [];

    opTypes.forEach(opType => {
        const opData = data.by_op_type[opType];
        labels.push(opType);
        values.push(opData?.total_requests || 0);
        bgColors.push(getOpColor(opType));
    });

    charts.ops = new Chart(ctx, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                label: 'Operations',
                data: values,
                backgroundColor: bgColors
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Operations'
                    }
                }
            }
        }
    });
}

function renderLatencyChart() {
    const canvas = document.getElementById('latency-chart');
    const ctx = canvas.getContext('2d');

    // Get latency time series data from total or first operation type
    let latencyTs = getOpLatencyTimeSeries(data.total);
    if (!latencyTs) {
        const opTypes = Object.keys(data.by_op_type || {});
        for (const opType of opTypes) {
            latencyTs = getOpLatencyTimeSeries(data.by_op_type[opType]);
            if (latencyTs) break;
        }
    }

    if (!latencyTs) {
        canvas.parentElement.style.display = 'none';
        return;
    }

    // Show the chart container (may have been hidden previously)
    canvas.parentElement.style.display = '';

    // Update the chart title
    canvas.parentElement.querySelector('h2').textContent = 'Request Time';

    charts.latency = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Avg',
                    data: latencyTs.segments.map(s => ({ x: s.time, y: s.avg })),
                    borderColor: '#c72c48',
                    tension: 0.3,
                    fill: false,
                    hidden: true
                },
                {
                    label: 'P50',
                    data: latencyTs.segments.map(s => ({ x: s.time, y: s.p50 })),
                    borderColor: '#10b981',
                    tension: 0.3,
                    fill: false
                },
                {
                    label: 'P90',
                    data: latencyTs.segments.map(s => ({ x: s.time, y: s.p90 })),
                    borderColor: '#f59e0b',
                    tension: 0.3,
                    fill: false,
                    hidden: true
                },
                {
                    label: 'P99',
                    data: latencyTs.segments.map(s => ({ x: s.time, y: s.p99 })),
                    borderColor: '#ef4444',
                    tension: 0.3,
                    fill: false,
                    hidden: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: true }
            },
            scales: {
                x: {
                    type: 'time',
                    time: { displayFormats: { second: 'HH:mm:ss' } }
                },
                y: {
                    beginAtZero: true,
                    title: { display: true, text: 'ms' }
                }
            }
        }
    });
}

function getOpLatencyTimeSeries(opData) {
    if (!opData?.requests_by_client) return null;

    // Collect all segments with timestamps
    const segments = [];
    for (const clientReqs of Object.values(opData.requests_by_client)) {
        for (const seg of clientReqs) {
            const single = seg.single_sized_requests;
            if (single && !single.skipped && seg.start_time) {
                const point = {
                    time: new Date(seg.start_time),
                    avg: single.dur_avg_millis,
                    p50: single.dur_median_millis,
                    p90: single.dur_90_millis,
                    p99: single.dur_99_millis
                };
                // TTFB data
                if (single.first_byte) {
                    point.ttfbAvg = single.first_byte.average_millis || 0;
                    point.ttfbP50 = single.first_byte.median_millis || 0;
                    point.ttfbP90 = single.first_byte.p90_millis || 0;
                    point.ttfbP99 = single.first_byte.p99_millis || 0;
                }
                segments.push(point);
            }
        }
    }

    if (segments.length === 0) return null;

    // Sort by time
    segments.sort((a, b) => a.time - b.time);

    // Check if we have TTFB data
    const hasTTFB = segments.some(s => s.ttfbAvg !== undefined);

    return { segments, hasTTFB };
}

function renderOperationsDetail() {
    const container = document.getElementById('operations-detail');
    const opTypes = Object.keys(data.by_op_type || {});

    if (opTypes.length === 0) {
        container.innerHTML = '<p style="color: var(--text-muted);">No operation data available.</p>';
        return;
    }

    container.innerHTML = opTypes.map((opType, idx) => {
        const opData = data.by_op_type[opType];
        const tp = opData?.throughput;

        let bps = 0, ops = 0;
        if (tp && tp.measure_duration_millis > 0) {
            bps = (tp.bytes * 1000) / tp.measure_duration_millis;
            ops = (tp.ops * 1000) / tp.measure_duration_millis;
        }

        const latencyTs = getOpLatencyTimeSeries(opData);
        const latencyHtml = latencyTs ? `
            <div class="op-latency">
                <h3>Request Time</h3>
                <div class="op-chart">
                    <canvas id="op-latency-chart-${idx}"></canvas>
                </div>
            </div>
            ${latencyTs.hasTTFB ? `
            <div class="op-latency">
                <h3>Time To First Byte (TTFB)</h3>
                <div class="op-chart">
                    <canvas id="op-ttfb-chart-${idx}"></canvas>
                </div>
            </div>
            ` : ''}
        ` : '';

        return `
            <div class="op-section">
                <h2 style="color: ${getOpColor(opType)}">${opType}</h2>
                <div class="op-stats">
                    <div class="op-stat">
                        <div class="label">Requests</div>
                        <div class="value">${formatNumber(opData?.total_requests || 0)}</div>
                    </div>
                    <div class="op-stat">
                        <div class="label">Bytes</div>
                        <div class="value">${formatBytes(opData?.total_bytes || 0)}</div>
                    </div>
                    <div class="op-stat">
                        <div class="label">Throughput</div>
                        <div class="value">${bps > 0 ? formatBytesPerSec(bps) : ops.toFixed(2) + ' ops/s'}</div>
                    </div>
                    <div class="op-stat">
                        <div class="label">Duration</div>
                        <div class="value">${formatDuration(tp?.measure_duration_millis || 0)}</div>
                    </div>
                    <div class="op-stat">
                        <div class="label">Concurrency</div>
                        <div class="value">${opData?.concurrency || '-'}</div>
                    </div>
                    ${opData?.total_errors > 0 ? `
                    <div class="op-stat" style="background: #fee2e2;">
                        <div class="label">Errors</div>
                        <div class="value" style="color: var(--error);">${opData.total_errors}</div>
                    </div>
                    ` : ''}
                </div>
                <div class="op-chart">
                    <canvas id="op-chart-${idx}"></canvas>
                </div>
                ${latencyHtml}
            </div>
        `;
    }).join('');

    // Render individual operation charts
    opTypes.forEach((opType, idx) => {
        const opData = data.by_op_type[opType];
        const segments = opData?.throughput?.segmented?.segments;
        if (!segments) return;

        const canvas = document.getElementById(`op-chart-${idx}`);
        const ctx = canvas.getContext('2d');

        new Chart(ctx, {
            type: 'line',
            data: {
                datasets: [{
                    label: opType,
                    data: segments.map(s => ({
                        x: new Date(s.start),
                        y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                    })),
                    borderColor: getOpColor(opType),
                    backgroundColor: getOpColor(opType) + '20',
                    fill: true,
                    tension: 0.3
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: { displayFormats: { second: 'HH:mm:ss' } }
                    },
                    y: { beginAtZero: true }
                }
            }
        });

        // Render latency time series chart
        const latencyTs = getOpLatencyTimeSeries(opData);
        if (latencyTs) {
            const latencyCanvas = document.getElementById(`op-latency-chart-${idx}`);
            if (latencyCanvas) {
                const latencyCtx = latencyCanvas.getContext('2d');
                new Chart(latencyCtx, {
                    type: 'line',
                    data: {
                        datasets: [
                            {
                                label: 'Avg',
                                data: latencyTs.segments.map(s => ({ x: s.time, y: s.avg })),
                                borderColor: '#c72c48',
                                backgroundColor: '#c72c4820',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            },
                            {
                                label: 'P50',
                                data: latencyTs.segments.map(s => ({ x: s.time, y: s.p50 })),
                                borderColor: '#10b981',
                                backgroundColor: '#10b98120',
                                tension: 0.3,
                                fill: false
                            },
                            {
                                label: 'P90',
                                data: latencyTs.segments.map(s => ({ x: s.time, y: s.p90 })),
                                borderColor: '#f59e0b',
                                backgroundColor: '#f59e0b20',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            },
                            {
                                label: 'P99',
                                data: latencyTs.segments.map(s => ({ x: s.time, y: s.p99 })),
                                borderColor: '#ef4444',
                                backgroundColor: '#ef444420',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { mode: 'index', intersect: false },
                        plugins: { legend: { display: true } },
                        scales: {
                            x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                            y: { beginAtZero: true, title: { display: true, text: 'ms' } }
                        }
                    }
                });
            }

            // Render TTFB time series chart
            if (latencyTs.hasTTFB) {
                const ttfbCanvas = document.getElementById(`op-ttfb-chart-${idx}`);
                if (ttfbCanvas) {
                    const ttfbCtx = ttfbCanvas.getContext('2d');
                    new Chart(ttfbCtx, {
                        type: 'line',
                        data: {
                            datasets: [
                                {
                                    label: 'Avg',
                                    data: latencyTs.segments.filter(s => s.ttfbAvg !== undefined).map(s => ({ x: s.time, y: s.ttfbAvg })),
                                    borderColor: '#c72c48',
                                    backgroundColor: '#c72c4820',
                                    tension: 0.3,
                                    fill: false,
                                    hidden: true
                                },
                                {
                                    label: 'P50',
                                    data: latencyTs.segments.filter(s => s.ttfbP50 !== undefined).map(s => ({ x: s.time, y: s.ttfbP50 })),
                                    borderColor: '#10b981',
                                    backgroundColor: '#10b98120',
                                    tension: 0.3,
                                    fill: false
                                },
                                {
                                    label: 'P90',
                                    data: latencyTs.segments.filter(s => s.ttfbP90 !== undefined).map(s => ({ x: s.time, y: s.ttfbP90 })),
                                    borderColor: '#f59e0b',
                                    backgroundColor: '#f59e0b20',
                                    tension: 0.3,
                                    fill: false,
                                    hidden: true
                                },
                                {
                                    label: 'P99',
                                    data: latencyTs.segments.filter(s => s.ttfbP99 !== undefined).map(s => ({ x: s.time, y: s.ttfbP99 })),
                                    borderColor: '#ef4444',
                                    backgroundColor: '#ef444420',
                                    tension: 0.3,
                                    fill: false,
                                    hidden: true
                                }
                            ]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            interaction: { mode: 'index', intersect: false },
                            plugins: { legend: { display: true } },
                            scales: {
                                x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                                y: { beginAtZero: true, title: { display: true, text: 'ms' } }
                            }
                        }
                    });
                }
            }
        }
    });
}

function renderHostsTable() {
    const container = document.querySelector('#tab-hosts .table-container');

    // Show notice if benchmark is still running
    if (data.final === false) {
        container.innerHTML = '<div class="running-notice"><p>Host statistics are not available while the benchmark is running.</p><p style="color: var(--text-muted); font-size: 0.875rem;">These statistics will be populated once the benchmark completes.</p></div>';
        return;
    }

    const opTypes = Object.keys(data.by_op_type || {}).sort();
    const hosts = [...(data.total?.hosts || [])].sort();

    if (hosts.length === 0 || opTypes.length === 0) {
        container.innerHTML = '<h2>Throughput by Host</h2><p style="color: var(--text-muted);">No host data available</p>';
        return;
    }

    container.innerHTML = opTypes.map((opType, opIdx) => {
        const opData = data.by_op_type[opType];
        const byHost = opData?.throughput_by_host || {};

        return `
            <div class="op-host-section" style="margin-bottom: 2rem;">
                <h2 style="color: ${getOpColor(opType)}; margin-bottom: 1rem;">${opType}</h2>
                <table class="hosts-table-${opIdx}">
                    <thead>
                        <tr>
                            <th>Host</th>
                            <th>Operations</th>
                            <th>Bytes</th>
                            <th>Throughput</th>
                            <th>Errors</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${hosts.map((host, idx) => {
                            const hostData = byHost[host] || {};
                            const bps = hostData.measure_duration_millis > 0
                                ? (hostData.bytes * 1000) / hostData.measure_duration_millis
                                : 0;
                            const globalIdx = opIdx * 1000 + idx;

                            return `
                                <tr class="clickable-row" data-host="${host}" data-op="${opType}" data-idx="${globalIdx}">
                                    <td>${host} <span class="expand-icon">▶</span></td>
                                    <td>${formatNumber(hostData.ops || 0)}</td>
                                    <td>${formatBytes(hostData.bytes || 0)}</td>
                                    <td>${formatBytesPerSec(bps)}</td>
                                    <td>${hostData.errors || 0}</td>
                                </tr>
                                <tr class="detail-row" id="host-detail-${globalIdx}" style="display: none;">
                                    <td colspan="5">
                                        <div class="detail-content" id="host-detail-content-${globalIdx}"></div>
                                    </td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    // Add click handlers
    container.querySelectorAll('.clickable-row').forEach(row => {
        row.addEventListener('click', () => toggleHostDetail(row.dataset.host, row.dataset.idx, row.dataset.op));
    });
}

function toggleHostDetail(host, idx, opType) {
    const detailRow = document.getElementById(`host-detail-${idx}`);
    const contentDiv = document.getElementById(`host-detail-content-${idx}`);
    const clickableRow = detailRow.previousElementSibling;
    const icon = clickableRow.querySelector('.expand-icon');

    if (detailRow.style.display === 'none') {
        detailRow.style.display = 'table-row';
        icon.textContent = '▼';
        clickableRow.classList.add('expanded');
        renderHostDetail(host, contentDiv, opType);
    } else {
        detailRow.style.display = 'none';
        icon.textContent = '▶';
        clickableRow.classList.remove('expanded');
    }
}

let hostChartCounter = 0;

function renderHostDetail(host, container, opType) {
    // Get host data from the specific operation type for summary stats
    const opData = data.by_op_type?.[opType];
    const hostThroughput = opData?.throughput_by_host?.[host];

    // Get detailed host data (with segments) from by_host
    const hostDetailData = data.by_host?.[host];

    if (!hostThroughput && !hostDetailData) {
        container.innerHTML = '<p>No detailed data available for this host.</p>';
        return;
    }

    const chartId = `host-throughput-chart-${hostChartCounter++}`;
    const latencyChartId = `host-latency-chart-${hostChartCounter++}`;
    const ttfbChartId = `host-ttfb-chart-${hostChartCounter++}`;

    // Use op-specific throughput for summary, fall back to host aggregate
    const tp = hostThroughput || hostDetailData?.throughput || {};
    const bps = tp.measure_duration_millis > 0 ? (tp.bytes * 1000) / tp.measure_duration_millis : 0;
    const ops = tp.measure_duration_millis > 0 ? (tp.ops * 1000) / tp.measure_duration_millis : 0;

    // Get segmented stats from host detail data (aggregate across ops for this host)
    const seg = hostDetailData?.throughput?.segmented;
    let throughputChartHtml = '';
    if (seg && seg.segments && seg.segments.length > 0) {
        throughputChartHtml = `
            <div class="detail-section">
                <h4>Throughput Over Time</h4>
                <div class="detail-chart">
                    <canvas id="${chartId}"></canvas>
                </div>
                <div class="detail-stats" style="margin-top: 0.75rem;">
                    <div class="detail-stat">
                        <span class="label">Fastest</span>
                        <span class="value">${seg.fastest_bps > 0 ? formatBytesPerSec(seg.fastest_bps) : seg.fastest_ops?.toFixed(2) + ' ops/s'}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Median</span>
                        <span class="value">${seg.median_bps > 0 ? formatBytesPerSec(seg.median_bps) : seg.median_ops?.toFixed(2) + ' ops/s'}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Slowest</span>
                        <span class="value">${seg.slowest_bps > 0 ? formatBytesPerSec(seg.slowest_bps) : seg.slowest_ops?.toFixed(2) + ' ops/s'}</span>
                    </div>
                </div>
            </div>
        `;
    }

    // Get latency time series from host detail data
    const latencyTs = getOpLatencyTimeSeries(hostDetailData);
    let latencyHtml = '';
    if (latencyTs) {
        latencyHtml = `
            <div class="detail-section">
                <h4>Request Time</h4>
                <div class="detail-chart">
                    <canvas id="${latencyChartId}"></canvas>
                </div>
            </div>
            ${latencyTs.hasTTFB ? `
            <div class="detail-section">
                <h4>Time To First Byte (TTFB)</h4>
                <div class="detail-chart">
                    <canvas id="${ttfbChartId}"></canvas>
                </div>
            </div>
            ` : ''}
        `;
    }

    container.innerHTML = `
        <div class="detail-grid">
            <div class="detail-section">
                <h4>Summary (${opType})</h4>
                <div class="detail-stats">
                    <div class="detail-stat">
                        <span class="label">Operations</span>
                        <span class="value">${formatNumber(tp.ops || 0)}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Total Bytes</span>
                        <span class="value">${formatBytes(tp.bytes || 0)}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Throughput</span>
                        <span class="value">${bps > 0 ? formatBytesPerSec(bps) : ops.toFixed(2) + ' ops/s'}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Duration</span>
                        <span class="value">${formatDuration(tp.measure_duration_millis || 0)}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Errors</span>
                        <span class="value ${tp.errors > 0 ? 'error' : ''}">${tp.errors || 0}</span>
                    </div>
                </div>
            </div>
            ${throughputChartHtml}
            ${latencyHtml}
        </div>
    `;

    // Render throughput chart
    if (seg && seg.segments && seg.segments.length > 0) {
        const canvas = document.getElementById(chartId);
        if (canvas) {
            new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: {
                    datasets: [{
                        label: host,
                        data: seg.segments.map(s => ({
                            x: new Date(s.start),
                            y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                        })),
                        borderColor: getOpColor(opType),
                        backgroundColor: getOpColor(opType) + '20',
                        fill: true,
                        tension: 0.3
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                        y: { beginAtZero: true }
                    }
                }
            });
        }
    }

    // Render latency time series chart
    if (latencyTs) {
        const latencyCanvas = document.getElementById(latencyChartId);
        if (latencyCanvas) {
            new Chart(latencyCanvas.getContext('2d'), {
                type: 'line',
                data: {
                    datasets: [
                        {
                            label: 'Avg',
                            data: latencyTs.segments.map(s => ({ x: s.time, y: s.avg })),
                            borderColor: '#c72c48',
                            tension: 0.3,
                            fill: false,
                            hidden: true
                        },
                        {
                            label: 'P50',
                            data: latencyTs.segments.map(s => ({ x: s.time, y: s.p50 })),
                            borderColor: '#10b981',
                            tension: 0.3,
                            fill: false
                        },
                        {
                            label: 'P90',
                            data: latencyTs.segments.map(s => ({ x: s.time, y: s.p90 })),
                            borderColor: '#f59e0b',
                            tension: 0.3,
                            fill: false,
                            hidden: true
                        },
                        {
                            label: 'P99',
                            data: latencyTs.segments.map(s => ({ x: s.time, y: s.p99 })),
                            borderColor: '#ef4444',
                            tension: 0.3,
                            fill: false,
                            hidden: true
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { display: true } },
                    scales: {
                        x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                        y: { beginAtZero: true, title: { display: true, text: 'ms' } }
                    }
                }
            });
        }

        // Render TTFB time series chart
        if (latencyTs.hasTTFB) {
            const ttfbCanvas = document.getElementById(ttfbChartId);
            if (ttfbCanvas) {
                new Chart(ttfbCanvas.getContext('2d'), {
                    type: 'line',
                    data: {
                        datasets: [
                            {
                                label: 'Avg',
                                data: latencyTs.segments.filter(s => s.ttfbAvg !== undefined).map(s => ({ x: s.time, y: s.ttfbAvg })),
                                borderColor: '#c72c48',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            },
                            {
                                label: 'P50',
                                data: latencyTs.segments.filter(s => s.ttfbP50 !== undefined).map(s => ({ x: s.time, y: s.ttfbP50 })),
                                borderColor: '#10b981',
                                tension: 0.3,
                                fill: false
                            },
                            {
                                label: 'P90',
                                data: latencyTs.segments.filter(s => s.ttfbP90 !== undefined).map(s => ({ x: s.time, y: s.ttfbP90 })),
                                borderColor: '#f59e0b',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            },
                            {
                                label: 'P99',
                                data: latencyTs.segments.filter(s => s.ttfbP99 !== undefined).map(s => ({ x: s.time, y: s.ttfbP99 })),
                                borderColor: '#ef4444',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { mode: 'index', intersect: false },
                        plugins: { legend: { display: true } },
                        scales: {
                            x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                            y: { beginAtZero: true, title: { display: true, text: 'ms' } }
                        }
                    }
                });
            }
        }
    }
}

function renderClientsTable() {
    const container = document.querySelector('#tab-clients .table-container');

    // Show notice if benchmark is still running
    if (data.final === false) {
        container.innerHTML = '<div class="running-notice"><p>Client statistics are not available while the benchmark is running.</p><p style="color: var(--text-muted); font-size: 0.875rem;">These statistics will be populated once the benchmark completes.</p></div>';
        return;
    }

    const opTypes = Object.keys(data.by_op_type || {}).sort();
    const clients = [...(data.total?.clients || [])].sort();

    if (clients.length === 0 || opTypes.length === 0) {
        container.innerHTML = '<h2>Throughput by Client</h2><p style="color: var(--text-muted);">No client data available</p>';
        return;
    }

    container.innerHTML = opTypes.map((opType, opIdx) => {
        const opData = data.by_op_type[opType];
        const byClient = opData?.throughput_by_client || {};

        return `
            <div class="op-client-section" style="margin-bottom: 2rem;">
                <h2 style="color: ${getOpColor(opType)}; margin-bottom: 1rem;">${opType}</h2>
                <table class="clients-table-${opIdx}">
                    <thead>
                        <tr>
                            <th>Client</th>
                            <th>Operations</th>
                            <th>Bytes</th>
                            <th>Throughput</th>
                            <th>Errors</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${clients.map((client, idx) => {
                            const clientData = byClient[client] || {};
                            const bps = clientData.measure_duration_millis > 0
                                ? (clientData.bytes * 1000) / clientData.measure_duration_millis
                                : 0;
                            const globalIdx = opIdx * 1000 + idx;

                            return `
                                <tr class="clickable-row" data-client="${client}" data-op="${opType}" data-idx="${globalIdx}">
                                    <td>${client} <span class="expand-icon">▶</span></td>
                                    <td>${formatNumber(clientData.ops || 0)}</td>
                                    <td>${formatBytes(clientData.bytes || 0)}</td>
                                    <td>${formatBytesPerSec(bps)}</td>
                                    <td>${clientData.errors || 0}</td>
                                </tr>
                                <tr class="detail-row" id="client-detail-${globalIdx}" style="display: none;">
                                    <td colspan="5">
                                        <div class="detail-content" id="client-detail-content-${globalIdx}"></div>
                                    </td>
                                </tr>
                            `;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        `;
    }).join('');

    // Add click handlers
    container.querySelectorAll('.clickable-row').forEach(row => {
        row.addEventListener('click', () => toggleClientDetail(row.dataset.client, row.dataset.idx, row.dataset.op));
    });
}

function toggleClientDetail(client, idx, opType) {
    const detailRow = document.getElementById(`client-detail-${idx}`);
    const contentDiv = document.getElementById(`client-detail-content-${idx}`);
    const clickableRow = detailRow.previousElementSibling;
    const icon = clickableRow.querySelector('.expand-icon');

    if (detailRow.style.display === 'none') {
        detailRow.style.display = 'table-row';
        icon.textContent = '▼';
        clickableRow.classList.add('expanded');
        renderClientDetail(client, contentDiv, opType);
    } else {
        detailRow.style.display = 'none';
        icon.textContent = '▶';
        clickableRow.classList.remove('expanded');
    }
}

let clientChartCounter = 0;

function renderClientDetail(client, container, opType) {
    // Get client data from the specific operation type
    const opData = data.by_op_type?.[opType];
    const clientThroughput = opData?.throughput_by_client?.[client];

    if (!clientThroughput) {
        container.innerHTML = '<p>No detailed data available for this client.</p>';
        return;
    }

    const chartId = `client-throughput-chart-${clientChartCounter++}`;
    const latencyChartId = `client-latency-chart-${clientChartCounter++}`;
    const ttfbChartId = `client-ttfb-chart-${clientChartCounter++}`;

    const tp = clientThroughput;
    const bps = tp.measure_duration_millis > 0 ? (tp.bytes * 1000) / tp.measure_duration_millis : 0;
    const ops = tp.measure_duration_millis > 0 ? (tp.ops * 1000) / tp.measure_duration_millis : 0;

    // Get segmented stats
    const seg = tp.segmented;
    let throughputChartHtml = '';
    if (seg && seg.segments && seg.segments.length > 0) {
        throughputChartHtml = `
            <div class="detail-section">
                <h4>Throughput Over Time</h4>
                <div class="detail-chart">
                    <canvas id="${chartId}"></canvas>
                </div>
                <div class="detail-stats" style="margin-top: 0.75rem;">
                    <div class="detail-stat">
                        <span class="label">Fastest</span>
                        <span class="value">${seg.fastest_bps > 0 ? formatBytesPerSec(seg.fastest_bps) : seg.fastest_ops?.toFixed(2) + ' ops/s'}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Median</span>
                        <span class="value">${seg.median_bps > 0 ? formatBytesPerSec(seg.median_bps) : seg.median_ops?.toFixed(2) + ' ops/s'}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Slowest</span>
                        <span class="value">${seg.slowest_bps > 0 ? formatBytesPerSec(seg.slowest_bps) : seg.slowest_ops?.toFixed(2) + ' ops/s'}</span>
                    </div>
                </div>
            </div>
        `;
    }

    // Get latency time series data for this client from operation data
    let latencyHtml = '';
    const clientRequests = opData?.requests_by_client?.[client];
    if (clientRequests && clientRequests.length > 0) {
        // Extract time series latency data
        const segments = [];
        for (const seg of clientRequests) {
            const single = seg.single_sized_requests;
            if (single && !single.skipped && seg.start_time) {
                const point = {
                    time: new Date(seg.start_time),
                    avg: single.dur_avg_millis,
                    p50: single.dur_median_millis,
                    p90: single.dur_90_millis,
                    p99: single.dur_99_millis
                };
                if (single.first_byte) {
                    point.ttfbAvg = single.first_byte.average_millis || 0;
                    point.ttfbP50 = single.first_byte.median_millis || 0;
                    point.ttfbP90 = single.first_byte.p90_millis || 0;
                    point.ttfbP99 = single.first_byte.p99_millis || 0;
                }
                segments.push(point);
            }
        }
        segments.sort((a, b) => a.time - b.time);
        const hasTTFB = segments.some(s => s.ttfbAvg !== undefined);

        if (segments.length > 0) {
            latencyHtml = `
                <div class="detail-section">
                    <h4>Request Time</h4>
                    <div class="detail-chart">
                        <canvas id="${latencyChartId}"></canvas>
                    </div>
                </div>
                ${hasTTFB ? `
                <div class="detail-section">
                    <h4>Time To First Byte (TTFB)</h4>
                    <div class="detail-chart">
                        <canvas id="${ttfbChartId}"></canvas>
                    </div>
                </div>
                ` : ''}
            `;
        }
    }

    container.innerHTML = `
        <div class="detail-grid">
            <div class="detail-section">
                <h4>Summary</h4>
                <div class="detail-stats">
                    <div class="detail-stat">
                        <span class="label">Operations</span>
                        <span class="value">${formatNumber(tp.ops || 0)}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Total Bytes</span>
                        <span class="value">${formatBytes(tp.bytes || 0)}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Throughput</span>
                        <span class="value">${bps > 0 ? formatBytesPerSec(bps) : ops.toFixed(2) + ' ops/s'}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Duration</span>
                        <span class="value">${formatDuration(tp.measure_duration_millis || 0)}</span>
                    </div>
                    <div class="detail-stat">
                        <span class="label">Errors</span>
                        <span class="value ${tp.errors > 0 ? 'error' : ''}">${tp.errors || 0}</span>
                    </div>
                </div>
            </div>
            ${throughputChartHtml}
            ${latencyHtml}
        </div>
    `;

    // Render throughput chart
    if (seg && seg.segments && seg.segments.length > 0) {
        const canvas = document.getElementById(chartId);
        if (canvas) {
            new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: {
                    datasets: [{
                        label: client,
                        data: seg.segments.map(s => ({
                            x: new Date(s.start),
                            y: s.bytes_per_sec > 0 ? s.bytes_per_sec / (1024 * 1024) : s.obj_per_sec
                        })),
                        borderColor: getOpColor(opType),
                        backgroundColor: getOpColor(opType) + '20',
                        fill: true,
                        tension: 0.3
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: false } },
                    scales: {
                        x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                        y: { beginAtZero: true }
                    }
                }
            });
        }
    }

    // Render latency time series chart for this client
    if (clientRequests && clientRequests.length > 0) {
        const segments = [];
        for (const seg of clientRequests) {
            const single = seg.single_sized_requests;
            if (single && !single.skipped && seg.start_time) {
                const point = {
                    time: new Date(seg.start_time),
                    avg: single.dur_avg_millis,
                    p50: single.dur_median_millis,
                    p90: single.dur_90_millis,
                    p99: single.dur_99_millis
                };
                if (single.first_byte) {
                    point.ttfbAvg = single.first_byte.average_millis || 0;
                    point.ttfbP50 = single.first_byte.median_millis || 0;
                    point.ttfbP90 = single.first_byte.p90_millis || 0;
                    point.ttfbP99 = single.first_byte.p99_millis || 0;
                }
                segments.push(point);
            }
        }
        segments.sort((a, b) => a.time - b.time);
        const hasTTFB = segments.some(s => s.ttfbAvg !== undefined);

        if (segments.length > 0) {
            const latencyCanvas = document.getElementById(latencyChartId);
            if (latencyCanvas) {
                new Chart(latencyCanvas.getContext('2d'), {
                    type: 'line',
                    data: {
                        datasets: [
                            {
                                label: 'Avg',
                                data: segments.map(s => ({ x: s.time, y: s.avg })),
                                borderColor: '#c72c48',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            },
                            {
                                label: 'P50',
                                data: segments.map(s => ({ x: s.time, y: s.p50 })),
                                borderColor: '#10b981',
                                tension: 0.3,
                                fill: false
                            },
                            {
                                label: 'P90',
                                data: segments.map(s => ({ x: s.time, y: s.p90 })),
                                borderColor: '#f59e0b',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            },
                            {
                                label: 'P99',
                                data: segments.map(s => ({ x: s.time, y: s.p99 })),
                                borderColor: '#ef4444',
                                tension: 0.3,
                                fill: false,
                                hidden: true
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { mode: 'index', intersect: false },
                        plugins: { legend: { display: true } },
                        scales: {
                            x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                            y: { beginAtZero: true, title: { display: true, text: 'ms' } }
                        }
                    }
                });
            }

            // Render TTFB time series chart
            if (hasTTFB) {
                const ttfbCanvas = document.getElementById(ttfbChartId);
                if (ttfbCanvas) {
                    new Chart(ttfbCanvas.getContext('2d'), {
                        type: 'line',
                        data: {
                            datasets: [
                                {
                                    label: 'Avg',
                                    data: segments.filter(s => s.ttfbAvg !== undefined).map(s => ({ x: s.time, y: s.ttfbAvg })),
                                    borderColor: '#c72c48',
                                    tension: 0.3,
                                    fill: false,
                                    hidden: true
                                },
                                {
                                    label: 'P50',
                                    data: segments.filter(s => s.ttfbP50 !== undefined).map(s => ({ x: s.time, y: s.ttfbP50 })),
                                    borderColor: '#10b981',
                                    tension: 0.3,
                                    fill: false
                                },
                                {
                                    label: 'P90',
                                    data: segments.filter(s => s.ttfbP90 !== undefined).map(s => ({ x: s.time, y: s.ttfbP90 })),
                                    borderColor: '#f59e0b',
                                    tension: 0.3,
                                    fill: false,
                                    hidden: true
                                },
                                {
                                    label: 'P99',
                                    data: segments.filter(s => s.ttfbP99 !== undefined).map(s => ({ x: s.time, y: s.ttfbP99 })),
                                    borderColor: '#ef4444',
                                    tension: 0.3,
                                    fill: false,
                                    hidden: true
                                }
                            ]
                        },
                        options: {
                            responsive: true,
                            maintainAspectRatio: false,
                            interaction: { mode: 'index', intersect: false },
                            plugins: { legend: { display: true } },
                            scales: {
                                x: { type: 'time', time: { displayFormats: { second: 'HH:mm:ss' } } },
                                y: { beginAtZero: true, title: { display: true, text: 'ms' } }
                            }
                        }
                    });
                }
            }
        }
    }
}

// Initialize
loadData();
