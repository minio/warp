/* Warp control-plane UI. */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

// --- Theme (dark/light), shares the key with the wui dashboard. ---
function getTheme() { return localStorage.getItem('warp-theme') || 'dark'; }
function applyTheme(t) {
    document.body.classList.toggle('light', t === 'light');
    localStorage.setItem('warp-theme', t);
    const btn = document.getElementById('theme-toggle');
    if (btn) btn.textContent = t === 'light' ? '☀' : '☾';
}
document.getElementById('theme-toggle').addEventListener('click', () => {
    applyTheme(getTheme() === 'dark' ? 'light' : 'dark');
});
applyTheme(getTheme());

function esc(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
}

function toast(msg, isErr) {
    const t = $('#toast');
    t.textContent = msg;
    t.className = 'toast show' + (isErr ? ' err' : '');
    setTimeout(() => { t.className = 'toast'; }, 2600);
}

async function api(method, path, body) {
    const opts = { method, headers: {} };
    if (body !== undefined) {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(body);
    }
    const resp = await fetch(path, opts);
    if (resp.status === 204) return null;
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.error || `HTTP ${resp.status}`);
    return data;
}

// --- Tabs ---
$$('.tab').forEach((t) => t.addEventListener('click', () => {
    $$('.tab').forEach((x) => x.classList.remove('active'));
    $$('.panel').forEach((x) => x.classList.remove('active'));
    t.classList.add('active');
    $(`#tab-${t.dataset.tab}`).classList.add('active');
    if (t.dataset.tab === 'runs') loadRuns();
}));

// --- Scenarios ---
function num(v) { return v === '' || v == null ? 0 : Number(v); }

// Advanced options: each control carries a data-flag attribute naming the warp
// flag. Checkboxes become bare boolean flags when checked; other controls are
// included only when non-empty. Everything maps into spec.extra_flags.
function collectFlags() {
    const flags = {};
    $$('[data-flag]').forEach((el) => {
        const name = el.dataset.flag;
        if (el.type === 'checkbox') {
            if (el.checked) flags[name] = '';
        } else {
            const v = el.value.trim();
            if (v) flags[name] = v;
        }
    });
    return flags;
}
function resetFlags() {
    $$('[data-flag]').forEach((el) => {
        if (el.type === 'checkbox') el.checked = false;
        else el.value = '';
    });
}

$('#scenario-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = e.target;
    const spec = {
        method: f.method.value,
        obj_size: f.obj_size.value,
        duration: f.duration.value,
        objects: num(f.objects.value),
        concurrent: num(f.concurrent.value),
        get_distrib: num(f.get_distrib.value),
        put_distrib: num(f.put_distrib.value),
        stat_distrib: num(f.stat_distrib.value),
        delete_distrib: num(f.delete_distrib.value),
        extra_flags: collectFlags(),
    };
    try {
        await api('POST', '/api/scenarios', { name: f.name.value, spec });
        f.reset();
        resetFlags();
        toast('Scenario saved');
        loadScenarios();
    } catch (err) { toast(err.message, true); }
});

let TARGETS = [];
let CLIENTS = [];

async function loadScenarios() {
    const list = await api('GET', '/api/scenarios');
    const el = $('#scenarios-list');
    if (!list.length) { el.innerHTML = '<div class="empty">No scenarios yet.</div>'; return; }
    el.innerHTML = list.map((s) => {
        const sp = s.spec;
        let summary = `${sp.method} · ${sp.obj_size} · ${sp.duration} · conc ${sp.concurrent}`;
        const extra = sp.extra_flags || {};
        const extraKeys = Object.keys(extra);
        if (extraKeys.length) {
            summary += ' · ' + extraKeys.map((k) => '--' + k + (extra[k] ? ' ' + extra[k] : '')).join(' ');
        }
        return `<div class="row">
            <div>
                <div class="title">${esc(s.name)}</div>
                <div class="meta">${esc(summary)}</div>
            </div>
            <div class="actions">
                <button class="btn small primary" data-run="${s.id}" data-name="${esc(s.name)}">Run…</button>
                <button class="btn small danger" data-del-scenario="${s.id}">Delete</button>
            </div>
        </div>`;
    }).join('');

    $$('[data-run]', el).forEach((b) => b.addEventListener('click', () => {
        openRunModal(b.dataset.run, b.dataset.name);
    }));
    $$('[data-del-scenario]', el).forEach((b) => b.addEventListener('click', async () => {
        await api('DELETE', `/api/scenarios/${b.dataset.delScenario}`);
        loadScenarios();
    }));
}

// --- Run modal: choose target + which clients to run on ---
let runScenarioId = null;

function openRunModal(scenarioId, scenarioName) {
    if (!TARGETS.length) { toast('Create a target first', true); return; }
    if (!CLIENTS.length) { toast('Add at least one client first', true); return; }
    runScenarioId = scenarioId;
    $('#run-modal-title').textContent = `Run "${scenarioName}"`;
    $('#run-target').innerHTML = TARGETS.map((t) => `<option value="${t.id}">${esc(t.name)}</option>`).join('');
    $('#run-clients').innerHTML = CLIENTS.map((c) => `
        <label class="check">
            <input type="checkbox" class="run-client" value="${c.id}" checked>
            ${esc(c.name)} <span class="meta">${esc(c.address)}</span>
            <span class="pill ${c.status || 'unknown'}">${esc(c.status || 'unknown')}</span>
        </label>`).join('');
    $('#run-modal').hidden = false;
}

function closeRunModal() {
    $('#run-modal').hidden = true;
    runScenarioId = null;
}

$('#run-cancel').addEventListener('click', closeRunModal);
$('#run-modal').addEventListener('click', (e) => { if (e.target.id === 'run-modal') closeRunModal(); });
$('#run-clients-all').addEventListener('click', () => {
    const boxes = $$('#run-clients .run-client');
    const anyUnchecked = boxes.some((b) => !b.checked);
    boxes.forEach((b) => { b.checked = anyUnchecked; });
});
$('#run-confirm').addEventListener('click', async () => {
    const targetId = $('#run-target').value;
    const clientIds = $$('#run-clients .run-client:checked').map((b) => b.value);
    if (!clientIds.length) { toast('Select at least one client', true); return; }
    try {
        await api('POST', `/api/scenarios/${runScenarioId}/run`, { target_id: targetId, client_ids: clientIds });
        toast(`Run started on ${clientIds.length} client(s) — see Runs tab`);
        closeRunModal();
    } catch (err) { toast(err.message, true); }
});

// --- Targets ---
$('#target-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = e.target;
    try {
        await api('POST', '/api/targets', {
            name: f.name.value, endpoint: f.endpoint.value, bucket: f.bucket.value,
            region: f.region.value, access_key: f.access_key.value,
            secret_key: f.secret_key.value, tls: f.tls.checked,
        });
        f.reset();
        toast('Target saved');
        await loadTargets();
        loadScenarios();
    } catch (err) { toast(err.message, true); }
});

async function loadTargets() {
    TARGETS = await api('GET', '/api/targets');
    const el = $('#targets-list');
    if (!TARGETS.length) { el.innerHTML = '<div class="empty">No targets yet.</div>'; return; }
    el.innerHTML = TARGETS.map((t) => `<div class="row">
        <div>
            <div class="title">${esc(t.name)}</div>
            <div class="meta">${esc(t.endpoint)} · ${esc(t.bucket)} ${t.tls ? '· TLS' : ''}</div>
        </div>
        <div class="actions">
            <span class="pill ${t.status || 'unknown'}" data-status="${t.id}">${esc(t.status || 'unknown')}</span>
            <button class="btn small" data-check-target="${t.id}">Check</button>
            <button class="btn small danger" data-del-target="${t.id}">Delete</button>
        </div>
    </div>`).join('');
    $$('[data-check-target]', el).forEach((b) => b.addEventListener('click', async () => {
        b.disabled = true; b.textContent = 'Checking…';
        try {
            const res = await api('POST', `/api/targets/${b.dataset.checkTarget}/check`);
            const pill = $(`[data-status="${b.dataset.checkTarget}"]`);
            if (pill) { pill.className = 'pill ' + res.status; pill.textContent = res.status; }
            if (res.ok) toast('S3 connection OK');
            else toast('S3 check failed: ' + res.error, true);
        } catch (err) { toast(err.message, true); }
        b.disabled = false; b.textContent = 'Check';
    }));
    $$('[data-del-target]', el).forEach((b) => b.addEventListener('click', async () => {
        await api('DELETE', `/api/targets/${b.dataset.delTarget}`);
        await loadTargets(); loadScenarios();
    }));
}

// --- Clients ---
$('#client-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const f = e.target;
    try {
        await api('POST', '/api/clients', { name: f.name.value, address: f.address.value });
        f.reset();
        toast('Client added');
        loadClients();
    } catch (err) { toast(err.message, true); }
});

async function loadClients() {
    CLIENTS = await api('GET', '/api/clients');
    const el = $('#clients-list');
    if (!CLIENTS.length) { el.innerHTML = '<div class="empty">No clients yet.</div>'; return; }
    el.innerHTML = CLIENTS.map((c) => `<div class="row">
        <div>
            <div class="title">${esc(c.name)}</div>
            <div class="meta">${esc(c.address)}</div>
        </div>
        <div class="actions">
            <span class="pill ${c.status || 'unknown'}">${esc(c.status || 'unknown')}</span>
            <button class="btn small" data-check="${c.id}">Check</button>
            <button class="btn small danger" data-del-client="${c.id}">Delete</button>
        </div>
    </div>`).join('');
    $$('[data-check]', el).forEach((b) => b.addEventListener('click', async () => {
        try { await api('POST', `/api/clients/${b.dataset.check}/check`); loadClients(); }
        catch (err) { toast(err.message, true); }
    }));
    $$('[data-del-client]', el).forEach((b) => b.addEventListener('click', async () => {
        await api('DELETE', `/api/clients/${b.dataset.delClient}`);
        loadClients();
    }));
}

// --- Runs ---
let lastRunsSig = '';

async function loadRuns() {
    const list = await api('GET', '/api/runs');
    const el = $('#runs-list');
    if (!list.length) { el.innerHTML = '<div class="empty">No runs yet.</div>'; lastRunsSig = ''; return; }

    // Skip re-rendering when nothing changed, so polling never clobbers the
    // user's in-progress dropdown selections in the compare bar.
    const sig = list.map((r) => `${r.id}:${r.status}`).join('|');
    if (sig === lastRunsSig && el.children.length) return;
    lastRunsSig = sig;

    const viewable = list.filter((r) => (r.status === 'done' || r.status === 'degraded') && r.result_file);

    // Compare bar: pick two finished runs and open the comparison dashboard.
    // Preserve the current selections across re-renders; default to the two most
    // recent runs so they differ out of the box.
    let compareBar = '';
    if (viewable.length >= 2) {
        const prevBefore = $('#cmp-before')?.value || viewable[0].id;
        const prevAfter = $('#cmp-after')?.value || viewable[1].id;
        const optsFor = (sel) => viewable.map((r) =>
            `<option value="${r.id}" ${r.id === sel ? 'selected' : ''}>${esc(r.scenario_name)} · ${new Date(r.started_at).toLocaleString()}</option>`).join('');
        compareBar = `<div class="card form" style="margin-bottom:16px">
            <h3>Compare two runs</h3>
            <div class="grid">
                <label>Before<select id="cmp-before">${optsFor(prevBefore)}</select></label>
                <label>After<select id="cmp-after">${optsFor(prevAfter)}</select></label>
            </div>
            <button class="btn primary" id="cmp-go">Compare</button>
        </div>`;
    }

    const rows = list.map((r) => {
        const when = new Date(r.started_at).toLocaleString();
        const clients = (r.client_addrs || []).length;
        const detail = r.error ? esc(r.error) : (r.result_file ? esc(r.result_file) : '');
        const canView = (r.status === 'done' || r.status === 'degraded') && r.result_file;
        const viewBtn = canView ? `<button class="btn small primary" data-view="${r.id}">View results</button>` : '';
        return `<div class="row">
            <div>
                <div class="title">${esc(r.scenario_name)} → ${esc(r.target_name)}</div>
                <div class="meta">${esc(when)} · ${clients} client(s)${detail ? ' · ' + detail : ''}</div>
            </div>
            <div class="actions">${viewBtn}<span class="pill ${r.status}">${esc(r.status)}</span></div>
        </div>`;
    }).join('');
    el.innerHTML = compareBar + rows;

    // View opens the wui dashboard, served through this same server under /dash/.
    $$('[data-view]', el).forEach((b) => b.addEventListener('click', () => {
        window.open(`/dash/?run=${encodeURIComponent(b.dataset.view)}`, '_blank');
    }));

    const go = $('#cmp-go', el);
    if (go) go.addEventListener('click', () => {
        const before = $('#cmp-before', el).value;
        const after = $('#cmp-after', el).value;
        if (before === after) { toast('Pick two different runs', true); return; }
        window.open(`/dash/compare.html?before=${encodeURIComponent(before)}&after=${encodeURIComponent(after)}`, '_blank');
    });
}
$('#refresh-runs').addEventListener('click', loadRuns);

// --- Init ---
async function init() {
    try {
        await loadTargets();
        await loadClients();
        await loadScenarios();
        await loadRuns();
    } catch (err) { toast(err.message, true); }
    // Light polling so in-progress runs update.
    setInterval(() => { if ($('#tab-runs').classList.contains('active')) loadRuns(); }, 3000);
}
init();
