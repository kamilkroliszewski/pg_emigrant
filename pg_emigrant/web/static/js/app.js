/* pg_emigrant web GUI — front-end logic.
 *
 * Talks to the JSON API exposed by app.py:
 *   GET  /api/status/<db>      → monitoring data (reused from monitor._collect_db_status)
 *   GET  /api/jobs , /api/jobs/<id>
 *   POST /api/action           → enqueue a background job, returns {job_id}
 */

'use strict';

/* ── Global init (runs on every page, before page-specific init) ─────────── */
document.addEventListener('DOMContentLoaded', function () {
  M.AutoInit();          // sidenav, modals, etc. — once, for all pages
  initTheme();
});

/* ── Dark / light theme toggle ───────────────────────────────────────────── */
function initTheme() {
  applyThemeIcon();
  const toggle = document.getElementById('theme-toggle');
  if (!toggle) return;
  toggle.addEventListener('click', function () {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const next = isDark ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', next);
    try { localStorage.setItem('pg_emigrant_theme', next); } catch (e) { /* ignore */ }
    applyThemeIcon();
  });
}

function applyThemeIcon() {
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const icon = document.getElementById('theme-icon');
  const label = document.getElementById('theme-label');
  // Show the action the click will perform (the *opposite* of the current mode).
  if (icon) icon.textContent = isDark ? 'light_mode' : 'dark_mode';
  if (label) label.textContent = isDark ? 'Light' : 'Dark';
}

/* ── Generic helpers ─────────────────────────────────────────────────────── */
function esc(value) {
  if (value === null || value === undefined) return '';
  return String(value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

async function fetchJSON(url, opts) {
  const res = await fetch(url, opts);
  let data = null;
  try { data = await res.json(); } catch (e) { /* non-JSON */ }
  if (!res.ok) {
    const msg = (data && data.error) ? data.error : (res.statusText || ('HTTP ' + res.status));
    throw new Error(msg);
  }
  return data;
}

function toast(message, kind) {
  const classes = { ok: 'green darken-1', err: 'red darken-1', warn: 'orange darken-2' }[kind] || 'blue-grey darken-1';
  M.toast({ html: esc(message), classes: classes, displayLength: kind === 'err' ? 6000 : 3500 });
}

function table(headers, rows) {
  const thead = '<thead><tr>' + headers.map(h => '<th>' + esc(h) + '</th>').join('') + '</tr></thead>';
  const body = rows.map(r => {
    const cls = r._rowClass ? ' class="' + r._rowClass + '"' : '';
    const cells = r.cells.map(c => {
      if (c && typeof c === 'object') return '<td>' + (c.html || esc(c.text)) + '</td>';
      return '<td>' + esc(c) + '</td>';
    }).join('');
    return '<tr' + cls + '>' + cells + '</tr>';
  }).join('');
  return '<table class="data">' + thead + '<tbody>' + body + '</tbody></table>';
}

function badge(text, kind) {
  return '<span class="badge-status badge-' + kind + '">' + esc(text) + '</span>';
}

function sectionError(msg) {
  return '<div class="section-error"><i class="material-icons tiny">error_outline</i> ' + esc(msg) + '</div>';
}

/* ── Health computation (dashboard) ──────────────────────────────────────── */
function computeHealth(data) {
  const errs = data.errors || {};
  const slots = data.slots || [];
  const subs = data.subscription || [];
  const tables = data.tables || [];

  // Hard failure: cannot reach servers for the core sections.
  if (errs.subscription || errs.slots || errs.lag) {
    return { cls: 'err', label: 'error' };
  }
  const slotActive = slots.some(s => s.active);
  const tableMismatch = tables.some(t => t.source !== t.target);
  const lagStr = subs.length ? String(subs[0].lag || '') : '';
  const hasLag = lagStr && lagStr !== '0 bytes';

  if (slots.length === 0 || subs.length === 0) {
    return { cls: 'warn', label: 'no replication' };
  }
  if (!slotActive) return { cls: 'warn', label: 'slot inactive' };
  if (tableMismatch || hasLag) return { cls: 'warn', label: 'warning' };
  // Schema drift (DDL differences) is intentionally NOT a health signal here:
  // it means "the schema has drifted", not "replication is broken" — a
  // subscription can be fully healthy and actively streaming data while a
  // column/index/function/trigger difference sits unresolved. Folding it in
  // also produced a confusing, load-order-dependent inconsistency: drift is
  // scanned lazily per card (only once a card scrolls into view — see
  // setupDriftLazyLoading), so two equally-drifted databases could show
  // different overall colors purely because one card had been scrolled past
  // and the other hadn't. Drift still gets its own dedicated badge in the
  // card's "Drift" row (see populateDrift) — it's just not conflated with
  // "is this database actively replicating".
  return { cls: 'ok', label: 'ok' };
}

/* ── Dashboard ───────────────────────────────────────────────────────────── */
let dashTimer = null;
let driftObserver = null;
const driftLoaded = new Set();   // databases whose drift has already been scanned

// Light, cheap sections rendered for every database in a single batch request.
// Schema drift is the most expensive section, so it is NOT fetched here — it is
// loaded lazily, per card, only when a card scrolls into view (see below).
const DASH_LIGHT_SECTIONS = 'subscription,slots,lag,tables';
// Auto-refresh cadence for the dashboard.  Deliberately gentler than the other
// pages because refreshing many databases at once is comparatively heavy.
const DASH_REFRESH_MS = 15000;

function initDashboard(databases) {
  refreshDashboardLight();
  setupDriftLazyLoading();
  setupDbSearch();

  const btn = document.getElementById('refresh-all');
  if (btn) btn.addEventListener('click', () => {
    driftLoaded.clear();        // a manual refresh re-scans drift for visible cards
    refreshDashboardLight();
    scanVisibleDrift();
  });

  wireQuickActions();
  wireConfirm();               // the dashboard has its own confirm modal (bootstrap)
  const closeBtn = document.getElementById('job-panel-close');
  if (closeBtn) closeBtn.addEventListener('click', () => document.getElementById('job-panel').classList.add('hidden'));

  // Auto-refresh only reloads the light sections; drift stays lazy so enabling
  // it never triggers a scan across every database at once.
  setupAutoRefresh(refreshDashboardLight, () => dashTimer, t => { dashTimer = t; }, DASH_REFRESH_MS);
}

// One batch request for the light sections of every database.  Replaces the old
// "one fetch per database" fan-out.
async function refreshDashboardLight() {
  let statuses;
  try {
    const res = await fetchJSON('/api/status?sections=' + DASH_LIGHT_SECTIONS);
    statuses = res.statuses || [];
  } catch (e) {
    document.querySelectorAll('.db-card').forEach(card => markCardError(card, e.message));
    updateStatsSummary();
    return;
  }
  statuses.forEach(data => populateCard(data));
  updateStatsSummary();
}

// Fleet summary tiles above the grid — counts the health pill states currently
// shown on the cards, so they stay truthful as statuses (and lazy drift) arrive.
function updateStatsSummary() {
  const counts = { ok: 0, warn: 0, err: 0 };
  document.querySelectorAll('.db-card .health-pill').forEach(pill => {
    if (pill.classList.contains('ok')) counts.ok++;
    else if (pill.classList.contains('warn')) counts.warn++;
    else if (pill.classList.contains('err')) counts.err++;
  });
  const set = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  set('stat-ok', counts.ok);
  set('stat-warn', counts.warn);
  set('stat-err', counts.err);
}

/* ── Lazy drift loading ──────────────────────────────────────────────────────
 * Drift detection is expensive (dozens of queries per database), so instead of
 * scanning every database on every refresh we scan a database's drift only when
 * its card is actually visible, and only once.  With hundreds of databases this
 * keeps the load proportional to what the user is looking at. */
function setupDriftLazyLoading() {
  if (!('IntersectionObserver' in window)) {
    // No observer support: fall back to scanning drift for whatever is on screen.
    scanVisibleDrift();
    return;
  }
  driftObserver = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) loadDriftForCard(entry.target.dataset.db);
    });
  }, { rootMargin: '120px' });
  document.querySelectorAll('.db-card').forEach(card => driftObserver.observe(card));
}

async function loadDriftForCard(db) {
  if (!db || driftLoaded.has(db)) return;
  driftLoaded.add(db);
  const card = cardFor(db);
  if (card) setCardMetric(card, 'drift', '<span class="muted">scanning…</span>');
  try {
    const data = await fetchJSON('/api/status/' + encodeURIComponent(db) + '?sections=drift');
    populateDrift(data);
  } catch (e) {
    driftLoaded.delete(db);   // allow a retry on the next view / manual refresh
    const c = cardFor(db);
    if (c) setCardMetric(c, 'drift', sectionError(e.message));
  }
}

function scanVisibleDrift() {
  document.querySelectorAll('.db-card').forEach(card => {
    const rect = card.getBoundingClientRect();
    if (rect.top < window.innerHeight && rect.bottom > 0) loadDriftForCard(card.dataset.db);
  });
}

// Refresh a single card's light sections (used after a quick action on that
// card), then re-scan its drift.
async function refreshOneCard(db) {
  try {
    const data = await fetchJSON('/api/status/' + encodeURIComponent(db) + '?sections=' + DASH_LIGHT_SECTIONS);
    populateCard(data);
  } catch (e) {
    const card = cardFor(db);
    if (card) markCardError(card, e.message);
  }
  driftLoaded.delete(db);
  loadDriftForCard(db);
}

/* ── Live database filter ────────────────────────────────────────────────────
 * Instant client-side substring match: typing in the search box hides every
 * card whose database name doesn't contain the query.  Hidden cards drop out of
 * the IntersectionObserver naturally, so no drift is scanned for filtered-out
 * databases. */
function setupDbSearch() {
  const input = document.getElementById('db-search');
  if (!input) return;
  const clearBtn = document.getElementById('db-search-clear');
  const countEl = document.getElementById('db-search-count');
  const noMatch = document.getElementById('db-no-matches');
  const noMatchQ = document.getElementById('db-no-matches-q');
  const cards = Array.from(document.querySelectorAll('.db-card'));

  const apply = () => {
    const raw = input.value.trim();
    const q = raw.toLowerCase();
    let shown = 0;
    cards.forEach(card => {
      const match = !q || (card.dataset.db || '').toLowerCase().includes(q);
      card.classList.toggle('hidden', !match);
      if (match) shown++;
    });
    if (clearBtn) clearBtn.classList.toggle('hidden', !raw);
    if (countEl) countEl.textContent = raw ? (shown + ' / ' + cards.length) : '';
    if (noMatch) noMatch.classList.toggle('hidden', shown !== 0);
    if (noMatchQ) noMatchQ.textContent = raw;
  };

  const clear = () => { input.value = ''; apply(); };

  input.addEventListener('input', apply);
  input.addEventListener('keydown', e => { if (e.key === 'Escape') { clear(); input.blur(); } });
  if (clearBtn) clearBtn.addEventListener('click', () => { clear(); input.focus(); });

  // Press "/" anywhere (outside a field) to jump to the filter.
  document.addEventListener('keydown', e => {
    if (e.key !== '/' || e.ctrlKey || e.metaKey || e.altKey) return;
    const tag = (e.target.tagName || '').toLowerCase();
    if (tag === 'input' || tag === 'textarea' || e.target.isContentEditable) return;
    e.preventDefault();
    input.focus();
  });
}

function cardFor(db) {
  return document.querySelector('.db-card[data-db="' + CSS.escape(db) + '"]');
}

function setCardMetric(card, name, html) {
  const el = card.querySelector('[data-metric="' + name + '"]');
  if (el) el.innerHTML = html;
}

// The card carries a health-* class too, driving the coloured strip along its
// top edge (see .db-card::before in style.css).
function setCardHealth(card, cls) {
  card.classList.remove('health-ok', 'health-warn', 'health-err');
  if (cls) card.classList.add('health-' + cls);
}

function markCardError(card, message) {
  const pill = card.querySelector('.health-pill');
  pill.className = 'health-pill err';
  pill.querySelector('.dot').className = 'dot dot-err';
  pill.querySelector('.health-label').textContent = 'error';
  setCardHealth(card, 'err');
  setCardMetric(card, 'subscription', sectionError(message));
}

function populateCard(data) {
  const card = cardFor(data.database);
  if (!card) return;

  const health = computeHealth(data);
  const pill = card.querySelector('.health-pill');
  pill.className = 'health-pill ' + health.cls;
  pill.querySelector('.dot').className = 'dot dot-' + health.cls;
  pill.querySelector('.health-label').textContent = health.label;
  setCardHealth(card, health.cls);

  const subs = data.subscription || [];
  const slots = data.slots || [];
  const tables = data.tables || [];

  setCardMetric(card, 'subscription', subs.length ? badge('active', 'ok') : badge('none', 'warn'));
  setCardMetric(card, 'lag', esc(subs.length ? (subs[0].lag || '—') : '—'));
  if (slots.length === 0) setCardMetric(card, 'slot', badge('none', 'err'));
  else setCardMetric(card, 'slot', slots.some(s => s.active) ? badge('active', 'ok') : badge('inactive', 'warn'));
  const src = tables.reduce((a, t) => a + (t.source || 0), 0);
  const tgt = tables.reduce((a, t) => a + (t.target || 0), 0);
  setCardMetric(card, 'tables', esc(src + ' / ' + tgt));
}

function populateDrift(data) {
  const card = cardFor(data.database);
  if (!card) return;
  const err = (data.errors || {}).drift;
  if (err) { setCardMetric(card, 'drift', sectionError(err)); return; }
  const drift = data.drift || {};
  // Drift gets its own badge here — it deliberately does NOT touch the
  // card's overall health pill (see the comment in computeHealth() for why).
  setCardMetric(card, 'drift', drift.has_drift ? badge(drift.summary || 'drift', 'warn') : badge('none', 'ok'));
}

function wireQuickActions() {
  document.querySelectorAll('.quick-actions .btn-action').forEach(btn => {
    btn.addEventListener('click', () => {
      const action = btn.dataset.action;
      const db = btn.dataset.db;
      const destructive = btn.dataset.destructive === 'true';
      const label = btn.dataset.label || action;
      const run = () => runQuickAction(action, db, label, destructive);

      // Destructive quick actions (bootstrap) go through the same
      // type-the-database-name confirmation as the detail page.
      if (destructive) openConfirm(label, db, false, run);
      else run();
    });
  });
}

async function runQuickAction(action, db, label, showPanel) {
  try {
    const { job_id } = await fetchJSON('/api/action', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action, database: db })
    });
    toast('Started: ' + label + ' → ' + db);
    // Long-running operations (bootstrap) stream their logs into the panel.
    if (showPanel) openJobPanel(label + ' — ' + db);
    watchJob(job_id, {
      onUpdate: showPanel ? renderJobPanel : undefined,
      onDone: (job) => {
        if (showPanel) renderJobPanel(job);
        if (job.status === 'success') { toast((job.result && job.result.message) || 'Done', 'ok'); refreshOneCard(db); }
        else toast('Error: ' + (job.error ? job.error.split('\n').pop() : 'job failed'), 'err');
      }
    });
  } catch (e) {
    toast(e.message, 'err');
  }
}

/* ── Database detail ─────────────────────────────────────────────────────── */
let detailTimer = null;
let confirmModal = null;
let pendingAction = null;

function initDatabaseDetail(db) {
  const refresh = () => loadDetail(db);
  refresh();

  const btn = document.getElementById('refresh-db');
  if (btn) btn.addEventListener('click', refresh);

  wireActionButtons(db);
  wireConfirm();
  const closeBtn = document.getElementById('job-panel-close');
  if (closeBtn) closeBtn.addEventListener('click', () => document.getElementById('job-panel').classList.add('hidden'));

  setupAutoRefresh(refresh, () => detailTimer, t => { detailTimer = t; });
}

async function loadDetail(db) {
  let data;
  try {
    data = await fetchJSON('/api/status/' + encodeURIComponent(db));
  } catch (e) {
    document.getElementById('status-errors').innerHTML =
      '<div class="card-panel red lighten-4 red-text text-darken-4">' + esc(e.message) + '</div>';
    return;
  }
  document.getElementById('status-errors').innerHTML = '';
  renderSection('subscription', data, renderSubscription);
  renderSection('slots', data, renderSlots);
  renderSection('lag', data, renderLag);
  renderSection('tables', data, renderTables);
  renderSection('sequences', data, renderSequences);
  renderSection('drift', data, renderDrift);
}

function renderSection(name, data, renderer) {
  const card = document.querySelector('.status-section[data-section="' + name + '"] .section-body');
  if (!card) return;
  const err = (data.errors || {})[name];
  if (err) { card.innerHTML = sectionError(err); return; }
  card.innerHTML = renderer(data[name]);
}

function renderSubscription(rows) {
  if (!rows || !rows.length) return '<div class="placeholder">No subscription.</div>';
  return table(['Name', 'PID', 'Received LSN', 'Latest End', 'Lag', 'Last message'],
    rows.map(r => {
      const lag = String(r.lag || '');
      const lagCell = { html: badge(lag || '0', (lag && lag !== '0 bytes') ? 'warn' : 'ok') };
      return { cells: [r.subname, r.pid, r.received_lsn, r.latest_end_lsn, lagCell, r.last_msg_receipt_time] };
    }));
}

function renderSlots(rows) {
  if (!rows || !rows.length) return sectionError('No replication slot — replication may be broken.');
  return table(['Slot', 'Type', 'Active', 'PID', 'Restart LSN', 'Confirmed Flush', 'WAL status'],
    rows.map(r => ({
      cells: [
        r.slot_name, r.slot_type,
        { html: r.active ? badge('active', 'ok') : badge('inactive', 'err') },
        r.active_pid || '', r.restart_lsn, r.confirmed_flush_lsn,
        { html: badge(r.wal_status || '?', r.wal_status === 'lost' ? 'err' : 'info') }
      ]
    })));
}

function renderLag(rows) {
  if (!rows || !rows.length) return '<div class="placeholder">No lag data (source).</div>';
  return table(['Application', 'State', 'Sent LSN', 'Write lag', 'Flush lag', 'Replay lag'],
    rows.map(r => ({ cells: [r.application_name, r.state, r.sent_lsn, r.write_lag, r.flush_lag, r.replay_lag] })));
}

function renderTables(rows) {
  if (!rows || !rows.length) return '<div class="placeholder">No tables.</div>';
  return table(['Schema', 'Source', 'Target', 'Match'],
    rows.map(r => {
      const ok = r.source === r.target;
      return {
        _rowClass: ok ? '' : 'row-warn',
        cells: [r.schema, r.source, r.target, { html: ok ? badge('OK', 'ok') : badge('mismatch', 'warn') }]
      };
    }));
}

function renderSequences(rows) {
  if (!rows || !rows.length) return '<div class="placeholder">No sequences.</div>';
  const kind = { ok: 'ok', updated: 'info', behind: 'warn', target_ahead: 'info', missing_on_target: 'err' };
  return table(['Schema', 'Sequence', 'Source', 'Target', 'Status'],
    rows.map(r => ({
      _rowClass: (r.status === 'missing_on_target') ? 'row-err' : (r.status === 'behind' ? 'row-warn' : ''),
      cells: [r.schema, r.sequence, r.source_value, r.target_value,
        { html: badge(r.status, kind[r.status] || 'info') }]
    })));
}

function renderDrift(drift) {
  if (!drift || !drift.has_drift) return '<div class="placeholder">' +
    '<span class="badge-status badge-ok">no drift</span> Source and target schemas are in sync.</div>';
  const items = drift.items || [];
  const kind = { missing_on_target: 'warn', missing_on_source: 'err', different: 'warn' };
  const html = '<p class="muted small">' + esc(drift.summary || '') + '</p>' +
    table(['Type', 'Schema', 'Table', 'Name', 'Drift', 'Detail', 'Fix DDL'],
      items.map(it => {
        let fix = it.fix_ddl || '—';
        if (fix.length > 90) fix = fix.slice(0, 87) + '…';
        return {
          cells: [it.object_type, it.schema, it.table, it.name,
            { html: badge(it.drift_type, kind[it.drift_type] || 'info') },
            it.detail, { html: '<code>' + esc(fix) + '</code>' }]
        };
      }));
  return html;
}

/* ── Actions + confirm modal (shared by dashboard and detail page) ───────── */

// Opens the type-the-database-name confirmation and calls run(opts) once the
// user confirms.  Both pages carry the same modal markup; on a page without it
// the action would just run directly (defensive fallback).
function openConfirm(label, db, hasDropExtra, run) {
  const modalEl = document.getElementById('confirm-modal');
  if (!modalEl) { run({}); return; }
  confirmModal = M.Modal.getInstance(modalEl) || M.Modal.init(modalEl);

  pendingAction = { db, hasDropExtra, run };
  document.getElementById('confirm-title').textContent = 'Confirm: ' + label;
  document.getElementById('confirm-text').innerHTML =
    'The <strong>' + esc(label) + '</strong> operation on database <code>' + esc(db) +
    '</code> is destructive or long-running and will run in the background.';
  document.getElementById('confirm-dropextra').classList.toggle('hidden', !hasDropExtra);
  document.getElementById('opt-dropextra').checked = false;
  document.getElementById('confirm-dbname').textContent = db;
  document.getElementById('confirm-input').value = '';
  const go = document.getElementById('confirm-go');
  go.classList.add('disabled');
  document.getElementById('confirm-typebox').classList.remove('hidden');
  confirmModal.open();
}

function wireActionButtons(db) {
  document.querySelectorAll('.action-buttons .btn-action').forEach(btn => {
    btn.addEventListener('click', () => {
      const action = btn.dataset.action;
      const destructive = btn.dataset.destructive === 'true';
      const label = btn.dataset.label || action;
      const hasDropExtra = btn.dataset.dropextra === 'true';

      if (!destructive) { runDetailAction(action, db, {}); return; }
      openConfirm(label, db, hasDropExtra, (opts) => runDetailAction(action, db, opts));
    });
  });
}

function wireConfirm() {
  const input = document.getElementById('confirm-input');
  const go = document.getElementById('confirm-go');
  if (!input || !go) return;
  input.addEventListener('input', () => {
    const match = pendingAction && input.value.trim() === pendingAction.db;
    go.classList.toggle('disabled', !match);
  });
  go.addEventListener('click', () => {
    if (go.classList.contains('disabled') || !pendingAction) return;
    const opts = {};
    if (pendingAction.hasDropExtra) opts.drop_extra = document.getElementById('opt-dropextra').checked;
    confirmModal.close();
    pendingAction.run(opts);
    pendingAction = null;
  });
}

async function runDetailAction(action, db, options) {
  try {
    const { job_id } = await fetchJSON('/api/action', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action, database: db, options })
    });
    toast('Job started: ' + action);
    openJobPanel(action + ' — ' + db);
    watchJob(job_id, {
      onUpdate: renderJobPanel,
      onDone: (job) => {
        renderJobPanel(job);
        if (job.status === 'success') { toast('Finished: ' + ((job.result && job.result.message) || action), 'ok'); loadDetail(db); }
        else toast('Job failed', 'err');
      }
    });
  } catch (e) {
    toast(e.message, 'err');
  }
}

function openJobPanel(title) {
  const panel = document.getElementById('job-panel');
  if (!panel) return;
  document.getElementById('job-panel-title').textContent = title;
  document.getElementById('job-panel-log').innerHTML = '<span class="muted">Starting…</span>';
  panel.classList.remove('hidden');
}

function renderJobPanel(job) {
  const log = document.getElementById('job-panel-log');
  if (!log) return;
  log.innerHTML = formatLogs(job.logs);
  log.scrollTop = log.scrollHeight;
}

/* ── Job watching (polling) ──────────────────────────────────────────────── */
function watchJob(jobId, handlers) {
  handlers = handlers || {};
  const tick = async () => {
    let job;
    try { job = await fetchJSON('/api/jobs/' + jobId); }
    catch (e) { return; }
    if (handlers.onUpdate) handlers.onUpdate(job);
    if (job.status === 'success' || job.status === 'error') {
      if (handlers.onDone) handlers.onDone(job);
      return;
    }
    setTimeout(tick, 1200);
  };
  tick();
}

function formatLogs(logs) {
  if (!logs || !logs.length) return '<span class="muted">No logs.</span>';
  return logs.map(l => {
    const t = (l.time || '').replace('T', ' ').replace(/\+.*$/, '');
    return '<span class="log-time">' + esc(t) + '</span> ' +
      '<span class="lvl-' + esc(l.level) + '">' + esc(l.level) + '</span> ' + esc(l.message);
  }).join('\n');
}

/* ── Jobs page ───────────────────────────────────────────────────────────── */
let jobsTimer = null;
let selectedJobId = null;

function initJobs() {
  const refresh = () => loadJobs();
  refresh();
  const btn = document.getElementById('refresh-jobs');
  if (btn) btn.addEventListener('click', refresh);
  setupAutoRefresh(refresh, () => jobsTimer, t => { jobsTimer = t; });
}

async function loadJobs() {
  let data;
  try { data = await fetchJSON('/api/jobs'); }
  catch (e) { return; }
  const jobs = data.jobs || [];
  const body = document.getElementById('jobs-body');
  const empty = document.getElementById('jobs-empty');
  if (!jobs.length) {
    body.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  body.innerHTML = jobs.map(j => {
    const sel = j.id === selectedJobId ? ' selected' : '';
    const start = (j.started_at || '').replace('T', ' ').replace(/\+.*$/, '');
    const end = (j.finished_at || '').replace('T', ' ').replace(/\+.*$/, '');
    return '<tr class="job-row' + sel + '" data-job="' + esc(j.id) + '">' +
      '<td><span class="status-chip status-' + esc(j.status) + '">' + esc(j.status) + '</span></td>' +
      '<td>' + esc(j.name) + '</td>' +
      '<td>' + esc(j.database || '—') + '</td>' +
      '<td class="muted small">' + esc(start) + '</td>' +
      '<td class="muted small">' + esc(end) + '</td></tr>';
  }).join('');
  body.querySelectorAll('.job-row').forEach(row => {
    row.addEventListener('click', () => selectJob(row.dataset.job));
  });
  if (selectedJobId) {
    const cur = jobs.find(j => j.id === selectedJobId);
    if (cur) showJobDetail(cur);
  }
}

async function selectJob(jobId) {
  selectedJobId = jobId;
  document.querySelectorAll('.job-row').forEach(r => r.classList.toggle('selected', r.dataset.job === jobId));
  try { showJobDetail(await fetchJSON('/api/jobs/' + jobId)); }
  catch (e) { toast(e.message, 'err'); }
}

function showJobDetail(job) {
  document.getElementById('job-detail-title').textContent = job.name;
  document.getElementById('job-detail-meta').innerHTML =
    'State: <span class="status-' + esc(job.status) + '">' + esc(job.status) + '</span>' +
    (job.database ? ' · database <code>' + esc(job.database) + '</code>' : '');
  document.getElementById('job-detail-log').innerHTML = formatLogs(job.logs);
  const errBox = document.getElementById('job-detail-error');
  if (job.error) {
    errBox.classList.remove('hidden');
    document.getElementById('job-detail-traceback').textContent = job.error;
  } else {
    errBox.classList.add('hidden');
  }
}

/* ── Auto-refresh wiring (shared) ────────────────────────────────────────── */
function updateAutorefreshLabel(cb) {
  const el = document.getElementById('autorefresh-state');
  if (!el || !cb) return;
  el.textContent = cb.checked ? 'On' : 'Off';
  el.classList.toggle('on', cb.checked);
  el.classList.toggle('off', !cb.checked);
}

function setupAutoRefresh(refreshFn, getTimer, setTimer, intervalMs) {
  const cb = document.getElementById('autorefresh');
  const ms = intervalMs || 5000;
  const start = () => { if (!getTimer()) setTimer(setInterval(refreshFn, ms)); };
  const stop = () => { if (getTimer()) { clearInterval(getTimer()); setTimer(null); } };
  if (cb) {
    updateAutorefreshLabel(cb);          // reflect the initial (default: off) state
    if (cb.checked) start();
    cb.addEventListener('change', () => {
      cb.checked ? start() : stop();
      updateAutorefreshLabel(cb);
    });
  } else {
    start();
  }
}
