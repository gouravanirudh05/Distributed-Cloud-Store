// Object Store Console - talks to the controller's REST API (/api/v1/..., via the gateway) and to the
// demo runner (/runner/...) for the infrastructure panel and the demo scenario buttons; both proxied by nginx.
"use strict";

const API = "/api/v1";
const PAGE_SIZE = 15;
const state = { page: 0, totalPages: 1, workers: [], detailId: null, detailMeta: null, metaDirty: false };

// ---------------------------------------------------------------- helpers

const $ = (id) => document.getElementById(id);

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children) if (c != null) node.append(c);
  return node;
}

async function api(path, options = {}) {
  const res = await fetch(API + path, options);
  if (res.status === 204) return null;
  const text = await res.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch { /* not JSON */ }
  if (!res.ok) {
    const msg = body && body.message ? body.message : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return body;
}

function human(bytes) {
  if (bytes == null || bytes < 0) return "–";
  if (bytes < 1024) return bytes + " B";
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let v = bytes, u = -1;
  while (v >= 1024 && u < units.length - 1) { v /= 1024; u++; }
  return v.toFixed(v >= 100 ? 0 : 1) + " " + units[u];
}

function timeOf(iso) {
  if (!iso) return "–";
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function dateTime(iso) {
  if (!iso) return "–";
  return new Date(iso).toLocaleString();
}

let toastTimer;
function toast(message, isError = false) {
  const t = $("toast");
  t.textContent = message;
  t.className = "toast show" + (isError ? " error" : "");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { t.className = "toast"; }, isError ? 6000 : 3500);
}

const STATUS_CLASS = {
  HEALTHY: "ok", UP: "ok", AVAILABLE: "ok",
  DEGRADED: "warn", RECOVERING: "warn", OVER_REPLICATED: "info", UPLOADING: "info",
  UNAVAILABLE: "bad", DOWN: "bad", DELETING: "warn",
};
const pill = (text) => el("span", { class: "pill " + (STATUS_CLASS[text] || "neutral"), text });

const EVENT_CLASS = {
  WORKER_DOWN: "bad", CHUNK_CORRUPTED: "bad", CHUNK_MISSING: "bad", CHUNK_LOST: "bad", CHUNK_UNAVAILABLE: "bad",
  UPLOAD_FAILED: "bad", WORKER_RECOVERING: "warn", WORKER_REGISTERED: "info", CHUNK_UNDER_REPLICATED: "warn",
  UPLOAD_DEGRADED: "warn", WORKER_UP: "ok", CHUNK_REPAIRED: "ok", CHUNK_RE_REPLICATED: "ok",
  REPLICATION_RESTORED: "ok", OBJECT_UPLOADED: "info", CHUNK_SURPLUS_REMOVED: "info", SCRUB_COMPLETED: "info",
  CONTROLLER_FAILOVER: "warn", CONTROLLER_ELECTED: "info", METADATA_STORE_RECOVERED: "warn", WORKER_REJECTED: "bad",
  OBJECT_DELETED: "neutral", METADATA_UPDATED: "info", ORPHAN_REMOVED: "neutral",
};

// ---------------------------------------------------------------- cluster

async function refreshCluster() {
  try {
    const [workers, rep, events, ctl] = await Promise.all([
      api("/admin/workers"), api("/admin/replication?limit=0"), api("/admin/events?limit=60"),
      api("/admin/controller").catch(() => null),
    ]);
    $("active-controller").textContent = ctl ? `${ctl.instanceId} · term ${ctl.term}` : "?";
    state.workers = workers;
    const up = workers.filter((w) => w.status === "UP").length;
    $("stat-workers").textContent = `${up} / ${workers.length}`;
    $("stat-workers-sub").textContent = up === workers.length ? "all heartbeating" :
      workers.filter((w) => w.status !== "UP").map((w) => `${w.workerId} ${w.status}`).join(", ");
    renderWorkers(workers);
    renderReplication(rep);
    renderEvents(events);
  } catch (e) {
    $("active-controller").textContent = "none (failover?)";
    $("cluster-status").replaceWith(Object.assign(pill("CONTROLLER UNREACHABLE"), { id: "cluster-status", className: "pill bad" }));
  }
}

function renderWorkers(workers) {
  const box = $("workers");
  box.replaceChildren(...workers.map((w) => {
    const dl = el("dl", {},
      el("dt", { text: "Chunk copies" }), el("dd", { text: String(w.replicaCount) }),
      el("dt", { text: "Files on disk" }), el("dd", { text: w.reportedChunkCount ?? "–" }),
      el("dt", { text: "Disk used" }), el("dd", { text: human(w.reportedUsedBytes) }),
      el("dt", { text: "Heartbeat" }), el("dd", { text: w.secondsSinceHeartbeat != null ? w.secondsSinceHeartbeat + " s ago" : "–" }),
    );
    let note = null;
    if (w.status === "DOWN" && w.reReplicationAt) {
      const secs = Math.round((new Date(w.reReplicationAt) - Date.now()) / 1000);
      note = el("p", { class: "note", text: secs > 0 ? `Re-replication of its chunks in ${secs} s` : "Its chunks are being re-replicated" });
    } else if (w.status === "RECOVERING") {
      note = el("p", { class: "note", text: "Verifying stored chunks…" });
    }
    return el("div", { class: "worker " + w.status },
      el("div", { class: "worker-head" }, el("span", { class: "worker-name", text: w.workerId }), pill(w.status)),
      dl, note);
  }));
}

function renderReplication(rep) {
  $("rf").textContent = rep.replicationFactor;
  const status = rep.totalChunks === 0 ? "HEALTHY" : rep.status;
  const p = pill(status);
  p.id = "cluster-status";
  $("cluster-status").replaceWith(p);
  $("stat-replication").replaceChildren(pill(status));
  $("stat-chunks").textContent = rep.totalChunks.toLocaleString();
  $("stat-copies").textContent = `× ${rep.replicationFactor} copies on different workers`;
  const problems = [
    rep.underReplicatedChunks && `${rep.underReplicatedChunks} under-replicated`,
    rep.overReplicatedChunks && `${rep.overReplicatedChunks} over-replicated`,
    rep.unavailableChunks && `${rep.unavailableChunks} unavailable`,
  ].filter(Boolean);
  $("replication-line").textContent = problems.length ? problems.join(" · ")
    : `every chunk has exactly ${rep.replicationFactor} live copies`;
}

function renderEvents(events) {
  $("events").replaceChildren(...events.map((e) => el("li", {},
    el("div", { class: "ev-head" },
      el("span", { class: "pill " + (EVENT_CLASS[e.type] || "neutral"), text: e.type }),
      el("time", { datetime: e.timestamp, text: timeOf(e.timestamp) })),
    el("div", { text: e.message }))));
}

$("scrub-btn").addEventListener("click", async (ev) => {
  const btn = ev.currentTarget;
  btn.disabled = true;
  $("scrub-result").textContent = "Scanning…";
  try {
    const r = await api("/admin/scrub", { method: "POST" });
    const text = `Scanned ${r.workersScanned} workers in ${r.durationMs} ms: ${r.chunksVerified} copies verified, ` +
      `${r.corruptedFound} corrupted, ${r.missingFound} missing, ${r.repaired} repaired, ${r.dropped} dropped, ` +
      `${r.unrepairable} unrepairable, ${r.orphansRemoved} orphans removed.`;
    $("scrub-result").textContent = text;
    toast(r.corruptedFound + r.missingFound > 0 ? "Integrity scan found and handled problems" : "Integrity scan: all copies valid");
    refreshCluster();
    if (state.detailId) openDetail(state.detailId);
  } catch (e) {
    $("scrub-result").textContent = "";
    toast("Scan failed: " + e.message, true);
  } finally {
    btn.disabled = false;
  }
});

// ---------------------------------------------------------------- objects

async function refreshObjects() {
  try {
    const page = await api(`/objects?page=${state.page}&size=${PAGE_SIZE}`);
    state.totalPages = Math.max(1, page.totalPages);
    if (state.page >= state.totalPages && state.page > 0) { state.page = state.totalPages - 1; return refreshObjects(); }
    $("objects-count").textContent = `${page.totalItems} object${page.totalItems === 1 ? "" : "s"}`;
    $("stat-objects").textContent = page.totalItems.toLocaleString();
    $("page-label").textContent = `page ${state.page + 1} / ${state.totalPages}`;
    $("prev").disabled = state.page === 0;
    $("next").disabled = state.page >= state.totalPages - 1;
    const body = $("objects-body");
    if (page.items.length === 0) {
      body.replaceChildren(el("tr", {}, el("td", { colspan: "5", class: "empty", text: "No objects yet — upload one above." })));
      return;
    }
    body.replaceChildren(...page.items.map((o) => el("tr", { onclick: () => openDetail(o.objectId), title: o.objectId },
      el("td", { class: "name", text: o.name }),
      el("td", { class: "num", text: human(o.size) }),
      el("td", { text: o.contentType }),
      el("td", { class: "num", text: String(o.chunkCount) }),
      el("td", { text: dateTime(o.createdAt) }))));
  } catch (e) {
    $("objects-body").replaceChildren(el("tr", {}, el("td", { colspan: "5", class: "empty", text: "Could not load objects: " + e.message })));
  }
}

$("prev").addEventListener("click", () => { if (state.page > 0) { state.page--; refreshObjects(); } });
$("next").addEventListener("click", () => { if (state.page < state.totalPages - 1) { state.page++; refreshObjects(); } });

// ---------------------------------------------------------------- metadata editor rows

function metaRow(container, key = "", value = "") {
  const row = el("div", { class: "meta-row" },
    el("input", { placeholder: "key", value: key, "aria-label": "metadata key" }),
    el("input", { placeholder: "value", value: value, "aria-label": "metadata value" }),
    el("button", {
      type: "button", class: "ghost", title: "Remove", "aria-label": "Remove key", text: "✕",
      onclick: () => { row.remove(); container.dispatchEvent(new Event("input", { bubbles: true })); },
    }));
  container.append(row);
  return row;
}

function readMetaRows(container) {
  const out = {};
  for (const row of container.querySelectorAll(".meta-row")) {
    const [k, v] = row.querySelectorAll("input");
    const key = k.value.trim();
    if (key) out[key] = v.value;
  }
  return out;
}

// ---------------------------------------------------------------- upload

const fileInput = $("file");
const drop = $("drop");
fileInput.addEventListener("change", () => {
  const f = fileInput.files[0];
  $("drop-text").textContent = f ? `${f.name} (${human(f.size)})` : "Choose a file or drop it here";
});
["dragenter", "dragover"].forEach((t) => drop.addEventListener(t, (e) => { e.preventDefault(); drop.classList.add("drag"); }));
["dragleave", "drop"].forEach((t) => drop.addEventListener(t, (e) => { e.preventDefault(); drop.classList.remove("drag"); }));
drop.addEventListener("drop", (e) => {
  if (e.dataTransfer.files.length) {
    fileInput.files = e.dataTransfer.files;
    fileInput.dispatchEvent(new Event("change"));
  }
});
$("add-upload-meta").addEventListener("click", () => metaRow($("upload-meta")).querySelector("input").focus());

$("upload-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const f = fileInput.files[0];
  if (!f) return toast("Choose a file first", true);
  const form = new FormData();
  const meta = readMetaRows($("upload-meta"));
  if (Object.keys(meta).length) form.append("metadata", JSON.stringify(meta));
  form.append("file", f, f.name);

  const xhr = new XMLHttpRequest();
  const bar = $("progress-bar");
  $("progress").hidden = false;
  bar.style.width = "0%";
  $("upload-btn").disabled = true;
  xhr.upload.onprogress = (ev) => { if (ev.lengthComputable) bar.style.width = (ev.loaded / ev.total * 100).toFixed(1) + "%"; };
  xhr.onloadend = () => {
    $("upload-btn").disabled = false;
    $("progress").hidden = true;
    let body = null;
    try { body = JSON.parse(xhr.responseText); } catch { /* ignore */ }
    if (xhr.status === 201) {
      toast(`Uploaded ${body.name}: ${body.chunkCount} chunks, replication ${body.replication.status}`);
      e.target.reset();
      $("upload-meta").replaceChildren();
      $("drop-text").textContent = "Choose a file or drop it here";
      state.page = 0;
      refreshObjects();
      refreshCluster();
      openDetail(body.objectId);
    } else {
      toast("Upload failed: " + (body && body.message ? body.message : "HTTP " + xhr.status), true);
    }
  };
  xhr.open("POST", API + "/objects");
  xhr.send(form);
});

// ---------------------------------------------------------------- detail panel

async function openDetail(id) {
  try {
    const m = await api(`/objects/${encodeURIComponent(id)}/metadata`);
    if (state.detailId !== id) state.metaDirty = false;
    state.detailId = id;
    state.detailMeta = m;
    renderDetail(m);
    $("detail").hidden = false;
    if (location.hash !== "#object=" + id) history.replaceState(null, "", "#object=" + id);
  } catch (e) {
    toast(e.message, true);
    if (state.detailId === id) closeDetail();
  }
}

function closeDetail() {
  if (location.hash.startsWith("#object=")) history.replaceState(null, "", location.pathname);
  $("detail").hidden = true;
  state.detailId = null;
  state.detailMeta = null;
}

function renderDetail(m) {
  $("detail-name").textContent = m.name;
  $("detail-id").textContent = m.objectId;
  $("download-link").href = `${API}/objects/${encodeURIComponent(m.objectId)}`;
  $("download-link").setAttribute("download", m.name);

  const rows = [
    ["Size", `${m.size.toLocaleString()} bytes (${human(m.size)})`],
    ["Content type", m.contentType],
    ["Checksum", `${m.checksumAlgorithm} ${m.checksum}`],
    ["Created", dateTime(m.createdAt)],
    ["Last modified", dateTime(m.lastModified)],
    ["Chunks", `${m.chunkCount} × ${human(m.chunkSize)} (last chunk may be smaller)`],
    ["Replication", null],
  ];
  $("sysmeta").replaceChildren(...rows.flatMap(([k, v]) => [
    el("dt", { text: k }),
    k === "Replication"
      ? el("dd", {}, pill(m.replication.status), ` factor ${m.replicationFactor} · ${m.replication.healthyChunks} healthy, ${m.replication.underReplicatedChunks} under, ${m.replication.unavailableChunks} unavailable`)
      : el("dd", { text: v }),
  ]));

  // keep unsaved edits: only rebuild the editor when the user has not changed it
  const editor = $("detail-meta");
  if (!state.metaDirty) {
    editor.replaceChildren();
    const entries = Object.entries(m.customMetadata || {});
    if (entries.length === 0) metaRow(editor);
    for (const [k, v] of entries) metaRow(editor, k, v);
  }

  renderChunkMap(m);
}

function renderChunkMap(m) {
  const workerIds = [...new Set([...state.workers.map((w) => w.workerId), ...m.chunks.flatMap((c) => c.replicas.map((r) => r.workerId))])].sort();
  const head = el("tr", {}, el("th", { class: "idx", text: "#" }), el("th", { class: "num", text: "Size" }),
    ...workerIds.map((w) => el("th", { text: w })), el("th", { text: "Live" }));
  const body = m.chunks.map((c) => {
    const byWorker = Object.fromEntries(c.replicas.map((r) => [r.workerId, r.workerStatus]));
    return el("tr", { title: c.chunkId + "\nsha256 " + c.checksum },
      el("td", { class: "idx", text: String(c.index) }),
      el("td", { class: "sz", text: human(c.size) }),
      ...workerIds.map((w) => el("td", {}, el("span", {
        class: "cell" + (byWorker[w] ? (byWorker[w] === "UP" ? " on" : " off") : ""),
        title: byWorker[w] ? `${w}: copy (${byWorker[w]})` : `${w}: no copy`,
      }))),
      el("td", {}, liveBadge(c.healthyReplicas, m.replicationFactor)));
  });
  $("chunkmap").replaceChildren(el("thead", {}, head), el("tbody", {}, ...body));
  const perWorker = {};
  m.chunks.forEach((c) => c.replicas.forEach((r) => { perWorker[r.workerId] = (perWorker[r.workerId] || 0) + 1; }));
  $("chunk-summary").textContent = "(" + Object.entries(perWorker).sort().map(([w, n]) => `${w}: ${n}`).join(", ") + ")";
}

function liveBadge(live, rf) {
  const cls = live === 0 ? "bad" : live < rf ? "warn" : live > rf ? "info" : "ok";
  return el("span", { class: "pill " + cls, text: `${live}/${rf}` });
}

$("close-detail").addEventListener("click", closeDetail);
$("detail-meta").addEventListener("input", () => { state.metaDirty = true; });
document.addEventListener("keydown", (e) => { if (e.key === "Escape" && !$("detail").hidden) closeDetail(); });
$("add-detail-meta").addEventListener("click", () => {
  state.metaDirty = true;
  metaRow($("detail-meta")).querySelector("input").focus();
});

$("save-meta").addEventListener("click", async () => {
  const m = state.detailMeta;
  if (!m) return;
  const now = readMetaRows($("detail-meta"));
  const patch = {};
  for (const k of Object.keys(m.customMetadata || {})) if (!(k in now)) patch[k] = null; // removed keys
  for (const [k, v] of Object.entries(now)) if ((m.customMetadata || {})[k] !== v) patch[k] = v;
  if (Object.keys(patch).length === 0) return toast("No metadata changes to save");
  try {
    const updated = await api(`/objects/${encodeURIComponent(m.objectId)}/metadata`, {
      method: "PATCH", headers: { "Content-Type": "application/merge-patch+json" }, body: JSON.stringify(patch),
    });
    state.metaDirty = false;
    state.detailMeta = updated;
    renderDetail(updated);
    toast("Metadata saved (object data was not re-uploaded)");
    refreshObjects();
  } catch (e) {
    toast("Save failed: " + e.message, true);
  }
});

$("delete-btn").addEventListener("click", async () => {
  const m = state.detailMeta;
  if (!m || !confirm(`Delete "${m.name}" and all of its chunks?`)) return;
  try {
    await api(`/objects/${encodeURIComponent(m.objectId)}`, { method: "DELETE" });
    toast(`Deleted ${m.name}`);
    closeDetail();
    refreshObjects();
    refreshCluster();
  } catch (e) {
    toast("Delete failed: " + e.message, true);
  }
});


// ---------------------------------------------------------------- infrastructure (from the demo runner)

const ROLE_CLASS = {
  ACTIVE: "ok", PRIMARY: "ok", ROUTING: "ok", HEALTHY: "ok",
  STANDBY: "info", "SYNC REPLICA": "info", REPLICA: "info", STARTING: "warn",
  "NO ACTIVE CONTROLLER": "bad", "NO PRIMARY": "bad", UNHEALTHY: "bad", DOWN: "bad",
};

function nodeRow(n, detail) {
  const cls = ROLE_CLASS[n.role] || "neutral";
  return el("div", { class: "node " + cls },
    el("span", { class: "node-name", text: n.name }, detail ? el("span", { class: "node-detail", text: detail }) : null),
    el("span", { class: "pill " + cls, text: n.role }));
}

async function refreshInfra() {
  try {
    const res = await fetch("/runner/infra");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const i = await res.json();
    $("infra-note").hidden = true;
    $("infra-control").replaceChildren(
      nodeRow(i.gateway, "client entry point"),
      ...i.controllers.map((c) => nodeRow(c, c.role === "ACTIVE" && c.term ? `term ${c.term}` : null)));
    $("infra-db").replaceChildren(
      nodeRow(i.pgProxy, "routes to primary"),
      ...i.postgres.map((p) => nodeRow(p, p.timeline ? `timeline ${p.timeline}` : null)));
    $("infra-etcd").replaceChildren(...i.etcd.map((e) => nodeRow(e)));
  } catch (e) {
    $("infra-note").hidden = false;
  }
}

// ---------------------------------------------------------------- demo scenarios

const runner = { scenarios: [], running: null, consoleN: null, next: 0, available: true };

function fmtSeconds(s) {
  return s >= 60 ? `~${Math.round(s / 60)} min` : `~${s} s`;
}

function scenarioStatusPill(s) {
  if (s.status === "running") return el("span", { class: "pill info running", text: `running · ${s.passed} passed` });
  if (s.status === "passed") return el("span", { class: "pill ok", text: `passed ${s.passed}/${s.passed + s.failed}` });
  if (s.status === "failed") return el("span", { class: "pill bad", text: `failed ${s.failed} of ${s.passed + s.failed}` });
  return el("span", { class: "pill neutral", text: "not run yet" });
}

function renderScenarios() {
  const busy = runner.running != null;
  $("scenario-list").replaceChildren(...runner.scenarios.map((s) => el("div", {
    class: "scenario" + (s.status === "running" ? " running" : "") + (runner.consoleN === s.n ? " selected" : ""),
  },
    el("div", { class: "scenario-head" },
      el("span", { class: "scenario-num", text: String(s.n) }),
      el("span", { class: "scenario-title", text: s.title })),
    el("p", { class: "scenario-desc", text: s.description }),
    el("div", { class: "scenario-foot" },
      scenarioStatusPill(s),
      el("div", { class: "actions" },
        s.status !== "idle" ? el("button", { class: "ghost small", text: "Log", onclick: () => openConsole(s.n) }) : null,
        el("button", {
          class: "primary small", text: s.status === "running" ? "Running…" : "▶ Run",
          disabled: busy || !runner.available ? "" : null,
          title: busy ? `Scenario ${runner.running} is running` : `Run scenario ${s.n} (${fmtSeconds(s.estimatedSeconds)})`,
          onclick: () => runScenario(s.n),
        }))),
    el("span", { class: "scenario-time", text: `takes ${fmtSeconds(s.estimatedSeconds)}` }))));
  const rs = $("runner-status");
  if (!runner.available) { rs.className = "pill bad"; rs.textContent = "runner unavailable"; }
  else if (busy) { rs.className = "pill info running"; rs.textContent = `scenario ${runner.running} running`; }
  else { rs.className = "pill ok"; rs.textContent = "ready"; }
}

async function refreshScenarios() {
  try {
    const res = await fetch("/runner/scenarios");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const body = await res.json();
    runner.available = true;
    const finished = runner.running != null && body.running !== runner.running ? runner.running : null;
    runner.scenarios = body.scenarios;
    runner.running = body.running;
    if (finished != null && finished === runner.consoleN) {
      pollLog();   // fetch the final lines and the result
      const s = body.scenarios.find((x) => x.n === finished);
      if (s) toast(`Scenario ${finished} ${s.status}: ${s.passed} passed, ${s.failed} failed`, s.status !== "passed");
    }
  } catch (e) {
    runner.available = false;
  }
  renderScenarios();
}

async function runScenario(n) {
  try {
    const res = await fetch(`/runner/run/${n}`, { method: "POST" });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) return toast(body.error || "Could not start scenario " + n, true);
    toast(`Scenario ${n} started`);
    runner.running = n;
    openConsole(n);
    refreshScenarios();
  } catch (e) {
    toast("Could not reach the demo runner", true);
  }
}

function openConsole(n) {
  runner.consoleN = n;
  runner.next = 0;
  $("console-body").replaceChildren();
  const s = runner.scenarios.find((x) => x.n === n);
  $("console-title").textContent = `Scenario ${n}${s ? " · " + s.title : ""}`;
  $("console").hidden = false;
  $("console").scrollIntoView({ behavior: "smooth", block: "nearest" });
  renderScenarios();
  pollLog();
}

function lineClass(line) {
  const t = line.trimStart();
  if (t.startsWith("PASS ")) return "l-pass";
  if (t.startsWith("FAIL ")) return "l-fail";
  if (t.startsWith("===")) return "l-section";
  if (t.startsWith("$ ")) return "l-cmd";
  if (t.startsWith("# ")) return "l-say";
  if (t.startsWith("waiting for")) return "l-wait";
  return "";
}

async function pollLog() {
  const n = runner.consoleN;
  if (n == null) return;
  try {
    const res = await fetch(`/runner/log/${n}?from=${runner.next}`);
    const log = await res.json();
    if (runner.consoleN !== n) return;
    const body = $("console-body");
    const atBottom = body.scrollHeight - body.scrollTop - body.clientHeight < 40;
    body.querySelector(".partial")?.remove();
    for (const line of log.lines || []) body.append(el("span", { class: lineClass(line), text: line + "\n" }));
    if (log.partial) body.append(el("span", { class: "partial " + lineClass(log.partial), text: log.partial }));
    runner.next = log.next || 0;
    if (atBottom) body.scrollTop = body.scrollHeight;
    const secs = log.startedAt ? Math.round(((log.finishedAt || Date.now() / 1000) - log.startedAt)) : 0;
    $("console-dot").className = "dot " + (log.status || "");
    $("console-meta").textContent = log.status === "idle" ? "not run yet"
      : `${log.status} · ${secs} s · ${log.passed} passed${log.failed ? ", " + log.failed + " failed" : ""}`;
    if (log.status !== "running" && runner.running === n) refreshScenarios();
  } catch (e) { /* runner restarting; try again */ }
}

$("console-close").addEventListener("click", () => {
  runner.consoleN = null;
  $("console").hidden = true;
  renderScenarios();
});

// ---------------------------------------------------------------- polling

refreshCluster();
refreshObjects();
if (location.hash.startsWith("#object=")) openDetail(decodeURIComponent(location.hash.slice(8)));  // deep link
refreshInfra();
refreshScenarios();
setInterval(refreshCluster, 2000);
setInterval(refreshInfra, 2000);
setInterval(refreshScenarios, 3000);
setInterval(() => {
  const s = runner.scenarios.find((x) => x.n === runner.consoleN);
  if (runner.consoleN != null && (runner.running === runner.consoleN || (s && s.status === "running"))) pollLog();
}, 700);
setInterval(refreshObjects, 5000);
setInterval(() => {
  if (state.detailId && !$("detail").hidden) {
    api(`/objects/${encodeURIComponent(state.detailId)}/metadata`)
      .then((m) => { state.detailMeta = m; renderDetail(m); })
      .catch(() => closeDetail());
  }
}, 3000);
