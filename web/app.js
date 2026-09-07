// Live dashboard client. Connects to the SSE stream, folds each job-transition
// event into a board of cards, and lets you enqueue demo jobs. No dependencies.

const $ = (id) => document.getElementById(id);

// Each job state maps to a lane container. "skipped" shares the "dead" lane.
const lanes = {
  queued:   $("lane-queued"),
  running:  $("lane-running"),
  retrying: $("lane-retrying"),
  done:     $("lane-done"),
  dead:     $("lane-dead"),
  skipped:  $("lane-dead"),
};

// A live job event's `kind` maps to the UI state.
const STATE_FROM_KIND = {
  enqueued: "queued", started: "running", succeeded: "done",
  retrying: "retrying", dead: "dead", reclaimed: "running", no_handler: "skipped",
};

const jobs = new Map(); // job id -> { el, state }

const shortId = (id) => (id ? "#" + id.slice(-6) : "#??????");
const esc = (s) => String(s).replace(/[<>&]/g, (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" }[c]));

// render creates or updates a job card and moves it to the right lane.
function render(v) {
  if (!v || !v.id) return;
  let entry = jobs.get(v.id);
  if (!entry) {
    entry = { el: document.createElement("div"), state: null };
    entry.el.className = "card";
    jobs.set(v.id, entry);
  }
  const el = entry.el;
  el.dataset.state = v.state;

  const bits = [
    `<span class="type">${esc(v.type || "?")}</span>`,
    `<span class="id">${shortId(v.id)}</span>`,
  ];
  if (v.worker) bits.push(`<span class="meta">${esc(v.worker)}</span>`);
  if (v.attempt) bits.push(`<span class="meta">try ${v.attempt + 1}</span>`);
  if ((v.state === "retrying" || v.state === "queued") && v.retry_in_ms)
    bits.push(`<span class="meta">${v.state === "queued" ? "starts" : "retry"} ~${(v.retry_in_ms / 1000).toFixed(1)}s</span>`);
  if (v.duration_ms && (v.state === "done")) bits.push(`<span class="meta">${v.duration_ms}ms</span>`);
  if (v.error && (v.state === "retrying" || v.state === "dead"))
    bits.push(`<span class="err" title="${esc(v.error)}">${esc(v.error)}</span>`);
  el.innerHTML = bits.join("");

  // Move to the correct lane only when the state actually changed.
  if (entry.state !== v.state) {
    (lanes[v.state] || lanes.queued).prepend(el);
    entry.state = v.state;
  }
  // Retrigger the pulse animation (remove class, force reflow, re-add).
  el.classList.remove("pulse"); void el.offsetWidth; el.classList.add("pulse");
  updateCounts();
}

function updateCounts() {
  const c = { queued: 0, running: 0, retrying: 0, done: 0, dead: 0 };
  for (const [, e] of jobs) {
    if (e.state === "skipped") c.dead++;
    else if (c[e.state] !== undefined) c[e.state]++;
  }
  $("c-queued").textContent = c.queued;
  $("c-running").textContent = c.running;
  $("c-retrying").textContent = c.retrying;
  $("c-done").textContent = c.done;
  $("c-dead").textContent = c.dead;
}

const viewFromEvent = (ev) => ({
  id: ev.job_id, type: ev.job_type,
  state: STATE_FROM_KIND[ev.kind] || "queued",
  worker: ev.worker, attempt: ev.attempt,
  duration_ms: ev.duration_ms, retry_in_ms: ev.retry_in_ms, error: ev.error,
});

// --- narration feed ---------------------------------------------------------
const feed = $("feed");
function feedLine(html) {
  const li = document.createElement("li");
  li.innerHTML = `<span class="t">${new Date().toLocaleTimeString()}</span>${html}`;
  feed.prepend(li);
  while (feed.children.length > 40) feed.removeChild(feed.lastChild);
}
function narrate(ev) {
  const id = ev.job_id ? "#" + ev.job_id.slice(-6) : "";
  const t = esc(ev.job_type || "job");
  switch (ev.kind) {
    case "enqueued":
      feedLine(ev.retry_in_ms
        ? `enqueued <b>${t}</b> ${id} — scheduled in ${(ev.retry_in_ms / 1000).toFixed(0)}s`
        : `enqueued <b>${t}</b> ${id}`); break;
    case "started":   feedLine(`<b>${esc(ev.worker)}</b> started ${t} ${id}`); break;
    case "succeeded": feedLine(`${t} ${id} ✅ done in ${ev.duration_ms}ms`); break;
    case "retrying":  feedLine(`${t} ${id} failed — retrying in ${(ev.retry_in_ms / 1000).toFixed(1)}s (attempt ${ev.attempt + 1})`); break;
    case "dead":      feedLine(`☠️ ${t} ${id} dead-lettered after exhausting retries`); break;
    case "reclaimed": feedLine(`♻️ <b>${esc(ev.worker)}</b> reclaimed stranded ${t} ${id} — recovering a crashed worker's job`); break;
    case "no_handler": feedLine(`no handler for ${t} ${id} — skipped`); break;
  }
}

// --- SSE wiring -------------------------------------------------------------
const es = new EventSource("/events");
es.addEventListener("snapshot", (e) => (JSON.parse(e.data) || []).forEach(render));
es.onmessage = (e) => { const ev = JSON.parse(e.data); render(viewFromEvent(ev)); narrate(ev); };
es.onopen  = () => ($("conn").className = "dot ok");
es.onerror = () => ($("conn").className = "dot bad"); // EventSource auto-reconnects

// --- job controls -----------------------------------------------------------
const post = (qs) => fetch("/api/enqueue" + (qs ? "?" + qs : ""), { method: "POST" });
$("enqueue").onclick = () => post("");
$("slow").onclick    = () => post("type=slow");
$("delay").onclick   = () => post("delay=6s");
$("flaky").onclick   = () => post("type=flaky");
$("fail").onclick    = () => post("type=always_fail");
$("burst").onclick   = () => { for (let i = 0; i < 10; i++) post(""); };
$("redrive").onclick = async () => {
  const r = await fetch("/api/dlq/redrive", { method: "POST" });
  if (r.ok) feedLine(`↩️ re-drove <b>${(await r.json()).redriven}</b> dead job(s) back to the queue`);
};

// --- worker controls (enabled only under cmd/demo) --------------------------
async function refreshWorkers() {
  try {
    const { count, enabled } = await (await fetch("/api/workers")).json();
    $("wcount").textContent = enabled ? `${count} worker${count === 1 ? "" : "s"}` : "controls off";
    $("addw").disabled = !enabled;
    $("killw").disabled = !enabled;
  } catch { /* ignore */ }
}
$("addw").onclick = async () => {
  const r = await fetch("/api/worker/add", { method: "POST" });
  if (r.ok) feedLine(`＋ spawned <b>${(await r.json()).worker}</b>`);
  refreshWorkers();
};
$("killw").onclick = async () => {
  const r = await fetch("/api/worker/kill", { method: "POST" });
  if (r.ok) feedLine(`💀 killed <b>${(await r.json()).worker}</b> (SIGKILL) — its in-flight jobs are now stranded in the PEL`);
  refreshWorkers();
};
refreshWorkers();
