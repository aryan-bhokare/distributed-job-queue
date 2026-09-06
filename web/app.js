// Live dashboard client. Connects to the SSE stream, folds each job-transition
// event into a board of cards, and lets you enqueue demo jobs. No dependencies.

const $ = (id) => document.getElementById(id);

// Each job state maps to a lane container. "skipped" shares the "failed" lane.
const lanes = {
  queued:  $("lane-queued"),
  running: $("lane-running"),
  done:    $("lane-done"),
  failed:  $("lane-failed"),
  skipped: $("lane-failed"),
};

// A live job event's `kind` maps to the UI state.
const STATE_FROM_KIND = {
  enqueued: "queued", started: "running", succeeded: "done",
  failed: "failed", no_handler: "skipped",
};

const jobs = new Map(); // job id -> { el, state }

const shortId = (id) => (id ? "#" + id.slice(-6) : "#??????");
const esc = (s) => String(s).replace(/[<>&]/g, (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" }[c]));

// render creates or updates a job card and moves it to the right lane.
// Accepts either a snapshot view {id,type,state,...} or one made from an event.
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
  el.innerHTML =
    `<span class="type">${esc(v.type || "?")}</span>` +
    `<span class="id">${shortId(v.id)}</span>` +
    (v.worker ? `<span class="meta">${esc(v.worker)}</span>` : "") +
    (v.duration_ms ? `<span class="meta">${v.duration_ms}ms</span>` : "") +
    (v.error ? `<span class="err" title="${esc(v.error)}">${esc(v.error)}</span>` : "");

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
  const c = { queued: 0, running: 0, done: 0, failed: 0 };
  for (const [, e] of jobs) {
    if (e.state === "skipped") c.failed++;
    else if (c[e.state] !== undefined) c[e.state]++;
  }
  $("c-queued").textContent = c.queued;
  $("c-running").textContent = c.running;
  $("c-done").textContent = c.done;
  $("c-failed").textContent = c.failed;
}

const viewFromEvent = (ev) => ({
  id: ev.job_id, type: ev.job_type,
  state: STATE_FROM_KIND[ev.kind] || "queued",
  worker: ev.worker, duration_ms: ev.duration_ms, error: ev.error,
});

// --- SSE wiring -------------------------------------------------------------
const es = new EventSource("/events");
es.addEventListener("snapshot", (e) => (JSON.parse(e.data) || []).forEach(render));
es.onmessage = (e) => render(viewFromEvent(JSON.parse(e.data)));
es.onopen  = () => ($("conn").className = "dot ok");
es.onerror = () => ($("conn").className = "dot bad"); // EventSource auto-reconnects

// --- controls ---------------------------------------------------------------
$("enqueue").onclick = () => fetch("/api/enqueue", { method: "POST" });
$("burst").onclick = () => { for (let i = 0; i < 10; i++) fetch("/api/enqueue", { method: "POST" }); };
