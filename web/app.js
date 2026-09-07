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
  retrying: "retrying", dead: "dead", no_handler: "skipped",
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

// --- SSE wiring -------------------------------------------------------------
const es = new EventSource("/events");
es.addEventListener("snapshot", (e) => (JSON.parse(e.data) || []).forEach(render));
es.onmessage = (e) => render(viewFromEvent(JSON.parse(e.data)));
es.onopen  = () => ($("conn").className = "dot ok");
es.onerror = () => ($("conn").className = "dot bad"); // EventSource auto-reconnects

// --- controls ---------------------------------------------------------------
const post = (qs) => fetch("/api/enqueue" + (qs ? "?" + qs : ""), { method: "POST" });
$("enqueue").onclick = () => post("");
$("delay").onclick   = () => post("delay=6s");
$("flaky").onclick   = () => post("type=flaky");
$("fail").onclick    = () => post("type=always_fail");
$("burst").onclick   = () => { for (let i = 0; i < 10; i++) post(""); };
