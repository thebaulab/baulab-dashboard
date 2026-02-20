const POLL_INTERVAL_MS = 30000; // 30 seconds

function getApiBase() {
  // ?api=URL overrides config (useful when tunnel URL changes)
  const params = new URLSearchParams(location.search);
  const qs = params.get("api");
  if (qs) return qs.replace(/\/$/, "");

  const base = window.GPU_DASHBOARD_API;
  if (base) {
    return base.replace(/\/$/, "");
  }
  return ""; // same origin
}

function utilClass(util) {
  if (util < 50) return "low";
  if (util < 80) return "med";
  return "high";
}

function formatMem(used, total) {
  if (!total) return "—";
  const usedGb = (used / 1024).toFixed(1);
  const totalGb = (total / 1024).toFixed(1);
  return `${usedGb} / ${totalGb} GB`;
}

function renderNode(name, data) {
  const card = document.createElement("div");
  card.className = "card" + (data.status === "error" ? " error" : "");

  if (data.status === "error") {
    card.innerHTML = `
      <h2>${escapeHtml(name)}</h2>
      <div class="error-msg">${escapeHtml(data.message || "Unknown error")}</div>
    `;
    return card;
  }

  const users = data.users || [];
  const usersTip =
    users.length > 0
      ? `Users: ${users.join(", ")}`
      : "No GPU processes running";

  const gpus = data.gpus || [];
  const gpuRows = gpus
    .map(
      (g) => `
    <div class="gpu-row">
      <span class="name">GPU ${g.index}: ${escapeHtml(g.name)}</span>
      <span class="util ${utilClass(g.utilization)}">${g.utilization}%</span>
      <span class="mem">${formatMem(g.memory_used, g.memory_total)}</span>
      <span class="temp">${g.temperature}°C</span>
    </div>
  `
    )
    .join("");

  card.innerHTML = `
    <h2>${escapeHtml(name)}</h2>
    <div class="gpu-list">${gpuRows || "<span class='text-muted'>No GPUs</span>"}</div>
    <div class="users-hint">Hover for users</div>
    <div class="users-tooltip">${escapeHtml(usersTip)}</div>
  `;
  return card;
}

function escapeHtml(s) {
  if (s == null) return "";
  const div = document.createElement("div");
  div.textContent = s;
  return div.innerHTML;
}

function formatTime(iso) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch {
    return iso;
  }
}

async function fetchStatus() {
  const base = getApiBase();
  const url = base ? `${base}/api/status` : "/api/status";
  const res = await fetch(url);
  if (!res.ok) throw new Error(res.statusText);
  return res.json();
}

function render(data) {
  const grid = document.getElementById("grid");
  const loading = document.getElementById("loading");
  const lastUpdated = document.getElementById("last-updated");

  lastUpdated.textContent = data.last_updated
    ? `Last updated: ${formatTime(data.last_updated)}`
    : "Collecting…";

  const nodes = data.nodes || {};
  if (Object.keys(nodes).length === 0) {
    loading.classList.remove("hidden");
    grid.innerHTML = "";
    return;
  }

  loading.classList.add("hidden");
  grid.innerHTML = "";
  for (const [name, nodeData] of Object.entries(nodes)) {
    grid.appendChild(renderNode(name, nodeData));
  }
}

async function poll() {
  try {
    const data = await fetchStatus();
    render(data);
  } catch (e) {
    document.getElementById("last-updated").textContent = `Error: ${e.message}`;
  }
}

// Initial load and periodic refresh
poll();
setInterval(poll, POLL_INTERVAL_MS);
