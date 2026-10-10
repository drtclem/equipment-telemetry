/* Live equipment dashboard — polls every 2 s. Vanilla JS, no build step. */
"use strict";

function age(seconds) {
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${(seconds / 3600).toFixed(1)}h ago`;
}

function deviceCard(device, { withClaim = false } = {}) {
  const card = document.createElement("div");
  card.className = "device-card";

  const head = document.createElement("div");
  head.className = "device-head";
  const id = document.createElement("span");
  id.className = "device-id";
  id.textContent = device.device_id;
  const chip = document.createElement("span");
  chip.className = `chip ${device.status}`;
  chip.textContent = device.status;
  head.append(id, chip);

  const gw = document.createElement("div");
  gw.className = "gateway";
  gw.textContent = `gateway: ${device.first_gateway ?? "—"}`;

  card.append(head, gw);

  const metrics = Object.entries(device.metrics ?? {});
  if (metrics.length === 0) {
    const none = document.createElement("div");
    none.className = "empty";
    none.textContent = "no state yet";
    card.append(none);
  }
  for (const [metric, m] of metrics) {
    const row = document.createElement("div");
    row.className = "metric-row";
    const name = document.createElement("span");
    name.className = "metric-name";
    name.textContent = metric;
    const value = document.createElement("span");
    value.className = "metric-value";
    value.textContent = String(m.value);
    const when = document.createElement("span");
    when.className = "metric-age";
    when.textContent = age(m.age_seconds ?? 0);
    when.title = m.event_time ?? "";
    row.append(name, value, when);
    card.append(row);

    if (m.reconciling) {
      const badge = document.createElement("span");
      badge.className = "badge";
      badge.textContent = "reconciling — backlog may be in flight";
      card.append(badge);
    }
  }

  if (withClaim) {
    const btn = document.createElement("button");
    btn.className = "claim-btn";
    btn.textContent = "Claim device";
    btn.setAttribute("aria-label", `Claim ${device.device_id}`);
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      btn.textContent = "claiming…";
      try {
        const res = await fetch(`/devices/${encodeURIComponent(device.device_id)}/claim`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({}),
        });
        if (!res.ok) {
          const detail = (await res.json().catch(() => ({}))).detail;
          btn.textContent = detail ?? `error ${res.status}`;
          return;
        }
        await refresh();
      } finally {
        btn.disabled = false;
      }
    });
    card.append(btn);
  }
  return card;
}

function renderDevices(target, devices, options) {
  target.replaceChildren();
  if (devices.length === 0) {
    const p = document.createElement("p");
    p.className = "empty";
    p.textContent = options.emptyText;
    target.append(p);
    return;
  }
  for (const device of devices) target.append(deviceCard(device, options));
}

async function refresh() {
  try {
    const [devRes, statRes] = await Promise.all([fetch("/devices"), fetch("/stats")]);
    if (!devRes.ok || !statRes.ok) return;
    const { devices = [] } = await devRes.json();
    const stats = await statRes.json();

    document.getElementById("stat-readings").textContent = stats.readings;
    document.getElementById("stat-devices").textContent = stats.devices;
    document.getElementById("stat-late").textContent = stats.late_readings;
    document.getElementById("stat-pending").textContent = stats.pending_entries;

    const unclaimed = devices.filter((d) => d.status === "unclaimed");
    const claimed = devices.filter((d) => d.status === "claimed");
    renderDevices(document.getElementById("devices"), claimed, {
      emptyText: "No claimed devices yet.",
    });
    renderDevices(document.getElementById("unclaimed"), unclaimed, {
      emptyText: "Nothing waiting.",
      withClaim: true,
    });
    document.getElementById("last-refresh").textContent =
      `updated ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    console.error("refresh failed", err);
  }
}

refresh();
setInterval(refresh, 2000);
