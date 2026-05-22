// Nexus Orchestrator UI
const RP = document.querySelector("meta[name=rp]")?.content ?? "";

let activeModuleId = null;
let modulesCache = {};

const $ = id => document.getElementById(id);

// ── Module list ───────────────────────────────────────────────────────────────

function renderModules(list) {
  modulesCache = {};
  const el = $("moduleList");
  el.innerHTML = "";
  if (!list.length) {
    el.innerHTML = '<p class="sidebar__empty">Нет модулей</p>';
    return;
  }
  for (const m of list) {
    modulesCache[m.id] = m;
    const btn = document.createElement("button");
    btn.className = "module-item" + (m.id === activeModuleId ? " module-item--active" : "");
    btn.dataset.id = m.id;
    btn.type = "button";
    btn.innerHTML = `
      <span class="module-item__dot module-item__dot--${m.status}"></span>
      <span class="module-item__name">${esc(m.name)}</span>
      <span class="module-item__ver">v${esc(m.version)}</span>`;
    btn.addEventListener("click", () => selectModule(m.id));
    el.appendChild(btn);
  }
}

async function refreshModules() {
  const res = await fetch(RP + "/api/modules");
  if (!res.ok) return;
  renderModules(await res.json());
  if (activeModuleId && modulesCache[activeModuleId]) updateToolbar(modulesCache[activeModuleId]);
}

// ── Select module ─────────────────────────────────────────────────────────────

function selectModule(id) {
  activeModuleId = id;
  const m = modulesCache[id];
  if (!m) return;

  document.querySelectorAll(".module-item").forEach(b => b.classList.toggle("module-item--active", b.dataset.id === id));
  $("topbarCrumb").textContent = m.name;
  updateToolbar(m);

  $("contentWelcome").hidden = true;
  $("contentModule").hidden = false;

  const frame = $("moduleFrame");
  frame.src = m.status === "active" ? `${RP}/${id}/panel/index.html` : "about:blank";
}

function updateToolbar(m) {
  $("mtName").textContent = m.name;
  $("mtVersion").textContent = "v" + m.version;
  $("mtStatus").textContent = statusLabel(m.status);
  $("mtStatus").className = "module-toolbar__status module-toolbar__status--" + m.status;
  $("mtPause").hidden  = m.status !== "active";
  $("mtResume").hidden = m.status !== "paused";
}

const STATUS_LABELS = { active: "активен", paused: "пауза", unloaded: "выгружен", error: "ошибка" };
function statusLabel(s) { return STATUS_LABELS[s] || s; }

// ── Toolbar actions ───────────────────────────────────────────────────────────

async function moduleAction(action) {
  if (!activeModuleId) return;
  const res = await fetch(`${RP}/api/modules/${activeModuleId}/${action}`, { method: "POST" });
  if (!res.ok) { const e = await res.json().catch(() => ({})); alert(e.error || "Ошибка"); return; }
  if (action === "unload") {
    activeModuleId = null;
    $("contentWelcome").hidden = false;
    $("contentModule").hidden = true;
    $("topbarCrumb").textContent = "Оркестратор";
  }
  await refreshModules();
  if (activeModuleId && modulesCache[activeModuleId]) selectModule(activeModuleId);
}

$("mtPause").addEventListener("click",  () => moduleAction("pause"));
$("mtResume").addEventListener("click", () => moduleAction("resume"));
$("mtUnload").addEventListener("click", () => {
  if (confirm(`Выгрузить модуль «${modulesCache[activeModuleId]?.name}»? Файлы будут удалены.`)) moduleAction("unload");
});

// ── Docs ──────────────────────────────────────────────────────────────────────

$("mtDocs").addEventListener("click", async () => {
  if (!activeModuleId) return;
  const m = modulesCache[activeModuleId];
  $("docsTitle").textContent = m?.name ?? activeModuleId;

  const docsUrl = `${RP}/${activeModuleId}/panel/docs.html`;
  const res = await fetch(docsUrl).catch(() => null);
  const body = $("docsBody");

  if (res?.ok) {
    const html = await res.text();
    // извлекаем <body> если есть
    const match = html.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
    body.innerHTML = `<div class="docs-body">${match ? match[1] : html}</div>`;
  } else {
    // fallback — из manifest
    const manifest = JSON.parse(m?.manifest_json || "{}");
    body.innerHTML = `<div class="docs-body">
      <h2>${esc(m?.name)} v${esc(m?.version)}</h2>
      <p>${esc(m?.description || "Документация не предоставлена.")}</p>
      ${manifest.docs ? `<pre>${esc(manifest.docs)}</pre>` : ""}
    </div>`;
  }
  $("docsOverlay").hidden = false;
});

$("docsCloseBtn").addEventListener("click", () => { $("docsOverlay").hidden = true; });
$("docsOverlay").addEventListener("click", e => { if (e.target === $("docsOverlay")) $("docsOverlay").hidden = true; });

// ── Upload dialog ─────────────────────────────────────────────────────────────

let selectedFile = null;

$("uploadBtn").addEventListener("click", () => { $("uploadOverlay").hidden = false; });
$("uploadCloseBtn").addEventListener("click", closeUpload);
$("uploadOverlay").addEventListener("click", e => { if (e.target === $("uploadOverlay")) closeUpload(); });

function closeUpload() {
  $("uploadOverlay").hidden = true;
  selectedFile = null;
  $("uploadInput").value = "";
  $("uploadZoneText").textContent = "Перетащите .zip или нажмите для выбора";
  $("uploadStatus").textContent = "";
  $("uploadStatus").className = "status-line";
  $("uploadSubmitBtn").disabled = true;
}

$("uploadZone").addEventListener("click", () => $("uploadInput").click());
$("uploadInput").addEventListener("change", () => {
  selectedFile = $("uploadInput").files[0];
  if (selectedFile) { $("uploadZoneText").textContent = selectedFile.name; $("uploadSubmitBtn").disabled = false; }
});
$("uploadZone").addEventListener("dragover", e => e.preventDefault());
$("uploadZone").addEventListener("drop", e => {
  e.preventDefault();
  const f = e.dataTransfer.files[0];
  if (f?.name.endsWith(".zip")) { selectedFile = f; $("uploadZoneText").textContent = f.name; $("uploadSubmitBtn").disabled = false; }
});

$("uploadSubmitBtn").addEventListener("click", async () => {
  if (!selectedFile) return;
  $("uploadSubmitBtn").disabled = true;
  $("uploadStatus").textContent = "Загрузка...";
  $("uploadStatus").className = "status-line";

  const fd = new FormData();
  fd.append("file", selectedFile);
  try {
    const res = await fetch(RP + "/api/modules/upload", { method: "POST", body: fd });
    const data = await res.json();
    if (!res.ok) {
      $("uploadStatus").textContent = data.error || "Ошибка";
      $("uploadStatus").className = "status-line status-line--err";
      $("uploadSubmitBtn").disabled = false;
      return;
    }
    $("uploadStatus").textContent = `Модуль «${data.name}» v${data.version} установлен`;
    $("uploadStatus").className = "status-line status-line--ok";
    await refreshModules();
    setTimeout(closeUpload, 1200);
  } catch {
    $("uploadStatus").textContent = "Сетевая ошибка";
    $("uploadStatus").className = "status-line status-line--err";
    $("uploadSubmitBtn").disabled = false;
  }
});

// ── Utils ─────────────────────────────────────────────────────────────────────

function esc(s) {
  return String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

// ── Init ──────────────────────────────────────────────────────────────────────

(async () => {
  const res = await fetch(RP + "/api/modules");
  if (res.ok) renderModules(await res.json());
})();
