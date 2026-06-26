// Nexus Orchestrator UI v3
const RP = document.querySelector("meta[name=rp]")?.content ?? "";

// ── Toast notifications ───────────────────────────────────────────────────────

const _toastWrap = () => document.getElementById("toastWrap");

function notify(message, level = "warn") {
  const wrap = _toastWrap();
  if (!wrap) return;
  const el = document.createElement("div");
  el.className = `toast toast--${level}`;
  el.textContent = message;
  el.addEventListener("click", () => el.remove());
  wrap.appendChild(el);
  const ttl = level === "error" ? 7000 : 4500;
  setTimeout(() => el.classList.contains("toast") && el.remove(), ttl);
}
// экспортируем глобально — iframe модулей могут вызывать window.parent.notify()
window.notify = notify;

let activeModuleId = null;
let modulesCache = {};
let sortOrder = [];  // localStorage порядок
let globalBusyCount = 0;

const $ = id => document.getElementById(id);

function setGlobalBusy(on, text = "Загрузка") {
  globalBusyCount = Math.max(0, globalBusyCount + (on ? 1 : -1));
  const el = $("globalBusy");
  if (!el) return;
  $("globalBusyText").textContent = text;
  el.hidden = globalBusyCount === 0;
}

async function withGlobalBusy(promise, text = "Загрузка") {
  setGlobalBusy(true, text);
  try { return await promise; }
  finally { setGlobalBusy(false, text); }
}

// ── LocalStorage order ────────────────────────────────────────────────────────

function loadOrder() {
  try { return JSON.parse(localStorage.getItem("nexus_order") || "[]"); } catch { return []; }
}
function saveOrder(ids) {
  localStorage.setItem("nexus_order", JSON.stringify(ids));
}
function applyOrder(list) {
  const order = loadOrder();
  if (!order.length) return list;
  const map = Object.fromEntries(list.map(m => [m.id, m]));
  const ordered = order.filter(id => map[id]).map(id => map[id]);
  const rest = list.filter(m => !order.includes(m.id));
  return [...ordered, ...rest];
}

// ── Hash routing ──────────────────────────────────────────────────────────────

function getHashRoute() {
  const h = location.hash.slice(1);
  if (!h) return { id: null, path: "" };
  const parts = h.split("/");
  return {
    id: parts.shift() || null,
    path: parts.length ? "/" + parts.join("/") : "",
  };
}
function getHashModule() {
  return getHashRoute().id;
}
function setHashModule(id, path = "", replace = false) {
  const cleanPath = path ? "/" + String(path).replace(/^\/+/, "") : "";
  const url = RP + "/#" + id + cleanPath;
  const method = replace ? "replaceState" : "pushState";
  history[method]({module: id, path: cleanPath}, "", url);
  document.title = id ? `Nexus / ${modulesCache[id]?.name ?? id}` : "Nexus";
}

window.addEventListener("popstate", () => {
  const route = getHashRoute();
  const id = route.id;
  if (id && modulesCache[id]) selectModule(id, false, route.path);
  else if (!id) {
    activeModuleId = null;
    $("contentWelcome").hidden = false;
    $("contentModule").hidden = true;
    $("topbarCrumb").textContent = "Оркестратор";
    document.title = "Nexus";
    document.querySelectorAll(".module-item").forEach(b => b.classList.remove("module-item--active"));
  }
});

window.addEventListener("message", event => {
  if (event.origin !== location.origin) return;
  const data = event.data || {};
  if (data.type !== "nexus:set-module-path") return;
  if (!activeModuleId || data.moduleId !== activeModuleId) return;
  setHashModule(activeModuleId, data.path || "", true);
});

// ── Module list render ────────────────────────────────────────────────────────

function renderModules(list) {
  modulesCache = {};
  const ordered = applyOrder(list);
  const el = $("moduleList");
  el.innerHTML = "";

  if (!ordered.length) {
    el.innerHTML = '<p class="sidebar__empty">Нет модулей</p>';
    return;
  }

  for (const m of ordered) {
    modulesCache[m.id] = m;
    const btn = document.createElement("button");
    btn.className = "module-item" + (m.id === activeModuleId ? " module-item--active" : "");
    btn.dataset.id = m.id;
    btn.type = "button";
    btn.draggable = true;
    btn.innerHTML = `
      <span class="module-item__drag" title="Перетащить">⠿</span>
      <span class="module-item__dot module-item__dot--${m.status}"></span>
      <span class="module-item__name">${esc(m.name)}</span>
      <span class="module-item__ver">v${esc(m.version)}</span>`;
    btn.addEventListener("click", e => {
      if (e.target.classList.contains("module-item__drag")) return;
      selectModule(m.id);
    });
    attachDrag(btn);
    el.appendChild(btn);
  }
}

async function refreshModules() {
  const res = await withGlobalBusy(fetch(RP + "/api/modules"), "Обновление модулей");
  if (!res.ok) return;
  renderModules(await res.json());
  if (activeModuleId && modulesCache[activeModuleId]) updateToolbar(modulesCache[activeModuleId]);
}

// ── Drag & drop ───────────────────────────────────────────────────────────────

let _dragSrc = null;

function attachDrag(el) {
  el.addEventListener("dragstart", e => {
    _dragSrc = el;
    el.classList.add("module-item--dragging");
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", el.dataset.id);
  });
  el.addEventListener("dragend", () => {
    el.classList.remove("module-item--dragging");
    document.querySelectorAll(".module-item--dragover").forEach(x => x.classList.remove("module-item--dragover"));
    // сохраняем порядок
    const ids = [...$("moduleList").querySelectorAll(".module-item")].map(b => b.dataset.id);
    saveOrder(ids);
  });
  el.addEventListener("dragover", e => {
    e.preventDefault();
    if (_dragSrc && _dragSrc !== el) {
      document.querySelectorAll(".module-item--dragover").forEach(x => x.classList.remove("module-item--dragover"));
      el.classList.add("module-item--dragover");
    }
  });
  el.addEventListener("drop", e => {
    e.preventDefault();
    el.classList.remove("module-item--dragover");
    if (!_dragSrc || _dragSrc === el) return;
    const list = $("moduleList");
    const items = [...list.querySelectorAll(".module-item")];
    const srcIdx = items.indexOf(_dragSrc);
    const dstIdx = items.indexOf(el);
    if (srcIdx < dstIdx) list.insertBefore(_dragSrc, el.nextSibling);
    else list.insertBefore(_dragSrc, el);
    const ids = [...list.querySelectorAll(".module-item")].map(b => b.dataset.id);
    saveOrder(ids);
  });
}

// ── Select module ─────────────────────────────────────────────────────────────

function selectModule(id, pushHistory = true, modulePath = "") {
  activeModuleId = id;
  const m = modulesCache[id];
  if (!m) return;

  document.querySelectorAll(".module-item").forEach(b =>
    b.classList.toggle("module-item--active", b.dataset.id === id));

  $("topbarCrumb").textContent = m.name;
  if (pushHistory) setHashModule(id, modulePath);

  updateToolbar(m);
  $("contentWelcome").hidden = true;
  $("contentModule").hidden = false;
  const panelHash = modulePath ? "#" + modulePath.replace(/^\/?/, "/") : "";
  $("moduleFrame").src = m.status === "active" ? `${RP}/${id}/panel/index.html?v=${encodeURIComponent(m.version || "")}${panelHash}` : "about:blank";

  // уведомления о статусе модуля
  if (m.status === "error") {
    notify(`Модуль «${m.name}» не запустился — проверьте Логгер`, "error");
  } else if (m.status === "paused") {
    notify(`Модуль «${m.name}» на паузе`, "warn");
  }

  // проверка ENV переменных модуля
  checkModuleEnv(m);
}

async function checkModuleEnv(m) {
  try {
    const manifest = JSON.parse(m.manifest_json || "{}");
    const envVars = manifest.env_vars || {};
    const keys = Array.isArray(manifest.env_required)
      ? manifest.env_required.filter(Boolean)
      : Object.keys(envVars).filter(Boolean);
    if (!keys.length) return;

    const res = await withGlobalBusy(fetch(`${RP}/api/env/check?keys=${keys.join(",")}`), "Проверка ENV");
    if (!res.ok) return;
    const status = await res.json();

    for (const [key, present] of Object.entries(status)) {
      if (!present) {
        notify(`${m.name}: не задана переменная ${key} — добавьте в ENV`, "warn");
      }
    }
  } catch { /* игнорируем */ }
}

function updateToolbar(m) {
  $("mtName").textContent = m.name;
  $("mtVersion").textContent = "v" + m.version;
  $("mtStatus").textContent = statusLabel(m.status);
  $("mtStatus").className = "module-toolbar__status module-toolbar__status--" + m.status;
  $("mtModuleSettings").hidden = m.id !== "customer-db" || m.status !== "active";
  $("mtPause").hidden  = m.status !== "active";
  $("mtResume").hidden = m.status !== "paused";
}

const STATUS_LABELS = { active: "активен", paused: "пауза", unloaded: "выгружен", error: "ошибка" };
function statusLabel(s) { return STATUS_LABELS[s] || s; }

// ── Toolbar actions ───────────────────────────────────────────────────────────

async function moduleAction(action) {
  if (!activeModuleId) return;
  const labels = {pause: "Пауза модуля", resume: "Запуск модуля", unload: "Выгрузка модуля"};
  const res = await withGlobalBusy(
    fetch(`${RP}/api/modules/${activeModuleId}/${action}`, { method: "POST" }),
    labels[action] || "Операция"
  );
  if (!res.ok) { const e = await res.json().catch(() => ({})); alert(e.error || "Ошибка"); return; }
  if (action === "unload") {
    history.pushState({}, "", RP + "/");
    document.title = "Nexus";
    activeModuleId = null;
    $("contentWelcome").hidden = false;
    $("contentModule").hidden = true;
    $("topbarCrumb").textContent = "Оркестратор";
  }
  await refreshModules();
  if (activeModuleId && modulesCache[activeModuleId]) selectModule(activeModuleId, false);
}

$("mtPause").addEventListener("click",  () => moduleAction("pause"));
$("mtResume").addEventListener("click", () => moduleAction("resume"));
$("mtUnload").addEventListener("click", () => {
  if (confirm(`Выгрузить модуль «${modulesCache[activeModuleId]?.name}»? Файлы будут удалены.`))
    moduleAction("unload");
});
$("mtModuleSettings").addEventListener("click", openModuleSettings);

async function openModuleSettings() {
  if (activeModuleId !== "customer-db") return;
  $("moduleSettingsTitle").textContent = "База клиентов";
  const body = $("moduleSettingsBody");
  body.innerHTML = '<p class="status-line">Загрузка...</p>';
  $("moduleSettingsOverlay").hidden = false;

  const res = await withGlobalBusy(fetch(`${RP}/customer-db/api/settings/token`), "Загрузка настроек");
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    body.innerHTML = `<p class="status-line status-line--err">${esc(err.detail || err.error || "Не удалось загрузить настройки")}</p>`;
    return;
  }
  const data = await res.json();
  const headersJson = JSON.stringify(data.headers_json || {}, null, 2);
  body.innerHTML = `
    <label class="form-label">NEXUS_CUSTOMER_DB_API_TOKEN
      <input id="customerDbTokenInput" class="form-input token-input" value="${esc(data.token || "")}" readonly />
    </label>
    <label class="form-label">Заголовки для Salebot JSON
      <textarea id="customerDbHeadersInput" class="form-input token-textarea" readonly>${esc(headersJson)}</textarea>
    </label>
    <p class="status-line">Источник: ${esc(data.source || "NEXUS_CUSTOMER_DB_API_TOKEN")}${data.generated ? " · создан сейчас" : ""}</p>
    <div class="dialog__actions">
      <button id="copyCustomerDbTokenBtn" class="btn btn--sm" type="button">Копировать токен</button>
      <button id="copyCustomerDbHeadersBtn" class="btn btn--sm btn--primary" type="button">Копировать JSON заголовков</button>
    </div>`;
  $("copyCustomerDbTokenBtn").addEventListener("click", () => copyText(data.token || "", "Токен скопирован"));
  $("copyCustomerDbHeadersBtn").addEventListener("click", () => copyText(headersJson, "JSON заголовков скопирован"));
}

async function copyText(text, message) {
  try {
    await navigator.clipboard.writeText(text);
    notify(message, "ok");
  } catch {
    notify("Не удалось скопировать", "error");
  }
}

$("moduleSettingsCloseBtn").addEventListener("click", () => { $("moduleSettingsOverlay").hidden = true; });
$("moduleSettingsOverlay").addEventListener("click", e => {
  if (e.target === $("moduleSettingsOverlay")) $("moduleSettingsOverlay").hidden = true;
});

// ── Docs dialog ───────────────────────────────────────────────────────────────

$("mtDocs").addEventListener("click", async () => {
  if (!activeModuleId) return;
  const m = modulesCache[activeModuleId];
  $("docsTitle").textContent = m?.name ?? activeModuleId;

  const res = await withGlobalBusy(fetch(`${RP}/${activeModuleId}/panel/docs.html`).catch(() => null), "Загрузка документации");
  const body = $("docsBody");
  if (res?.ok) {
    const html = await res.text();
    const match = html.match(/<body[^>]*>([\s\S]*?)<\/body>/i);
    body.innerHTML = `<div class="docs-body">${match ? match[1] : html}</div>`;
  } else {
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
    const res = await withGlobalBusy(fetch(RP + "/api/modules/upload", { method: "POST", body: fd }), "Установка модуля");
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
  const res = await withGlobalBusy(fetch(RP + "/api/modules"), "Загрузка Nexus");
  if (!res.ok) return;
  const list = await res.json();
  renderModules(list);

  // hash routing — открыть модуль из URL
  const route = getHashRoute();
  if (route.id && modulesCache[route.id]) {
    selectModule(route.id, false, route.path);
  }
})();
