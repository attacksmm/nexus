// Nexus Orchestrator — main UI
let activeModuleId = null;

const $ = id => document.getElementById(id);

const moduleList   = $("moduleList");
const contentWelcome = $("contentWelcome");
const contentModule  = $("contentModule");
const topbarCrumb    = $("topbarCrumb");

// toolbar elements
const mtName   = $("mtName");
const mtVersion = $("mtVersion");
const mtStatus  = $("mtStatus");
const mtPause  = $("mtPause");
const mtResume = $("mtResume");
const mtUnload = $("mtUnload");

// upload dialog
const uploadBtn    = $("uploadBtn");
const uploadOverlay = $("uploadOverlay");
const uploadClose  = $("uploadCloseBtn");
const uploadZone   = $("uploadZone");
const uploadInput  = $("uploadInput");
const uploadZoneText = $("uploadZoneText");
const uploadStatus = $("uploadStatus");
const uploadSubmit = $("uploadSubmitBtn");

let modules = {};  // id → meta

// ── Module list ──────────────────────────────────────────────────

function renderModules(list) {
  modules = {};
  moduleList.innerHTML = "";
  if (!list.length) {
    moduleList.innerHTML = '<p class="sidebar__empty">Нет модулей</p>';
    return;
  }
  for (const m of list) {
    modules[m.id] = m;
    const btn = document.createElement("button");
    btn.className = "module-item" + (m.id === activeModuleId ? " module-item--active" : "");
    btn.dataset.id = m.id;
    btn.dataset.status = m.status;
    btn.type = "button";
    btn.innerHTML = `
      <span class="module-item__dot module-item__dot--${m.status}"></span>
      <span class="module-item__name">${m.name}</span>
      <span class="module-item__ver">v${m.version}</span>`;
    btn.addEventListener("click", () => selectModule(m.id));
    moduleList.appendChild(btn);
  }
}

async function refreshModules() {
  const res = await fetch("/api/modules");
  if (!res.ok) return;
  const list = await res.json();
  renderModules(list);
  if (activeModuleId && modules[activeModuleId]) {
    updateToolbar(modules[activeModuleId]);
  }
}

// ── Select module ────────────────────────────────────────────────

function selectModule(id) {
  activeModuleId = id;
  const m = modules[id];
  if (!m) return;

  // sidebar highlight
  for (const btn of moduleList.querySelectorAll(".module-item")) {
    btn.classList.toggle("module-item--active", btn.dataset.id === id);
  }

  // topbar path
  topbarCrumb.textContent = m.name;

  // toolbar
  updateToolbar(m);

  // show module view
  contentWelcome.hidden = true;
  contentModule.hidden = false;

  // load panel in iframe
  const frame = $("moduleFrame");
  if (m.status === "active") {
    frame.src = `/m/${id}/panel/index.html`;
  } else {
    frame.src = "about:blank";
  }
}

function updateToolbar(m) {
  mtName.textContent = m.name;
  mtVersion.textContent = "v" + m.version;
  mtStatus.textContent = statusLabel(m.status);
  mtStatus.className = "module-toolbar__status module-toolbar__status--" + m.status;

  const isPaused = m.status === "paused";
  const isActive = m.status === "active";
  mtPause.hidden  = !isActive;
  mtResume.hidden = !isPaused;
}

function statusLabel(s) {
  return { active: "активен", paused: "пауза", unloaded: "выгружен", error: "ошибка" }[s] || s;
}

// ── Module actions ───────────────────────────────────────────────

async function moduleAction(action) {
  if (!activeModuleId) return;
  const res = await fetch(`/api/modules/${activeModuleId}/${action}`, { method: "POST" });
  if (!res.ok) {
    const e = await res.json().catch(() => ({}));
    alert(e.error || "Ошибка");
    return;
  }
  if (action === "unload") {
    activeModuleId = null;
    contentWelcome.hidden = false;
    contentModule.hidden = true;
    topbarCrumb.textContent = "Оркестратор";
  }
  await refreshModules();
  if (activeModuleId && modules[activeModuleId]) {
    selectModule(activeModuleId);
  }
}

mtPause.addEventListener("click",  () => moduleAction("pause"));
mtResume.addEventListener("click", () => moduleAction("resume"));
mtUnload.addEventListener("click", () => {
  if (confirm(`Выгрузить модуль "${modules[activeModuleId]?.name}"? Файлы будут удалены.`)) {
    moduleAction("unload");
  }
});

// ── Upload dialog ────────────────────────────────────────────────

let selectedFile = null;

uploadBtn.addEventListener("click", () => { uploadOverlay.hidden = false; });
uploadClose.addEventListener("click", closeUpload);
uploadOverlay.addEventListener("click", e => { if (e.target === uploadOverlay) closeUpload(); });

function closeUpload() {
  uploadOverlay.hidden = true;
  selectedFile = null;
  uploadInput.value = "";
  uploadZoneText.textContent = "Перетащите .zip или нажмите для выбора";
  uploadStatus.textContent = "";
  uploadStatus.className = "status-line";
  uploadSubmit.disabled = true;
}

uploadZone.addEventListener("click", () => uploadInput.click());
uploadInput.addEventListener("change", () => {
  selectedFile = uploadInput.files[0];
  if (selectedFile) {
    uploadZoneText.textContent = selectedFile.name;
    uploadSubmit.disabled = false;
  }
});

uploadZone.addEventListener("dragover", e => { e.preventDefault(); uploadZone.classList.add("upload-zone--drag"); });
uploadZone.addEventListener("dragleave", () => uploadZone.classList.remove("upload-zone--drag"));
uploadZone.addEventListener("drop", e => {
  e.preventDefault();
  uploadZone.classList.remove("upload-zone--drag");
  const f = e.dataTransfer.files[0];
  if (f?.name.endsWith(".zip")) {
    selectedFile = f;
    uploadZoneText.textContent = f.name;
    uploadSubmit.disabled = false;
  }
});

uploadSubmit.addEventListener("click", async () => {
  if (!selectedFile) return;
  uploadSubmit.disabled = true;
  uploadStatus.textContent = "Загрузка...";
  uploadStatus.className = "status-line";

  const fd = new FormData();
  fd.append("file", selectedFile);

  try {
    const res = await fetch("/api/modules/upload", { method: "POST", body: fd });
    const data = await res.json();
    if (!res.ok) {
      uploadStatus.textContent = data.error || "Ошибка";
      uploadStatus.className = "status-line status-line--err";
      uploadSubmit.disabled = false;
      return;
    }
    uploadStatus.textContent = `Модуль «${data.name}» v${data.version} установлен`;
    uploadStatus.className = "status-line status-line--ok";
    await refreshModules();
    setTimeout(closeUpload, 1200);
  } catch (e) {
    uploadStatus.textContent = "Сетевая ошибка";
    uploadStatus.className = "status-line status-line--err";
    uploadSubmit.disabled = false;
  }
});

// ── Init ─────────────────────────────────────────────────────────

(async () => {
  const res = await fetch("/api/modules");
  if (res.ok) {
    const list = await res.json();
    renderModules(list);
  }
})();
