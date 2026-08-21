(function () {
  "use strict";

  var script = document.currentScript;
  var API = String((script && script.dataset && script.dataset.nexusWazzupApi) || "").replace(/\/$/, "");
  var GUIDE_URL = API.replace(/\/widget$/, "/guide");
  var TEST_MODE = !!(script && script.dataset && script.dataset.nexusWazzupTest === "1");
  var TEST_SOURCE_URL = String((script && script.dataset && script.dataset.nexusWazzupSourceUrl) || "");
  var SUPPORTED = /\/(?:user\/control\/user\/update\/id|sales\/control\/deal\/update\/id)\/\d+(?:\/|$)/i;
  var CARD_PAGE = TEST_MODE || SUPPORTED.test(location.pathname);
  var STAFF_PAGE = /\/user\/control\/user(?:\/index)?\/?$/i.test(location.pathname);
  var STORAGE_KEY = "nexus:messenger-widget:device-token:v1";
  var PREFS_KEY = "nexus:messenger-widget:prefs:v1";
  var AUTO_OPEN_KEY = "nexus:messenger-widget:auto-open:v1";
  var BUTTON_ID = "nexus-messenger-widget-button";
  var DRAWER_ID = "nexus-messenger-widget-drawer";
  var INBOX_ID = "nexus-messenger-widget-inbox";
  var REQUEST_TIMEOUT_MS = 15000;
  var PALETTE_COLORS = ["#337ab7", "#46b45f", "#8b5cf6", "#d97706", "#374151"];
  [[STORAGE_KEY, "nexus:getcourse-wazzup:device-token:v1"], [PREFS_KEY, "nexus:getcourse-wazzup:prefs:v1"], [AUTO_OPEN_KEY, "nexus:getcourse-wazzup:auto-open:v1"]].forEach(function (keys) {
    if (!localStorage.getItem(keys[0]) && localStorage.getItem(keys[1])) localStorage.setItem(keys[0], localStorage.getItem(keys[1]));
  });
  if (!API || document.getElementById(INBOX_ID)) return;
  var forcedContext = null;

  function openGuide() {
    window.open(GUIDE_URL, "_blank", "noopener");
  }

  function isAdminShell() {
    if (TEST_MODE) return true;
    if (!document.body || !document.body.classList.contains("gc-user-logined")) return false;
    var bar = document.querySelector(".gc-account-leftbar");
    if (!bar) return false;
    var text = String(bar.textContent || "");
    return ["Ученики", "CRM", "Продажи"].filter(function (label) { return text.indexOf(label) !== -1; }).length >= 2;
  }

  function readPrefs() {
    var value = {};
    try { value = JSON.parse(localStorage.getItem(PREFS_KEY) || "{}"); } catch (error) { value = {}; }
    var color = /^#[0-9a-f]{6}$/i.test(String(value.color || "")) ? String(value.color) : "#337ab7";
    var positions = ["bottom-left", "bottom-right", "top-left", "top-right"];
    var sizes = ["small", "medium", "large"];
    var themes = ["light", "gray", "dark"];
    return {
      color: color,
      theme: themes.indexOf(value.theme) >= 0 ? value.theme : "light",
      position: positions.indexOf(value.position) >= 0 ? value.position : "bottom-left",
      inboxSize: sizes.indexOf(value.inboxSize) >= 0 ? value.inboxSize : "medium",
      drawerSize: sizes.indexOf(value.drawerSize) >= 0 ? value.drawerSize : "medium",
      inboxChannels: Array.isArray(value.inboxChannels) ? value.inboxChannels.map(String).slice(0, 30) : []
    };
  }

  function mixWhite(hex, amount) {
    var raw = String(hex || "#337ab7").slice(1);
    return "#" + [0, 2, 4].map(function (index) {
      var value = parseInt(raw.slice(index, index + 2), 16);
      return Math.round(value + (255 - value) * amount).toString(16).padStart(2, "0");
    }).join("");
  }

  function shadeColor(hex, factor) {
    var raw = String(hex || "#337ab7").slice(1);
    return "#" + [0, 2, 4].map(function (index) {
      return Math.round(parseInt(raw.slice(index, index + 2), 16) * factor).toString(16).padStart(2, "0");
    }).join("");
  }

  function applyPrefs(view) {
    if (!view) return;
    var prefs = readPrefs();
    var accent = prefs.theme === "dark" ? shadeColor(prefs.color, 0.7) : prefs.color;
    view.wrap.dataset.position = prefs.position;
    view.wrap.dataset.inboxSize = prefs.inboxSize;
    view.wrap.dataset.theme = prefs.theme;
    view.wrap.style.setProperty("--accent", accent);
    view.wrap.style.setProperty("--accent-soft", mixWhite(accent, 0.82));
    view.wrap.style.setProperty("--accent-faint", mixWhite(accent, 0.93));
    view.wrap.style.setProperty("--outgoing-bg", prefs.theme === "dark" ? shadeColor(prefs.color, 0.45) : mixWhite(accent, prefs.theme === "gray" ? 0.48 : 0.82));
    view.wrap.style.setProperty("--outgoing-border", accent);
  }

  function applyDrawerPrefs(view) {
    if (!view) return;
    var prefs = readPrefs();
    var accent = prefs.theme === "dark" ? shadeColor(prefs.color, 0.7) : prefs.color;
    view.layer.dataset.drawerSize = prefs.drawerSize;
    view.layer.dataset.theme = prefs.theme;
    view.layer.style.setProperty("--accent", accent);
    view.layer.style.setProperty("--accent-soft", mixWhite(accent, 0.82));
    view.layer.style.setProperty("--accent-faint", mixWhite(accent, 0.93));
    view.layer.style.setProperty("--outgoing-bg", prefs.theme === "dark" ? shadeColor(prefs.color, 0.45) : mixWhite(accent, prefs.theme === "gray" ? 0.48 : 0.82));
    view.layer.style.setProperty("--outgoing-border", accent);
  }

  function applyButtonPrefs() {
    var host = document.getElementById(BUTTON_ID);
    if (!host) return;
    var prefs = readPrefs();
    var accent = prefs.theme === "dark" ? shadeColor(prefs.color, 0.7) : prefs.color;
    host.style.setProperty("--accent", accent);
    host.style.setProperty("--accent-soft", mixWhite(accent, 0.82));
  }

  function wheelScrollX(node) {
    node.addEventListener("wheel", function (event) {
      if (Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
      node.scrollLeft += event.deltaY;
      event.preventDefault();
    }, { passive: false });
  }

  function wheelScrollY(node) {
    node.addEventListener("wheel", function (event) {
      if (!event.deltaY || Math.abs(event.deltaX) > Math.abs(event.deltaY)) return;
      var max = node.scrollHeight - node.clientHeight;
      var next = Math.max(0, Math.min(max, node.scrollTop + event.deltaY));
      if (next === node.scrollTop) return;
      node.scrollTop = next;
      event.preventDefault();
    }, { passive: false });
  }

  function normalizePhone(value) {
    var digits = String(value || "").replace(/\D+/g, "");
    if (digits.length === 10) digits = "7" + digits;
    else if (digits.length === 11 && digits.charAt(0) === "8") digits = "7" + digits.slice(1);
    return digits.length >= 8 && digits.length <= 15 ? "+" + digits : "";
  }

  function phoneFromNode(node) {
    if (!node) return "";
    var href = node.getAttribute && node.getAttribute("href");
    var raw = href && /^tel:/i.test(href) ? href.slice(4) : node.textContent;
    return normalizePhone(raw);
  }

  function findPhone() {
    var selectors = [".user-call-to-phone", "a[href^='tel:']", ".user-phone", "[data-field-name*='phone' i]", "[data-field-name*='телефон' i]"];
    for (var s = 0; s < selectors.length; s += 1) {
      var nodes = document.querySelectorAll(selectors[s]);
      for (var n = 0; n < nodes.length; n += 1) {
        var value = phoneFromNode(nodes[n]);
        if (value) return value;
      }
    }
    var scopes = document.querySelectorAll(".user-card,.gc-user-user-info,.panel-body,.content-menu,body");
    for (var i = 0; i < scopes.length; i += 1) {
      var match = String(scopes[i].textContent || "").match(/(?:\+?7|8)?[\s\-()]?\d[\d\s\-()]{8,}\d/);
      var phone = normalizePhone(match && match[0]);
      if (phone) return phone;
    }
    return "";
  }

  function findName() {
    var node = document.querySelector("h1,.page-header h2,.user-name");
    var value = String((node && node.textContent) || "").trim();
    return value.slice(0, 200);
  }

  function findEmail() {
    var node = document.querySelector("a[href^='mailto:'],.user-email,[data-field-name*='email' i]");
    var raw = String((node && ((node.getAttribute && node.getAttribute("href")) || node.textContent)) || "");
    var match = raw.replace(/^mailto:/i, "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    if (match) return match[0].toLowerCase().slice(0, 320);
    match = String(document.body.textContent || "").match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
    return match ? match[0].toLowerCase().slice(0, 320) : "";
  }

  function findVkId() {
    var direct = document.querySelector("a[href*='vk.com/'],a[href*='vkontakte.ru/'],input[value*='vk.com/'],input[value*='vkontakte.ru/']");
    if (direct) return String(direct.getAttribute("href") || direct.value || "").slice(0, 1000);
    var labels = document.querySelectorAll("label,dt,.field-label,.control-label");
    for (var i = 0; i < labels.length; i += 1) {
      if (!/^(vkontakte|вконтакте|vk)$/i.test(String(labels[i].textContent || "").trim())) continue;
      var scope = labels[i].closest(".form-group,.field,.custom-field") || labels[i].parentElement;
      var input = scope && scope.querySelector("input,a[href]");
      var value = String(input && (input.value || input.getAttribute("href")) || "").trim();
      if (value) return value.slice(0, 1000);
    }
    return "";
  }

  function findTelegramIdentity() {
    var result = { telegram_id: "", telegram_username: "" };
    var direct = document.querySelector("a[href*='t.me/'],input[value*='t.me/']");
    if (direct) {
      var raw = String(direct.getAttribute("href") || direct.value || "").trim();
      var match = raw.match(/t\.me\/([A-Za-z0-9_]{5,})/i);
      if (match) result.telegram_username = match[1];
    }
    var labels = document.querySelectorAll("label,dt,.field-label,.control-label");
    for (var i = 0; i < labels.length; i += 1) {
      var label = String(labels[i].textContent || "").trim();
      if (!/telegram|телеграм/i.test(label)) continue;
      var scope = labels[i].closest(".form-group,.field,.custom-field") || labels[i].parentElement;
      var input = scope && scope.querySelector("input,a[href]");
      var value = String(input && (input.value || input.getAttribute("href") || input.textContent) || "").trim();
      if (!result.telegram_id && /\bid\b/i.test(label) && /^\d{5,}$/.test(value)) result.telegram_id = value;
      if (!result.telegram_username) {
        var username = value.match(/(?:t\.me\/|@)([A-Za-z0-9_]{5,})/i);
        if (username) result.telegram_username = username[1];
      }
    }
    return result;
  }

  function visibleSourceFields() {
    var fields = {};
    document.querySelectorAll("tr").forEach(function (row) {
      var cells = row.querySelectorAll("th,td");
      if (cells.length < 2) return;
      var key = String(cells[0].textContent || "").trim().toLowerCase();
      if (!/^utm_(source|medium|campaign|content|term)$/.test(key)) return;
      var value = String(cells[cells.length - 1].textContent || "").trim().slice(0, 2000);
      if (value) fields[key] = value;
    });
    return fields;
  }

  function context() {
    if (forcedContext) return forcedContext;
    var telegram = findTelegramIdentity();
    var fields = visibleSourceFields();
    document.querySelectorAll("input[name],textarea[name],select[name],[data-field-name]").forEach(function (node) {
      var key = String(node.getAttribute("data-field-name") || node.name || "").trim().slice(0, 200);
      var value = String(node.value || node.textContent || "").trim().slice(0, 2000);
      if (key && value && !/password|token|secret|cookie/i.test(key)) fields[key] = value;
    });
    return { platform: "getcourse", phone: findPhone(), email: findEmail(), vk_id: findVkId(), telegram_id: telegram.telegram_id, telegram_username: telegram.telegram_username, name: findName(), source_url: TEST_MODE ? TEST_SOURCE_URL : location.href, fields: fields };
  }

  function optimisticTemplate(body, source) {
    source = source || {};
    var fields = source.fields || {}, name = String(source.name || "");
    var values = {
      "contact.name": name, "contact.first_name": name.split(/\s+/)[0] || "",
      "contact.phone": source.phone || "", "contact.email": source.email || "",
      "manager.name": source.manager_name || fields.manager_name || "",
      "amo.lead.id": source.entity_type === "lead" ? source.entity_id || "" : "",
      "amo.contact.id": source.entity_type === "contact" ? source.entity_id || "" : fields.contact_id || "",
      "getcourse.user.id": source.platform === "getcourse" ? source.entity_id || "" : "",
      "vk.id": source.vk_id || "", "telegram.id": source.telegram_id || ""
    };
    ["source", "medium", "campaign", "content", "term"].forEach(function (key) {
      values["utm." + key] = fields["utm_" + key] || fields["utm." + key] || "";
    });
    return String(body || "").replace(/\{\{\s*([^{}]+?)\s*\}\}/g, function (_, key) {
      return String(values[key] !== undefined ? values[key] : (fields[key] !== undefined ? fields[key] : fields[key.replace(/\./g, "_")] || ""));
    });
  }

  function flashTemplateInput(input) {
    input.classList.remove("template-applied");
    void input.offsetWidth;
    input.classList.add("template-applied");
    setTimeout(function () { input.classList.remove("template-applied"); }, 360);
  }

  function shadowHost(id) {
    var host = document.createElement("div");
    host.id = id;
    var root = host.attachShadow ? host.attachShadow({ mode: "open" }) : host;
    return { host: host, root: root };
  }

  function buttonCss() {
    return ":host{--accent:#337ab7;--accent-soft:#d6e4f1}*{box-sizing:border-box}.button{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:30px;padding:5px 10px;border:1px solid var(--accent);border-radius:3px;background:var(--accent);color:#fff;font:600 12px/1.2 Arial,sans-serif;cursor:pointer;box-shadow:none;transition:filter .12s,transform .1s}.button:hover{filter:brightness(.92)}.button:active{transform:translateY(1px)}.button:focus{outline:2px solid var(--accent-soft);outline-offset:2px}.mark{width:7px;height:7px;border-radius:50%;background:#67d391}@media(prefers-reduced-motion:reduce){.button{transition:none}}";
  }

  function inboxCss() {
    return [
      ":host{all:initial}",
      "*{box-sizing:border-box}",
      ".wrap{--accent:#337ab7;--accent-soft:#d6e4f1;--accent-faint:#f1f6fa;--outgoing-bg:#d6e4f1;--outgoing-border:#337ab7;--inbox-width:620px;--inbox-height:720px;position:fixed;left:100px;bottom:18px;z-index:2147483500;font-family:Arial,sans-serif;color:#17212b}.wrap[data-inbox-size='small']{--inbox-width:380px;--inbox-height:560px}.wrap[data-inbox-size='large']{--inbox-width:920px;--inbox-height:860px}.wrap[data-position$='right']{left:auto;right:18px}.wrap[data-position^='top']{top:18px;bottom:auto}",
      ".launcher{position:absolute;left:0;bottom:0;width:50px;height:50px;display:grid;place-items:center;border:0;border-radius:50%;background:var(--accent);color:#fff;cursor:pointer;box-shadow:0 8px 24px rgba(27,54,77,.25);transition:transform .16s,box-shadow .16s}.wrap[data-position$='right'] .launcher{left:auto;right:0}.wrap[data-position^='top'] .launcher{top:0;bottom:auto}.launcher:hover{transform:translateY(-1px);box-shadow:0 10px 28px rgba(27,54,77,.3)}.launcher svg{width:25px;height:25px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}.launcher.attention{animation:unanswered 2.4s ease-in-out infinite}",
      ".badge{position:absolute;right:-4px;top:-5px;min-width:21px;height:21px;padding:0 5px;border:2px solid #fff;border-radius:12px;background:#e33b32;color:#fff;font:700 11px/17px Arial,sans-serif;text-align:center}.badge[hidden]{display:none}",
      ".panel{position:absolute;left:0;bottom:62px;width:min(var(--inbox-width),calc(100vw - 20px));height:min(var(--inbox-height),calc(100vh - 96px));display:grid;grid-template-rows:48px minmax(0,1fr);border:1px solid #b9c5ce;border-radius:8px;background:#fff;box-shadow:0 18px 54px rgba(22,42,58,.25);opacity:0;transform:translateY(8px) scale(.985);transform-origin:bottom left;pointer-events:none;transition:opacity .15s,transform .15s,width .16s,height .16s;overflow:hidden}.wrap[data-position$='right'] .panel{left:auto;right:0;transform-origin:bottom right}.wrap[data-position^='top'] .panel{top:62px;bottom:auto;transform:translateY(-8px) scale(.985);transform-origin:top left}.wrap[data-position='top-right'] .panel{transform-origin:top right}",
      ".wrap.open .panel{opacity:1;transform:none;pointer-events:auto}",
      ".panel-head{display:flex;align-items:center;gap:7px;padding:0 9px;border-bottom:1px solid #d7dfe5;background:var(--accent-faint)}.panel-head b{min-width:0;flex:1;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.icon{width:30px;height:30px;border:1px solid #bac5ce;border-radius:5px;background:#fff;color:#334b5c;font:700 16px Arial,sans-serif;cursor:pointer;transition:background-color .12s,transform .1s}.icon:hover{background:var(--accent-soft)}.icon:active{transform:translateY(1px)}.back[hidden]{display:none}",
      ".list-view{min-height:0;display:grid;grid-template-rows:auto minmax(0,1fr);background:#fff}.list{min-height:0;overflow-y:auto;overflow-x:hidden;background:#fff}.list,.message-feed,.settings{scrollbar-width:none}.list::-webkit-scrollbar,.message-feed::-webkit-scrollbar,.settings::-webkit-scrollbar{display:none}.inbox-filter{position:relative;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:6px;padding:7px;border-bottom:1px solid #d7dfe5;background:#fff}.inbox-search{min-width:0;height:32px;padding:0 9px;border:1px solid #bac5ce;border-radius:3px;color:#17212b}.filter-button{height:32px;padding:0 9px;border:1px solid #bac5ce;border-radius:3px;background:#fff;color:#334b5c;font-weight:600;cursor:pointer}.channel-menu{position:absolute;z-index:3;top:43px;right:7px;width:min(290px,calc(100vw - 40px));max-height:260px;overflow:auto;border:1px solid #bac5ce;background:#fff;box-shadow:0 10px 30px rgba(22,42,58,.18);scrollbar-width:none}.channel-menu::-webkit-scrollbar{display:none}.channel-option{display:flex;align-items:center;gap:8px;padding:9px;border-bottom:1px solid #e4e9ed;color:#334b5c;font:12px Arial,sans-serif}.empty{padding:34px 20px;color:#6c7e8c;font:13px/1.5 Arial,sans-serif;text-align:center}.inbox-loading{display:flex;min-height:120px;align-items:center;justify-content:center;gap:9px}.inbox-loading .spinner,.icon.busy:before{display:block;width:15px;height:15px;flex:0 0 15px;border:2px solid #c9d3dc;border-top-color:var(--accent);border-radius:50%;animation:spin .75s linear infinite}.icon.busy{font-size:0;cursor:wait}.icon.busy:before{content:'';margin:auto}.thread{width:100%;min-width:0;display:grid;grid-template-columns:38px minmax(0,1fr) 24px;gap:9px;align-items:center;padding:10px 12px;border:0;border-bottom:1px solid #e3e8ec;background:#fff;color:#17212b;text-align:left;cursor:pointer;transition:background-color .12s,transform .1s}.thread:hover{background:#f3f7fa}.thread:active{transform:translateY(1px)}.thread.new{background:#edf6fc;animation:newMessage .45s ease}.avatar{width:38px;height:38px;display:grid;place-items:center;border-radius:50%;background:#dfeaf2;color:#2d688f;font:700 13px Arial,sans-serif}.copy,.name,.meta,.preview{display:block;min-width:0}.name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font:700 13px/1.25 Arial,sans-serif}.meta,.preview{margin-top:3px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#738492;font:11px/1.25 Arial,sans-serif}.preview{color:#415565;font-size:12px}.count{min-width:20px;height:20px;padding:0 5px;border-radius:10px;background:#e33b32;color:#fff;font:700 11px/20px Arial,sans-serif;text-align:center}.count[hidden]{visibility:hidden}",
      ".chat{height:100%;min-height:0;display:grid;grid-template-rows:auto minmax(0,1fr) auto;background:#eef2f5}.chat-tools{display:flex;align-items:center;gap:6px;padding:7px;border-bottom:1px solid #d4dde4;background:#fff;overflow:hidden}.channel-strip{display:flex;flex:1;gap:5px;min-width:0;overflow-x:auto;overscroll-behavior:contain;scrollbar-width:none}.channel-strip::-webkit-scrollbar{display:none}.channel{flex:0 0 auto;height:30px;padding:0 9px;border:1px solid #b8c4ce;border-radius:5px;background:#fff;color:#334b5c;font:600 11px Arial,sans-serif;white-space:nowrap;cursor:pointer;transition:background-color .12s,border-color .12s,transform .1s}.channel:active{transform:translateY(1px)}.channel.active{border-color:var(--accent);background:var(--accent);color:#fff}.channel:disabled{border-color:#d4dbe0;background:#eef1f3;color:#9aa6ad;cursor:default}.card-link{width:32px;padding:0;text-decoration:none;display:grid;place-items:center;color:var(--accent)}.card-link svg{width:17px;height:17px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}.message-feed{min-height:0;overflow:auto;padding:10px}.history-note,.empty-chat{margin:7px auto;padding:7px 9px;width:min(340px,100%);border:1px solid #cad4dc;background:#fff;color:#66788a;text-align:center;font:11px/1.4 Arial,sans-serif}.message-row{display:flex;margin:7px 0}.message-row.outgoing{justify-content:flex-end}.bubble{max-width:82%;padding:8px 9px;border:1px solid #d5dde3;border-radius:8px;background:#fff;color:#17212b;font:12px/1.42 Arial,sans-serif;white-space:pre-wrap;overflow-wrap:anywhere}.outgoing .bubble{border-color:var(--outgoing-border);background:var(--outgoing-bg)}.message-meta{margin-top:4px;color:#81909b;font-size:9px;text-align:right}.attachment{display:block;margin-top:6px;color:var(--accent)}.message-image-link{display:block;margin:-3px -4px 6px}.message-image{display:block;max-width:100%;max-height:260px;border-radius:5px;object-fit:contain}.composer{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:7px;padding:8px;border-top:1px solid #ccd6de;background:#fff}.composer textarea{min-height:40px;max-height:100px;resize:none;padding:8px;border:1px solid #aab7c2;border-radius:5px;font:12px/1.4 Arial,sans-serif}.send{min-width:82px;border:0;border-radius:5px;background:var(--accent);color:#fff;font:700 11px Arial,sans-serif;cursor:pointer;transition:filter .12s,transform .1s}.send:active{transform:translateY(1px)}.send:disabled{opacity:.5}.compose-error{grid-column:1/-1;color:#a23a3a;font-size:10px}",
      ".list,.message-feed,.settings{overscroll-behavior:contain;touch-action:pan-y}.composer-menu{position:relative}.composer-more{width:34px;height:42px;border:1px solid #aab7c2;border-radius:5px;background:#fff;color:#334b5c;font:700 18px/1 Arial,sans-serif;cursor:pointer}.composer-popover{position:absolute;right:0;bottom:48px;z-index:5;width:230px;max-height:290px;overflow:auto;border:1px solid #b8c4ce;background:#fff;box-shadow:0 10px 30px rgba(22,42,58,.2);scrollbar-width:none}.composer-popover::-webkit-scrollbar{display:none}.composer-menu-button{display:block;width:100%;padding:10px;border:0;border-bottom:1px solid #e3e8ec;background:#fff;color:#263746;text-align:left;font:600 12px Arial,sans-serif;cursor:pointer}.composer-menu-button:hover{background:#edf3f7}.composer-menu-button small{display:block;margin-top:3px;color:#788895;font:11px/1.3 Arial,sans-serif;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.composer-menu-button.menu-back{color:#66788a}.composer-menu-empty{padding:12px;color:#788895;font:12px Arial,sans-serif}",
      ".composer textarea{min-height:52px;max-height:min(45vh,360px);resize:none;overflow-y:hidden;scrollbar-width:thin;scrollbar-color:#87959d transparent;transition:border-color .12s,box-shadow .12s}.composer textarea::-webkit-scrollbar{width:8px}.composer textarea::-webkit-scrollbar-track{background:transparent}.composer textarea::-webkit-scrollbar-thumb{background:#87959d;border:2px solid transparent;background-clip:padding-box;border-radius:8px}.composer textarea.template-applied{border-color:var(--accent);box-shadow:0 0 0 2px rgba(51,122,183,.14)}.composer-menu-button,.composer-more,.template-star{transition:background-color .12s,border-color .12s,transform .1s}.composer-menu-button:active,.composer-more:active,.template-star:active{transform:translateY(1px)}",
      ".composer-input{position:relative;min-width:0}.composer-input textarea{width:100%;padding-right:40px}.composer-menu{position:absolute;top:5px;right:5px;z-index:4}.composer-more{width:28px;height:28px;border:0;border-radius:3px;background:transparent;color:#334b5c;font:700 16px/1 Arial,sans-serif;cursor:pointer}.composer-popover{top:32px;bottom:auto}.composer-menu.open-up .composer-popover{top:auto;bottom:32px}.channel-option input{width:14px;height:14px;flex:0 0 14px;margin:0;accent-color:var(--accent);cursor:pointer}.composer-template-row[draggable='true']{grid-template-columns:22px minmax(0,1fr) 38px}.template-drag{display:grid;place-items:center;color:#8b98a1;cursor:grab}.composer-template-row.dragging{opacity:.45}.composer-template-row.drag-over{box-shadow:inset 0 2px var(--accent)}.composer-menu-loading{min-height:90px;display:flex;align-items:center;justify-content:center;gap:8px;padding:12px;color:#66788a;font:12px Arial,sans-serif}.composer-menu-loading .spinner{width:15px;height:15px;margin:0}",
      ".send-all{display:flex;align-items:center;gap:5px;flex:0 0 auto;color:#415565;font:600 11px Arial,sans-serif;white-space:nowrap}.send-all input{width:14px;height:14px;margin:0;accent-color:var(--accent)}",
      ".settings{padding:18px;overflow:auto}.field{display:grid;gap:7px;margin-bottom:18px;color:#415565;font:600 12px Arial,sans-serif}.color{width:100%;height:42px;padding:3px;border:1px solid #bac5ce;border-radius:6px;background:#fff}.positions{display:grid;grid-template-columns:1fr 1fr;gap:7px}.sizes,.themes{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}.palettes{display:flex;gap:8px}.palette{width:34px;height:34px;border:2px solid #fff;border-radius:50%;box-shadow:0 0 0 1px #bac5ce;cursor:pointer}.palette.active{box-shadow:0 0 0 2px var(--accent)}.position,.size,.theme,.reset{min-height:36px;border:1px solid #bac5ce;border-radius:6px;background:#fff;color:#334b5c;font:600 12px Arial,sans-serif;cursor:pointer}.position.active,.size.active,.theme.active{border-color:var(--accent);background:var(--accent-soft)}.reset{width:100%}.logout{margin-top:10px;border-color:#bd7474;color:#983a3a}.logout.busy{display:flex;align-items:center;justify-content:center;gap:7px}.logout.busy:before{content:'';width:12px;height:12px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .7s linear infinite}",
      ".notify-loading{min-height:140px;display:flex;align-items:center;justify-content:center;gap:9px;color:#6c7e8c;font:12px Arial,sans-serif}.notify-loading .spinner{width:16px;height:16px;border:2px solid #c9d3dc;border-top-color:var(--accent);border-radius:50%;animation:spin .75s linear infinite}.notify-card{margin-bottom:10px;padding:12px;border:1px solid #d7dfe5;background:#fff}.notify-card-head{display:flex;justify-content:space-between;gap:10px;align-items:center}.notify-card b{font:700 13px Arial,sans-serif}.notify-state{color:#71828f;font:11px Arial,sans-serif}.notify-state.ok{color:#2d7d46}.notify-state.waiting{display:inline-flex;align-items:center;gap:6px}.notify-state.waiting .spinner{width:11px;height:11px}.notify-help{margin:8px 0;color:#647684;font:11px/1.4 Arial,sans-serif}.notify-actions{display:flex;gap:7px;flex-wrap:wrap}.notify-action{min-height:34px;padding:0 10px;border:1px solid #bac5ce;border-radius:4px;background:#fff;color:#334b5c;font:600 11px Arial,sans-serif;cursor:pointer}.notify-action.primary{border-color:var(--accent);background:var(--accent);color:#fff}.notify-action:disabled{opacity:.58;cursor:wait}.notify-action.busy{display:inline-flex;align-items:center;gap:6px}.notify-action.busy:before{content:'';width:11px;height:11px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .7s linear infinite}.notify-pairing{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:9px;padding:9px;border:1px solid #b8c4ce;background:#f4f7f9;color:#334b5c;font:11px/1.45 Arial,sans-serif;overflow-wrap:anywhere}.notify-pairing>span:last-of-type{flex:1 1 260px}.notify-code{font-family:monospace;letter-spacing:.02em}.notify-routing{margin:14px 0;padding:10px 12px;border:1px solid #d7dfe5;color:#415565;font:12px/1.45 Arial,sans-serif}.notify-routing summary{cursor:pointer;font-weight:700}.notify-fallback{display:flex;gap:8px;align-items:flex-start;margin:12px 0;color:#415565;font:12px/1.4 Arial,sans-serif}",
      ".operation-list{display:grid;gap:8px}.operation-row{display:grid;grid-template-columns:minmax(120px,.8fr) minmax(130px,1fr) minmax(160px,1.5fr);gap:9px;padding:10px;border:1px solid #d7dfe5;color:#334b5c;font:12px/1.4 Arial,sans-serif}.operation-row b,.operation-row small{display:block}.operation-row small{margin-top:4px;color:#71828f}.operation-result{white-space:pre-wrap;overflow-wrap:anywhere}.operation-error{margin:0 0 6px;color:#a23a3a;font-weight:700}.operation-state{display:flex;align-items:center;gap:6px;margin-top:4px;color:#647684}.operation-state.success{color:#2d7d46}.operation-state.failed,.operation-state.dead{color:#a23a3a}.operation-state .spinner{width:12px;height:12px;flex:0 0 12px;margin:0}.operation-empty{padding:34px 12px;color:#71828f;text-align:center}@media(max-width:520px){.operation-row{grid-template-columns:1fr}}",
      ".wrap[data-theme='dark'] .panel,.wrap[data-theme='dark'] .panel-head,.wrap[data-theme='dark'] .list-view,.wrap[data-theme='dark'] .list,.wrap[data-theme='dark'] .thread,.wrap[data-theme='dark'] .inbox-filter,.wrap[data-theme='dark'] .chat,.wrap[data-theme='dark'] .chat-tools,.wrap[data-theme='dark'] .composer,.wrap[data-theme='dark'] .settings{background:#17212b;color:#edf3f7;border-color:#344553}.wrap[data-theme='dark'] .thread:hover,.wrap[data-theme='dark'] .composer-menu-button:hover{background:#22313d}.wrap[data-theme='dark'] .thread,.wrap[data-theme='dark'] .composer-menu-button,.wrap[data-theme='dark'] .composer-popover,.wrap[data-theme='dark'] .channel-menu,.wrap[data-theme='dark'] .channel,.wrap[data-theme='dark'] .icon,.wrap[data-theme='dark'] .filter-button,.wrap[data-theme='dark'] .position,.wrap[data-theme='dark'] .size,.wrap[data-theme='dark'] .theme,.wrap[data-theme='dark'] .reset,.wrap[data-theme='dark'] .notify-card,.wrap[data-theme='dark'] .notify-pairing,.wrap[data-theme='dark'] .notify-action,.wrap[data-theme='dark'] .operation-row,.wrap[data-theme='dark'] input,.wrap[data-theme='dark'] textarea{background:#1f2d38;color:#edf3f7;border-color:#465968}.wrap[data-theme='dark'] .message-feed{background:#14202a}.wrap[data-theme='dark'] .bubble,.wrap[data-theme='dark'] .history-note,.wrap[data-theme='dark'] .empty-chat{background:#22313d;color:#edf3f7;border-color:#465968}",
      ".wrap[data-theme='gray'],.wrap[data-theme='gray'] .message-feed{background:#92999d;color:#202b31}.wrap[data-theme='gray'] .panel,.wrap[data-theme='gray'] .panel-head,.wrap[data-theme='gray'] .list-view,.wrap[data-theme='gray'] .list,.wrap[data-theme='gray'] .thread,.wrap[data-theme='gray'] .inbox-filter,.wrap[data-theme='gray'] .chat,.wrap[data-theme='gray'] .chat-tools,.wrap[data-theme='gray'] .composer,.wrap[data-theme='gray'] .settings{background:#adb3b6;color:#202b31;border-color:#737d82}.wrap[data-theme='gray'] .bubble,.wrap[data-theme='gray'] .channel,.wrap[data-theme='gray'] .icon,.wrap[data-theme='gray'] .operation-row,.wrap[data-theme='gray'] input,.wrap[data-theme='gray'] textarea{background:#d2d5d7;color:#202b31;border-color:#737d82}.wrap[data-theme='gray'] .channel.active,.wrap[data-theme='gray'] .submit{background:var(--accent);border-color:var(--accent);color:#fff}",
      ".wrap[data-theme='gray'] .outgoing .bubble{background:var(--outgoing-bg);border-color:var(--outgoing-border)}",
      "@keyframes spin{to{transform:rotate(360deg)}}@keyframes newMessage{from{background:var(--accent-soft)}to{background:#edf6fc}}@keyframes unanswered{0%,100%{box-shadow:0 8px 24px rgba(27,54,77,.25)}50%{box-shadow:0 8px 24px rgba(27,54,77,.25),0 0 0 8px var(--accent-soft)}}",
      "@media(max-width:520px){.wrap{left:10px;bottom:72px}.wrap[data-position$='right']{left:auto;right:10px}.wrap[data-position^='top']{top:10px;bottom:auto}.panel,.wrap[data-position$='right'] .panel{position:fixed;left:8px;right:8px;width:auto;height:min(620px,calc(100dvh - 142px));transform-origin:center bottom}.wrap[data-position^='bottom'] .panel{bottom:132px;top:auto}.wrap[data-position^='top'] .panel{top:72px;bottom:auto;transform-origin:center top}}",
      "@media(prefers-reduced-motion:reduce){.launcher,.panel,.icon,.thread,.channel,.send{transition:none}.thread.new,.launcher.attention{animation:none}}"
    ].join("");
  }

  function placeButton() {
    if (document.getElementById(BUTTON_ID)) return true;
    var pair = shadowHost(BUTTON_ID);
    pair.root.innerHTML = '<style>' + buttonCss() + '</style><button class="button" type="button"><span class="mark"></span>Написать</button>';
    pair.root.querySelector("button").addEventListener("click", openDrawer);
    var actions = Array.prototype.slice.call(document.querySelectorAll("button,a,input[type='button'],input[type='submit']")).filter(function (node) {
      var label = String(node.textContent || node.value || "").replace(/\s+/g, " ").trim();
      if (!/^(общение с пользователем|написать пользователю)$/i.test(label)) return false;
      var rect = node.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0;
    });
    var target = actions.find(function (node) { return node.getBoundingClientRect().left > window.innerWidth * .62; });
    if (!target && TEST_MODE) target = actions[actions.length - 1];
    if (!target) {
      target = Array.prototype.slice.call(document.querySelectorAll(".user-call-to-phone,a[href^='tel:'],.user-phone")).find(function (node) {
        var rect = node.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0 && rect.left > window.innerWidth * .62;
      });
    }
    if (!target || !target.parentNode) return false;
    var targetRect = target.getBoundingClientRect();
    target.insertAdjacentElement("afterend", pair.host);
    applyButtonPrefs();
    pair.host.style.display = "block";
    pair.host.style.width = Math.ceil(targetRect.width) + "px";
    pair.host.style.margin = "6px 0 4px";
    pair.host.style.verticalAlign = "middle";
    pair.root.querySelector("button").style.width = "100%";
    pair.root.querySelector("button").style.height = Math.ceil(targetRect.height) + "px";
    return true;
  }

  function staffFromPage() {
    var rows = [];
    var seen = {};
    var links = document.querySelectorAll("a[href*='/user/control/user/update/id/']");
    for (var i = 0; i < links.length && rows.length < 100; i += 1) {
      var link = links[i];
      var match = String(link.getAttribute("href") || "").match(/\/user\/control\/user\/update\/id\/(\d+)/i);
      if (!match || seen[match[1]]) continue;
      seen[match[1]] = true;
      var row = link.closest("tr") || link.parentElement || link;
      var text = String(row.textContent || link.textContent || "").replace(/\s+/g, " ").trim();
      var name = String(link.textContent || "").replace(/\s+/g, " ").trim() || text.split(" · ")[0] || "Сотрудник " + match[1];
      var phoneMatch = text.match(/(?:\+?7|8)?[\s\-()]?\d[\d\s\-()]{8,}\d/);
      rows.push({ id: match[1], name: name.slice(0, 150), phone: phoneMatch ? phoneMatch[0] : "" });
    }
    return rows;
  }

  function placeStaffButton() {
    if (document.getElementById(BUTTON_ID)) return true;
    var pair = shadowHost(BUTTON_ID);
    pair.root.innerHTML = '<style>' + buttonCss() + '</style><button class="button" type="button"><span class="mark"></span>Синхронизировать сотрудников с Wazzup</button>';
    pair.root.querySelector("button").addEventListener("click", async function () {
      var token = localStorage.getItem(STORAGE_KEY) || "";
      if (!token) { window.alert("Сначала откройте карточку пользователя GetCourse и активируйте браузер кодом сотрудника."); return; }
      var staff = staffFromPage();
      if (!staff.length) { window.alert("На этой странице не найдены строки сотрудников. Откройте вкладку «Сотрудники» в разделе «Пользователи»."); return; }
      var button = pair.root.querySelector("button");
      button.disabled = true;
      try {
        var result = await request("/staff-sync", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify({ staff: staff }) });
        window.alert("Синхронизировано сотрудников: " + result.synced);
      } catch (error) {
        window.alert(error.message || "Не удалось синхронизировать сотрудников");
      } finally { button.disabled = false; }
    });
    var target = document.querySelector(".page-header,.content-menu,h1") || document.body.firstElementChild;
    if (!target || !target.parentNode) return false;
    target.insertAdjacentElement("afterend", pair.host);
    pair.host.style.display = "inline-block";
    pair.host.style.margin = "8px 0 8px 8px";
    return true;
  }

  function drawerCss() {
    return [
      ":host{all:initial}",
      "*{box-sizing:border-box}",
      ".layer{--accent:#337ab7;--accent-soft:#d6e4f1;--accent-faint:#f1f6fa;--outgoing-bg:#d6e4f1;--outgoing-border:#337ab7;position:fixed;inset:0;z-index:2147483600;pointer-events:none;font-family:Arial,sans-serif;color:#17212b}",
      ".backdrop{position:absolute;inset:0;background:rgba(10,16,24,.38);opacity:0;transition:opacity .16s;pointer-events:auto}",
      ".drawer{position:absolute;top:0;right:0;width:62vw;height:100dvh;background:#fff;border-left:1px solid #bac4ce;box-shadow:-18px 0 55px rgba(15,23,42,.2);display:grid;grid-template-columns:minmax(0,1fr);grid-template-rows:auto minmax(0,1fr);transform:translateX(100%);transition:transform .18s ease,width .16s;pointer-events:auto}.layer[data-drawer-size='small'] .drawer{width:44vw}.layer[data-drawer-size='large'] .drawer{width:84vw}",
      ".layer.open .backdrop{opacity:1}.layer.open .drawer{transform:none}",
      ".head{min-height:56px;display:grid;grid-template-columns:minmax(0,1fr) auto auto auto auto;align-items:center;gap:8px;padding:8px 12px;border-bottom:1px solid #d9e0e7;background:#f6f8fa}",
      ".title{min-width:0;grid-column:1;grid-row:1}.drawer-send-all{grid-column:2;grid-row:1}.copy{grid-column:3;grid-row:1}.drawer-settings{grid-column:4;grid-row:1}.close:not(.drawer-settings){grid-column:5;grid-row:1}.title b{display:block;font-size:14px;line-height:1.25}.title span{display:block;margin-top:2px;color:#66788a;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
      ".copy,.close,.submit,.channel{height:32px;border:1px solid #b8c4ce;border-radius:3px;background:#fff;color:#263746;font:600 12px Arial,sans-serif;cursor:pointer;transition:background-color .12s,border-color .12s,transform .1s}.copy{padding:0 10px}.close{width:32px;font-size:20px;line-height:1}.copy:hover,.close:hover,.channel:hover{background:var(--accent-soft)}.copy:active,.close:active,.channel:active{transform:translateY(1px)}.channels{grid-column:1/-1;grid-row:2;display:flex;gap:5px;min-width:0;overflow:auto;scrollbar-width:none}.channels::-webkit-scrollbar{display:none}.channel{padding:0 8px;white-space:nowrap}.channel.active{border-color:var(--accent);background:var(--accent);color:#fff}.channel:disabled{border-color:#d4dbe0;background:#eef1f3;color:#9aa6ad;cursor:default}",
      ".body{min-width:0;min-height:0;position:relative;background:#eef2f5}.frame{display:block;width:100%;height:100%;border:0;background:#fff}",
      ".state{position:absolute;inset:0;display:grid;place-items:center;padding:24px;background:#eef2f5;text-align:center}.state-card{width:min(360px,100%);color:#526475;font:13px/1.45 Arial,sans-serif}.state-card b{display:block;margin-bottom:6px;color:#17212b;font-size:15px}.state-card p{margin:0 0 14px}.channel-list{display:grid;gap:8px;margin-top:12px}.channel-list .submit{margin:0;text-align:left;padding:0 12px}",
      ".chat-shell{min-width:0;height:100%;display:grid;grid-template-rows:42px minmax(0,1fr) auto;background:#eef2f5}.chat-tools{display:flex;align-items:center;gap:7px;padding:5px 8px;border-bottom:1px solid #d4dde4;background:#fff}.tool{height:30px;padding:0 9px;border:1px solid #b8c4ce;border-radius:2px;background:#fff;color:#334b5c;font:600 12px Arial,sans-serif;cursor:pointer;transition:background-color .12s,transform .1s}.tool:hover{background:#edf2f6}.tool:active{transform:translateY(1px)}.channel-name{min-width:0;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#526475;font-size:12px}.message-feed{min-height:0;overflow:auto;padding:14px;scrollbar-width:none}.message-feed::-webkit-scrollbar{display:none}.history-note,.empty-chat{margin:8px auto;padding:8px 10px;width:min(440px,100%);border:1px solid #cad4dc;background:#f7f9fa;color:#66788a;text-align:center;font-size:11px;line-height:1.4}.message-row{display:flex;margin:7px 0}.message-row.outgoing{justify-content:flex-end}.bubble{max-width:78%;padding:8px 10px;border:1px solid #d5dde3;border-radius:3px;background:#fff;color:#17212b;font:13px/1.45 Arial,sans-serif;white-space:pre-wrap;overflow-wrap:anywhere}.outgoing .bubble{border-color:var(--accent-soft);background:var(--accent-faint)}.message-meta{display:flex;justify-content:flex-end;gap:6px;margin-top:4px;color:#81909b;font-size:10px}.attachment{display:block;margin-top:6px;color:var(--accent);overflow-wrap:anywhere}.message-image-link{display:block;margin:-4px -6px 7px}.message-image{display:block;max-width:min(360px,100%);max-height:360px;border-radius:2px;object-fit:contain;background:#edf2f5}.composer{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;padding:9px;border-top:1px solid #ccd6de;background:#fff}.composer textarea{min-height:42px;max-height:130px;resize:none;padding:9px;border:1px solid #aab7c2;border-radius:2px;color:#17212b;font:13px/1.4 Arial,sans-serif;outline:none}.composer textarea:focus{border-color:var(--accent)}.send{min-width:104px;border:1px solid var(--accent);border-radius:2px;background:var(--accent);color:#fff;font:700 12px Arial,sans-serif;cursor:pointer;transition:filter .12s,transform .1s}.send:active{transform:translateY(1px)}.send:disabled{opacity:.55;cursor:default}.compose-error{grid-column:1/-1;min-height:0;color:#a23a3a;font-size:11px}",
      ".body{overflow:hidden}.message-feed{overscroll-behavior:contain;touch-action:pan-y}.composer-menu{position:relative}.composer-more{width:34px;height:42px;border:1px solid #aab7c2;border-radius:2px;background:#fff;color:#334b5c;font:700 18px/1 Arial,sans-serif;cursor:pointer}.composer-popover{position:absolute;right:0;bottom:48px;z-index:5;width:250px;max-height:320px;overflow:auto;border:1px solid #b8c4ce;background:#fff;box-shadow:0 10px 30px rgba(22,42,58,.2);scrollbar-width:none}.composer-popover::-webkit-scrollbar{display:none}.composer-menu-button{display:block;width:100%;padding:10px;border:0;border-bottom:1px solid #e3e8ec;background:#fff;color:#263746;text-align:left;font:600 12px Arial,sans-serif;cursor:pointer}.composer-menu-button:hover{background:#edf3f7}.composer-menu-button small{display:block;margin-top:3px;color:#788895;font:11px/1.3 Arial,sans-serif;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.composer-menu-button.menu-back{color:#66788a}.composer-menu-empty{padding:12px;color:#788895;font:12px Arial,sans-serif}.composer-template-row{display:grid;grid-template-columns:minmax(0,1fr) 38px;border-bottom:1px solid #e3e8ec}.composer-template-row .composer-menu-button{min-width:0;border-bottom:0}.template-star{width:38px;border:0;border-left:1px solid #e3e8ec;background:#fff;color:#8b98a1;font-size:20px;line-height:1;cursor:pointer}.template-star:hover{background:#edf3f7}.template-star.active{color:#dfa900}.template-star:disabled{opacity:.5;cursor:default}",
      ".composer textarea{min-height:52px;max-height:min(45vh,360px);resize:none;overflow-y:hidden;scrollbar-width:thin;scrollbar-color:#87959d transparent;transition:border-color .12s,box-shadow .12s}.composer textarea::-webkit-scrollbar{width:8px}.composer textarea::-webkit-scrollbar-track{background:transparent}.composer textarea::-webkit-scrollbar-thumb{background:#87959d;border:2px solid transparent;background-clip:padding-box;border-radius:8px}.composer textarea.template-applied{border-color:var(--accent);box-shadow:0 0 0 2px rgba(51,122,183,.14)}.composer-menu-button,.composer-more,.template-star{transition:background-color .12s,border-color .12s,transform .1s}.composer-menu-button:active,.composer-more:active,.template-star:active{transform:translateY(1px)}",
      ".composer-input{position:relative;min-width:0}.composer-input textarea{width:100%;padding-right:40px}.composer-menu{position:absolute;top:5px;right:5px;z-index:4}.composer-more{width:28px;height:28px;border:0;border-radius:3px;background:transparent;color:#334b5c;font:700 16px/1 Arial,sans-serif;cursor:pointer}.composer-popover{top:32px;bottom:auto}.composer-menu.open-up .composer-popover{top:auto;bottom:32px}.channel-option input{width:14px;height:14px;flex:0 0 14px;margin:0;accent-color:var(--accent);cursor:pointer}",
      ".template-settings{height:100%;overflow:auto;padding:14px;background:#fff;scrollbar-width:none}.template-settings::-webkit-scrollbar{display:none}.template-toolbar{display:flex;gap:7px;margin-bottom:12px}.template-toolbar .tool{flex:0 0 auto}.template-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;padding:10px 0;border-bottom:1px solid #e3e8ec}.template-row b,.template-row small{display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.template-row small{margin-top:3px;color:#788895}.template-row-actions{display:flex;align-items:center;gap:6px}.template-row-actions .template-star{height:30px;border:1px solid #b8c4ce;border-radius:2px}.template-editor{display:grid;gap:8px}.template-editor label{display:grid;gap:4px;color:#526475;font:600 12px Arial,sans-serif}.template-editor input,.template-editor textarea,.template-editor select{width:100%;padding:8px;border:1px solid #aab7c2;background:#fff;color:#17212b;font:13px Arial,sans-serif}.template-editor textarea{min-height:180px;resize:vertical}.variable-list{display:flex;flex-wrap:wrap;gap:6px}.variable-list button{padding:6px 8px;border:1px solid #b8c4ce;background:#f6f8fa;color:#334b5c;cursor:pointer}.variable-list code{font-size:11px}.template-actions{display:flex;gap:7px}.template-actions .tool{flex:1}.template-actions .danger{border-color:#bd7474;color:#983a3a}",
      ".drawer-preferences{height:100%;overflow:auto;padding:18px;background:#fff}.drawer-preferences .field{display:grid;gap:7px;margin:0 0 18px;color:#415565;font:600 12px Arial,sans-serif}.drawer-preferences .choices{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}.drawer-preferences .themes{grid-template-columns:repeat(2,1fr)}.drawer-preferences .palettes{display:flex;gap:8px}.drawer-preferences .palette{width:34px;height:34px;border:2px solid #fff;border-radius:50%;box-shadow:0 0 0 1px #bac5ce}.drawer-preferences .active{border-color:var(--accent);background:var(--accent-soft)}.drawer-preferences .logout{width:100%;margin-top:4px;border-color:#bd7474;color:#983a3a}.drawer-preferences .logout.busy{display:flex;align-items:center;justify-content:center;gap:7px}.drawer-preferences .logout.busy:before{content:'';width:12px;height:12px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .7s linear infinite}",
      ".drawer-preferences .notify-loading{min-height:180px;display:flex;align-items:center;justify-content:center;gap:9px}.drawer-preferences .notify-loading .spinner{width:16px;height:16px;border:2px solid #c9d3dc;border-top-color:var(--accent);border-radius:50%;animation:spin .75s linear infinite}.drawer-preferences .notify-card{margin-bottom:10px;padding:12px;border:1px solid #d7dfe5;background:#fff}.drawer-preferences .notify-card-head{display:flex;justify-content:space-between;gap:10px}.drawer-preferences .notify-state{color:#71828f;font-size:11px}.drawer-preferences .notify-state.ok{color:#2d7d46}.drawer-preferences .notify-help{font-size:11px;line-height:1.4;color:#647684}.drawer-preferences .notify-actions{display:flex;gap:7px;flex-wrap:wrap}.drawer-preferences .notify-action{min-height:34px;padding:0 10px;border:1px solid #bac5ce;border-radius:4px;background:#fff;color:#334b5c;font:600 11px Arial,sans-serif;cursor:pointer}.drawer-preferences .notify-action.primary{border-color:var(--accent);background:var(--accent);color:#fff}.drawer-preferences .notify-action:disabled{opacity:.58;cursor:wait}.drawer-preferences .notify-action.busy{display:inline-flex;align-items:center;gap:6px}.drawer-preferences .notify-action.busy:before{content:'';width:11px;height:11px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .7s linear infinite}.drawer-preferences .notify-pairing{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:9px;padding:9px;border:1px solid #b8c4ce;background:#f4f7f9;overflow-wrap:anywhere}.drawer-preferences .notify-pairing>span:last-of-type{flex:1 1 250px}.drawer-preferences .notify-code{font-family:monospace}.drawer-preferences .notify-routing{margin:14px 0;padding:10px 12px;border:1px solid #d7dfe5;font-size:12px;line-height:1.45}.drawer-preferences .notify-routing summary{cursor:pointer;font-weight:700}.drawer-preferences .notify-fallback{display:flex;gap:8px;align-items:flex-start;margin:12px 0;font-size:12px;line-height:1.4}",
      ".drawer-preferences .operation-list{display:grid;gap:8px}.drawer-preferences .operation-row{display:grid;grid-template-columns:minmax(140px,.8fr) minmax(150px,1fr) minmax(180px,1.5fr);gap:10px;padding:11px;border:1px solid #d7dfe5;color:#334b5c;font:12px/1.4 Arial,sans-serif}.drawer-preferences .operation-row b,.drawer-preferences .operation-row small{display:block}.drawer-preferences .operation-row small{margin-top:4px;color:#71828f}.drawer-preferences .operation-result{white-space:pre-wrap;overflow-wrap:anywhere}.drawer-preferences .operation-error{margin:0 0 6px;color:#a23a3a;font-weight:700}.drawer-preferences .operation-state{display:flex;align-items:center;gap:6px;margin-top:4px}.drawer-preferences .operation-state.success{color:#2d7d46}.drawer-preferences .operation-state.failed,.drawer-preferences .operation-state.dead{color:#a23a3a}.drawer-preferences .operation-state .spinner{width:12px;height:12px;flex:0 0 12px;margin:0}.drawer-preferences .operation-empty{padding:34px 12px;color:#71828f;text-align:center}@media(max-width:700px){.drawer-preferences .operation-row{grid-template-columns:1fr}}",
      ".send-all{display:flex;align-items:center;gap:5px;flex:0 0 auto;color:#415565;font:600 11px Arial,sans-serif;white-space:nowrap}.send-all input{width:14px;height:14px;margin:0;accent-color:var(--accent)}",
      ".layer[data-theme='dark'] .drawer,.layer[data-theme='dark'] .head,.layer[data-theme='dark'] .chat-shell,.layer[data-theme='dark'] .chat-tools,.layer[data-theme='dark'] .composer,.layer[data-theme='dark'] .template-settings,.layer[data-theme='dark'] .drawer-preferences{background:#17212b;color:#edf3f7;border-color:#344553}.layer[data-theme='dark'] .body,.layer[data-theme='dark'] .state,.layer[data-theme='dark'] .message-feed{background:#14202a}.layer[data-theme='dark'] .tool,.layer[data-theme='dark'] .copy,.layer[data-theme='dark'] .close,.layer[data-theme='dark'] .channel,.layer[data-theme='dark'] .composer-menu-button,.layer[data-theme='dark'] .composer-popover,.layer[data-theme='dark'] .template-star,.layer[data-theme='dark'] .notify-card,.layer[data-theme='dark'] .notify-pairing,.layer[data-theme='dark'] .notify-action,.layer[data-theme='dark'] .operation-row,.layer[data-theme='dark'] input,.layer[data-theme='dark'] textarea,.layer[data-theme='dark'] select{background:#1f2d38;color:#edf3f7;border-color:#465968}.layer[data-theme='dark'] .template-star.active{color:#ffd35a}.layer[data-theme='dark'] .bubble,.layer[data-theme='dark'] .history-note,.layer[data-theme='dark'] .empty-chat{background:#22313d;color:#edf3f7;border-color:#465968}.layer[data-theme='dark'] .state-card,.layer[data-theme='dark'] .state-card b,.layer[data-theme='dark'] .state-card p{color:#edf3f7}.layer[data-theme='dark'] .submit{background:var(--accent);color:#fff;border-color:var(--accent)}",
      ".layer[data-theme='gray'] .body,.layer[data-theme='gray'] .state,.layer[data-theme='gray'] .message-feed{background:#92999d;color:#202b31}.layer[data-theme='gray'] .drawer,.layer[data-theme='gray'] .head,.layer[data-theme='gray'] .chat-shell,.layer[data-theme='gray'] .chat-tools,.layer[data-theme='gray'] .composer,.layer[data-theme='gray'] .template-settings,.layer[data-theme='gray'] .drawer-preferences,.layer[data-theme='gray'] .gc-widget{background:#adb3b6;color:#202b31;border-color:#737d82}.layer[data-theme='gray'] .tool,.layer[data-theme='gray'] .copy,.layer[data-theme='gray'] .close,.layer[data-theme='gray'] .channel,.layer[data-theme='gray'] .notify-card,.layer[data-theme='gray'] .notify-pairing,.layer[data-theme='gray'] .notify-action,.layer[data-theme='gray'] .operation-row,.layer[data-theme='gray'] input,.layer[data-theme='gray'] textarea,.layer[data-theme='gray'] select,.layer[data-theme='gray'] .bubble{background:#d2d5d7;color:#202b31;border-color:#737d82}.layer[data-theme='gray'] .channel.active,.layer[data-theme='gray'] .submit,.layer[data-theme='gray'] .gc-tabs .active{background:var(--accent);border-color:var(--accent);color:#fff}",
      ".layer .outgoing .bubble,.layer[data-theme='dark'] .outgoing .bubble{background:var(--outgoing-bg);border-color:var(--outgoing-border)}.template-row{grid-template-columns:22px minmax(0,1fr) auto}.template-row.dragging{opacity:.45}.template-row.drag-over{box-shadow:inset 0 2px var(--accent)}.channel.busy{display:inline-flex;align-items:center;gap:6px}.channel.busy:before,.tool.busy:before{content:'';width:11px;height:11px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .7s linear infinite}.gc-widget{height:100%;overflow:auto;padding:14px;background:#fff}.gc-facts{display:grid;grid-template-columns:repeat(6,minmax(100px,1fr));border:1px solid #d7dfe5}.gc-fact{padding:9px;border-right:1px solid #d7dfe5;min-width:0}.gc-fact:last-child{border-right:0}.gc-fact span{display:block;color:#788895;font:10px Arial,sans-serif;text-transform:uppercase}.gc-fact b{display:block;margin-top:4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font:700 12px Arial,sans-serif}.gc-tabs{display:flex;gap:7px;margin:12px 0}.gc-tabs .active{border-color:var(--accent);background:var(--accent);color:#fff}.gc-pane{font:12px/1.45 Arial,sans-serif}.gc-section{padding:12px;border:1px solid #d7dfe5}.gc-section h3{margin:0 0 10px}.gc-chip-list{display:flex;gap:7px;flex-wrap:wrap}.gc-chip-list:empty{min-height:34px;border:1px solid #d7dfe5}.gc-chip{min-height:34px;padding:6px 9px;border:1px solid #b8c4ce;background:#fff;color:#526475}.gc-chip[type='button']{cursor:pointer}.gc-chip.on{border-color:var(--accent);background:var(--outgoing-bg);color:#17212b}.gc-chip.changed{box-shadow:inset 0 0 0 2px var(--accent)}.gc-notice{padding:11px;border:1px solid #d7dfe5;color:#66788a}.gc-notice.bad{color:#a23a3a}.gc-actions{display:flex;justify-content:flex-end;gap:7px;margin-top:12px}.layer[data-theme='dark'] .gc-widget{background:#17212b;color:#edf3f7}.layer[data-theme='dark'] .gc-facts,.layer[data-theme='dark'] .gc-fact,.layer[data-theme='dark'] .gc-section,.layer[data-theme='dark'] .gc-notice,.layer[data-theme='dark'] .gc-chip-list:empty{border-color:#465968}.layer[data-theme='dark'] .gc-chip{background:#1f2d38;color:#edf3f7;border-color:#465968}.layer[data-theme='dark'] .gc-chip.on{background:var(--outgoing-bg);color:#edf3f7}@media(max-width:980px){.gc-facts{grid-template-columns:repeat(2,1fr)}}",
      ".gc-access-layout{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.gc-access-course,.gc-access-minis{min-width:0;padding:10px;border:1px solid #d7dfe5}.gc-access-course h3,.gc-access-minis h3{margin:0 0 8px}.gc-access-row{display:grid;grid-template-columns:58px minmax(0,1fr);align-items:start;gap:7px;margin-top:6px}.gc-access-row>span{padding-top:7px;color:#788895;font-size:10px;text-transform:uppercase}.gc-access-options{display:flex;gap:4px;flex-wrap:wrap}.gc-access-minis{grid-column:1/-1}.gc-pending,.gc-confirm{margin-top:10px;padding:11px;border:1px solid #d7dfe5}.gc-pending{display:flex;align-items:center;gap:8px;color:#315f3a;background:#eff9f1}.gc-pending .spinner{width:14px;height:14px;flex:0 0 14px;margin:0}.gc-loading{min-height:76px;margin-top:10px;display:flex;align-items:center;justify-content:center;gap:8px;border:1px solid #d7dfe5;color:#66788a}.gc-loading .spinner{width:16px;height:16px;flex:0 0 16px;margin:0}.gc-confirm h3{margin:0 0 8px}.gc-confirm-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.gc-confirm-list{padding:8px;border:1px solid #d7dfe5;white-space:pre-wrap}.gc-confirm-list b{display:block;margin-bottom:5px}.gc-confirm-actions{display:flex;justify-content:flex-end;gap:7px;margin-top:9px}.layer[data-theme='dark'] .gc-access-course,.layer[data-theme='dark'] .gc-access-minis,.layer[data-theme='dark'] .gc-confirm,.layer[data-theme='dark'] .gc-confirm-list,.layer[data-theme='dark'] .gc-loading{border-color:#465968}.layer[data-theme='dark'] .gc-pending{color:#bde6c6;background:#173122;border-color:#315f3a}@media(max-width:980px){.gc-access-layout,.gc-confirm-grid{grid-template-columns:1fr}.gc-access-minis{grid-column:auto}}",
      ".gc-widget .template-toolbar{flex-wrap:wrap}.gc-trial-days{width:100%;height:34px;margin-top:6px;padding:6px;border:1px solid #b8c4ce}.gc-trial-date{display:block;margin-top:5px;color:#66788a}.gc-operation-toast{position:absolute;z-index:8;top:64px;right:12px;display:flex;align-items:center;gap:8px;width:min(420px,calc(100% - 24px));padding:11px 13px;border:1px solid #91b49a;background:#eff9f1;color:#315f3a;box-shadow:0 8px 24px rgba(22,42,58,.2);font:12px/1.4 Arial,sans-serif}.gc-operation-toast .spinner{width:14px;height:14px;flex:0 0 14px;margin:0}.gc-operation-toast.error{border-color:#d49b9b;background:#fff2f2;color:#9a3535}.layer[data-theme='dark'] .gc-operation-toast{color:#bde6c6;background:#173122;border-color:#315f3a}.layer[data-theme='dark'] .gc-operation-toast.error{color:#ffbaba;background:#3b2020;border-color:#884747}",
      ".code{width:100%;height:38px;padding:0 10px;border:1px solid #aab7c2;border-radius:3px;background:#fff;color:#17212b;text-align:center;text-transform:uppercase;font:600 15px/1 Arial,sans-serif;letter-spacing:.08em}.submit{width:100%;height:38px;margin-top:8px;border-color:var(--accent);background:var(--accent);color:#fff}.submit:hover{filter:brightness(.92)}.submit:disabled{opacity:.55;cursor:default}.submit.busy{display:flex;align-items:center;justify-content:center;gap:7px}.submit.busy:before{content:'';width:12px;height:12px;border:2px solid currentColor;border-right-color:transparent;border-radius:50%;animation:spin .7s linear infinite}",
      ".layer[data-theme='gray'] .outgoing .bubble{background:var(--outgoing-bg);border-color:var(--outgoing-border)}",
      ".error{min-height:18px;margin-top:8px;color:#a23a3a;font-size:12px}.spinner{width:24px;height:24px;margin:0 auto 12px;border:2px solid #c9d3dc;border-top-color:var(--accent);border-radius:50%;animation:spin .75s linear infinite}",
      "@keyframes spin{to{transform:rotate(360deg)}}",
      "@media(max-width:900px){.drawer,.layer[data-drawer-size] .drawer{width:100%;border-left:0}.head{grid-template-columns:minmax(0,1fr) auto auto}.title{grid-column:1;grid-row:1}.drawer-settings{grid-column:2;grid-row:1}.close:not(.drawer-settings){grid-column:3;grid-row:1}.drawer-send-all{grid-column:1;grid-row:2}.copy{grid-column:2/4;grid-row:2;max-width:none}.channels{grid-column:1/-1;grid-row:3;height:40px}}",
      "@media(prefers-reduced-motion:reduce){.backdrop,.drawer,.copy,.close,.submit,.channel,.tool,.send{transition:none}.spinner{animation:none}}"
    ].join("");
  }

  var drawer = null;
  var lastFocus = null;
  var activeChannel = null;
  var cardChannels = [];
  var conversationTimer = null;
  var conversationSignature = "";
  var conversationCache = new Map();
  var conversationGeneration = 0;
  var getcourseCard = null;
  var getcourseCardLoading = false;
  var getcourseCardLoadingText = "Ищем пользователя GetCourse…";
  var getcourseCardKey = "";
  var getcourseMiniOpen = false;
  var getcourseTrialPollTimer = null;
  var getcourseAccessPollTimer = null;
  var getcourseNoticeTimer = null;
  var inbox = null;
  var inboxTimer = null;
  var inboxSignature = "";
  var inboxOpenGeneration = 0;

  function conversationKey(payload) {
    return [
      payload.thread_channel_id, payload.thread_chat_type, payload.thread_chat_id,
      payload.entity_type, payload.entity_id, payload.phone,
      payload.channel_id, payload.transport, payload.provider
    ].map(function (value) { return String(value || ""); }).join("|");
  }

  function rememberConversation(key, data) {
    if (!key || !data) return;
    conversationCache.delete(key);
    conversationCache.set(key, data);
    if (conversationCache.size > 12) conversationCache.delete(conversationCache.keys().next().value);
  }

  function stopConversationPoll() {
    if (conversationTimer) clearTimeout(conversationTimer);
    conversationTimer = null;
    conversationGeneration += 1;
    activeChannel = null;
  }

  function ensureDrawer() {
    if (drawer) return drawer;
    var pair = shadowHost(DRAWER_ID);
    pair.root.innerHTML = '<style>' + drawerCss() + '</style><div class="layer" role="dialog" aria-modal="true" aria-label="Сообщения"><div class="backdrop"></div><section class="drawer"><header class="head"><div class="title"><b>Сообщения</b><span class="subtitle">Подготовка…</span></div><label class="send-all drawer-send-all" hidden><input type="checkbox" checked><span>Отправить везде</span></label><div class="channels" hidden></div><button class="copy" type="button">Скопировать номер</button><button class="close drawer-settings" type="button" aria-label="Настройки">⚙</button><button class="close" type="button" aria-label="Закрыть">×</button></header><main class="body"><div class="state"><div class="state-card"><div class="spinner"></div><b>Открываем</b></div></div></main></section></div>';
    document.body.appendChild(pair.host);
    var layer = pair.root.querySelector(".layer");
    pair.root.querySelector(".backdrop").addEventListener("click", closeDrawer);
    pair.root.querySelector(".close:not(.drawer-settings)").addEventListener("click", closeDrawer);
    pair.root.querySelector(".drawer-settings").addEventListener("click", showDrawerSettings);
    pair.root.querySelector(".copy").addEventListener("click", copyPhone);
    drawer = { host: pair.host, root: pair.root, layer: layer, body: pair.root.querySelector(".body"), subtitle: pair.root.querySelector(".subtitle"), channels: pair.root.querySelector(".channels"), copy: pair.root.querySelector(".copy"), sendAll: pair.root.querySelector(".drawer-send-all") };
    wheelScrollX(drawer.channels);
    applyDrawerPrefs(drawer);
    return drawer;
  }

  function setState(title, text, extra) {
    var d = ensureDrawer();
    d.sendAll.hidden = true;
    d.body.innerHTML = '<div class="state"><div class="state-card"><b></b><p></p>' + (extra || "") + '<div class="error"></div></div></div>';
    d.body.querySelector("b").textContent = title;
    d.body.querySelector("p").textContent = text;
    return d.body.querySelector(".state-card");
  }

  function activationForm(message) {
    var card = setState("Активация сотрудника", message || "Введите код сотрудника из панели интеграции.", '<input class="code" autocomplete="one-time-code" inputmode="text" maxlength="16" placeholder="XXXX-XXXX-XXXX"><button class="submit" type="button">Активировать</button><button class="submit guide-button" type="button">Открыть инструкцию</button>');
    var input = card.querySelector(".code");
    var submit = card.querySelector(".submit");
    card.querySelector(".guide-button").addEventListener("click", openGuide);
    submit.addEventListener("click", function () { activate(input.value, submit, card.querySelector(".error")); });
    input.addEventListener("keydown", function (event) { if (event.key === "Enter") submit.click(); });
    setTimeout(function () { input.focus(); }, 30);
  }

  async function request(path, options) {
    var settings = Object.assign({ method: "POST", mode: TEST_MODE ? "same-origin" : "cors", credentials: TEST_MODE ? "same-origin" : "omit" }, options || {});
    var timeoutMs = Math.max(1000, Number(settings.timeoutMs) || REQUEST_TIMEOUT_MS);
    delete settings.timeoutMs;
    settings.headers = Object.assign(
      { "Content-Type": "application/json" },
      TEST_MODE ? { "X-Nexus-Wazzup-Test": "1" } : {},
      (options && options.headers) || {}
    );
    var controller = new AbortController();
    var timer = setTimeout(function () { controller.abort(); }, timeoutMs);
    settings.signal = controller.signal;
    try {
      var response = await fetch(API + path, settings);
      var data = await response.json().catch(function () { return {}; });
      if (!response.ok || data.ok === false) {
        var error = new Error(data.error || "HTTP " + response.status);
        error.reauth = !!data.reauth || response.status === 401;
        throw error;
      }
      return data;
    } catch (error) {
      if (error && error.name === "AbortError") throw new Error("Сервер не ответил за " + Math.round(timeoutMs / 1000) + " секунд. Нажмите «Повторить».");
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  function resizeComposerTextarea(input) {
    if (!input || input.tagName !== "TEXTAREA") return;
    var computed = window.getComputedStyle(input);
    var minimum = parseFloat(computed.minHeight) || 40;
    var maximum = parseFloat(computed.maxHeight);
    if (!Number.isFinite(maximum) || maximum <= 0) maximum = Math.min(window.innerHeight * 0.45, 360);
    input.style.height = "auto";
    var desired = Math.max(minimum, Math.min(input.scrollHeight, maximum));
    input.style.height = Math.ceil(desired) + "px";
    input.style.overflowY = input.scrollHeight > maximum + 1 ? "auto" : "hidden";
  }

  function bindComposerTextarea(input) {
    if (!input || input.dataset.composerBound === "1") return;
    input.dataset.composerBound = "1";
    input.addEventListener("keydown", function (event) {
      event.stopPropagation();
      if ((event.key === " " || event.key === "Spacebar" || event.code === "Space") && !event.ctrlKey && !event.metaKey && !event.altKey && !input.disabled && !input.readOnly) {
        event.preventDefault();
        var start = typeof input.selectionStart === "number" ? input.selectionStart : input.value.length;
        var end = typeof input.selectionEnd === "number" ? input.selectionEnd : start;
        input.setRangeText(" ", start, end, "end");
        input.dispatchEvent(new Event("input", { bubbles: false }));
      }
    });
    ["keypress", "keyup", "beforeinput"].forEach(function (type) {
      input.addEventListener(type, function (event) { event.stopPropagation(); });
    });
    input.addEventListener("input", function (event) {
      event.stopPropagation();
      resizeComposerTextarea(input);
    });
    setTimeout(function () { resizeComposerTextarea(input); }, 0);
  }

  function attachTemplates(composer, input, payloadFactory) {
    bindComposerTextarea(input);
    composer.style.gridTemplateColumns = "minmax(0,1fr) auto";
    var inputWrap = document.createElement("div");
    inputWrap.className = "composer-input";
    composer.insertBefore(inputWrap, input);
    inputWrap.appendChild(input);
    var menu = document.createElement("div");
    menu.className = "composer-menu";
    var more = document.createElement("button");
    more.type = "button";
    more.className = "composer-more";
    more.setAttribute("aria-label", "Действия");
    more.setAttribute("aria-expanded", "false");
    more.textContent = "⌃";
    var popover = document.createElement("div");
    popover.className = "composer-popover";
    popover.hidden = true;
    var file = document.createElement("input");
    file.type = "file";
    file.hidden = true;
    menu.appendChild(more);
    menu.appendChild(popover);
    menu.appendChild(file);
    inputWrap.appendChild(menu);
    var token = localStorage.getItem(STORAGE_KEY) || "";
    var base = function () { return Object.assign({}, payloadFactory ? payloadFactory() : context()); };
    var templates = [];
    var variables = [];
    var templateDragId = 0;

    function payload(next) { return Object.assign(base(), next || {}); }
    function clearMenu() { popover.innerHTML = ""; }
    function menuButton(label, action) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "composer-menu-button";
      button.textContent = label;
      button.addEventListener("click", function (event) {
        event.preventDefault();
        event.stopPropagation();
        action();
        if (!popover.hidden) placeMenu();
      });
      popover.appendChild(button);
      return button;
    }
    function closeMenu() {
      popover.hidden = true;
      more.setAttribute("aria-expanded", "false");
    }
    function placeMenu() {
      var rect = more.getBoundingClientRect();
      menu.classList.toggle("open-up", rect.top > window.innerHeight - rect.bottom);
      popover.style.transform = "";
      var box = popover.getBoundingClientRect();
      var shift = box.left < 8 ? 8 - box.left : (box.right > window.innerWidth - 8 ? window.innerWidth - 8 - box.right : 0);
      if (shift) popover.style.transform = "translateX(" + shift + "px)";
    }
    function showRoot() {
      clearMenu();
      menuButton("Шаблон", showScopes);
      menuButton("Вложение", function () {
        var url = window.prompt("HTTPS-ссылка на файл:", input.dataset.attachmentUrl || "");
        if (url === null) return;
        url = url.trim();
        if (url && !/^https:\/\//i.test(url)) { window.alert("Нужна HTTPS-ссылка"); return; }
        input.dataset.attachmentUrl = url;
        input.dataset.attachmentType = url ? (window.prompt("Тип: image, video, audio или document:", "document") || "document").trim() : "";
        more.title = url ? "Вложение добавлено" : "";
        closeMenu();
      });
    }
    function showScopes() {
      clearMenu();
      menuButton("← Назад", showRoot).classList.add("menu-back");
      menuButton("★ Избранное", showFavorites);
      menuButton("Общие", function () { showFolders("shared"); });
      menuButton("Личные", function () { showFolders("personal"); });
    }
    function favoriteRows() {
      return templates.filter(function (template) { return template.favorite; }).sort(function (left, right) {
        return Number(left.favorite_order || 0) - Number(right.favorite_order || 0);
      });
    }
    function showFavorites() {
      clearMenu();
      menuButton("← Шаблоны", showScopes).classList.add("menu-back");
      showTemplateRows(favoriteRows(), true);
    }
    function showFolders(scope) {
      clearMenu();
      menuButton("← Шаблоны", showScopes).classList.add("menu-back");
      if (scope === "personal") {
        showTemplateRows(templates.filter(function (template) { return template.scope === "personal"; }), false);
        return;
      }
      var folders = [];
      templates.filter(function (template) { return template.scope === scope; }).forEach(function (template) {
        var folder = String(template.folder || "Без папки");
        if (folders.indexOf(folder) === -1) folders.push(folder);
      });
      if (!folders.length) {
        var empty = document.createElement("div");
        empty.className = "composer-menu-empty";
        empty.textContent = "Шаблонов нет";
        popover.appendChild(empty);
        return;
      }
      folders.forEach(function (folder) { menuButton(folder, function () { showTemplates(scope, folder); }); });
    }
    function showTemplates(scope, folder) {
      clearMenu();
      menuButton("← Папки", function () { showFolders(scope); }).classList.add("menu-back");
      showTemplateRows(templates.filter(function (template) {
        return template.scope === scope && String(template.folder || "Без папки") === folder;
      }), false);
    }
    async function moveTemplate(sourceId, targetId, before, refresh) {
      var sourceIndex = templates.findIndex(function (row) { return Number(row.id) === Number(sourceId); });
      var targetIndex = templates.findIndex(function (row) { return Number(row.id) === Number(targetId); });
      if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) return;
      var source = templates.splice(sourceIndex, 1)[0];
      var adjusted = templates.findIndex(function (row) { return Number(row.id) === Number(targetId); });
      templates.splice(adjusted + (before ? 0 : 1), 0, source);
      clearMenu();
      var loading = document.createElement("div");
      loading.className = "composer-menu-loading";
      loading.innerHTML = '<span class="spinner" aria-hidden="true"></span><span>Сохраняем ваш порядок шаблонов…</span>';
      popover.appendChild(loading);
      try {
        await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "reorder", template_ids: templates.map(function (row) { return Number(row.id); }) })) });
        refresh();
      } catch (error) {
        window.alert(error.message || "Не удалось сохранить порядок");
        loadTemplates().then(refresh);
      }
    }
    function showTemplateRows(rows, favoritesOnly) {
      if (!rows.length) {
        var empty = document.createElement("div");
        empty.className = "composer-menu-empty";
        empty.textContent = favoritesOnly ? "Избранных шаблонов нет" : "Шаблонов нет";
        popover.appendChild(empty);
        return;
      }
      rows.forEach(function (template) {
        var row = document.createElement("div");
        row.className = "composer-template-row";
        if (!favoritesOnly) {
          var drag = document.createElement("span");
          drag.className = "template-drag";
          drag.textContent = "⋮⋮";
          drag.title = "Перетащите шаблон";
          row.draggable = true;
          row.dataset.templateId = template.id;
          row.addEventListener("dragstart", function (event) { templateDragId = Number(template.id); row.classList.add("dragging"); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", String(template.id)); });
          row.addEventListener("dragend", function () { templateDragId = 0; row.classList.remove("dragging"); popover.querySelectorAll(".drag-over").forEach(function (node) { node.classList.remove("drag-over"); }); });
          row.addEventListener("dragover", function (event) { event.preventDefault(); row.classList.add("drag-over"); });
          row.addEventListener("dragleave", function () { row.classList.remove("drag-over"); });
          row.addEventListener("drop", function (event) { event.preventDefault(); row.classList.remove("drag-over"); var before = event.clientY < row.getBoundingClientRect().top + row.offsetHeight / 2; moveTemplate(templateDragId, template.id, before, function () { if (template.scope === "personal") showFolders("personal"); else showTemplates(template.scope, String(template.folder || "Без папки")); }); });
          row.appendChild(drag);
        }
        var button = document.createElement("button");
        button.type = "button";
        button.className = "composer-menu-button";
        button.textContent = template.title;
        var preview = document.createElement("small");
        preview.textContent = String(template.body || "").replace(/\s+/g, " ").slice(0, 90);
        button.appendChild(preview);
        button.addEventListener("click", function (event) {
          event.preventDefault();
          event.stopPropagation();
          applyTemplate(template);
        });
        var star = document.createElement("button");
        star.type = "button";
        star.className = "template-star" + (template.favorite ? " active" : "");
        star.textContent = template.favorite ? "★" : "☆";
        star.setAttribute("aria-label", template.favorite ? "Убрать из избранного" : "Добавить в избранное");
        star.title = star.getAttribute("aria-label");
        star.addEventListener("click", async function (event) {
          event.preventDefault();
          event.stopPropagation();
          star.disabled = true;
          try {
            var next = !template.favorite;
            await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "favorite", id: Number(template.id), favorite: next })) });
            template.favorite = next;
            if (next) template.favorite_order = templates.reduce(function (maximum, item) { return Math.max(maximum, Number(item.favorite_order || 0)); }, -1) + 1;
            if (favoritesOnly) showFavorites();
            else {
              star.classList.toggle("active", next);
              star.textContent = next ? "★" : "☆";
              star.setAttribute("aria-label", next ? "Убрать из избранного" : "Добавить в избранное");
              star.title = star.getAttribute("aria-label");
            }
          } catch (error) { window.alert(error.message || "Не удалось изменить избранное"); }
          finally { star.disabled = false; }
        });
        row.appendChild(button);
        row.appendChild(star);
        popover.appendChild(row);
      });
    }
    async function applyTemplate(template) {
      var optimistic = optimisticTemplate(template.body, base());
      input.value = optimistic;
      resizeComposerTextarea(input);
      input.focus();
      flashTemplateInput(input);
      closeMenu();
      try {
        var data = await request("/template-preview", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ id: Number(template.id) })) });
        if (input.value === optimistic) {
          input.value = data.text || "";
          resizeComposerTextarea(input);
        }
      } catch (error) { window.alert(error.message || "Не удалось применить шаблон"); }
    }
    more.addEventListener("click", function () {
      popover.hidden = !popover.hidden;
      more.setAttribute("aria-expanded", String(!popover.hidden));
      if (!popover.hidden) { showRoot(); placeMenu(); }
    });
    file.addEventListener("change", function () {
      if (!file.files || !file.files[0]) return;
      more.title = "Файл: " + file.files[0].name;
      closeMenu();
    });
    request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "list" })) }).then(function (data) {
      templates = Array.isArray(data.templates) ? data.templates : [];
    }).catch(function () {});
  }

  function channelLabel(channel) {
    return String(channel.label || channel.name || channel.transport || "Канал").split(" · ")[0];
  }

  async function sendComposerText(rawText, channels, payloadFor, token, attachment) {
    var targets = channels.filter(function (channel) { return channel && channel.can_send !== false; });
    if (!targets.length) throw new Error("Нет доступных каналов");
    var preview = rawText ? await request("/template-preview", {
      headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token },
      body: JSON.stringify(Object.assign({}, payloadFor(targets[0]), { body: rawText }))
    }) : { text: "" };
    var sent = [];
    var queued = [];
    var failed = [];
    var batchId = window.crypto && crypto.randomUUID ? crypto.randomUUID() : Date.now() + "-" + Math.random().toString(36).slice(2);
    await Promise.all(targets.map(async function (channel, index) {
      if (attachment && attachment.attachment_url && channel.provider !== "salebot") {
        failed.push(channelLabel(channel) + ": вложения поддерживает SaleBot");
        return;
      }
      try {
        var result = await request("/send", {
          headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token },
          body: JSON.stringify(Object.assign({}, payloadFor(channel), attachment || {}, { text: preview.text, request_id: batchId + ":" + index }))
        });
        if (result.queued) queued.push(channelLabel(channel));
        else if (result.sent === false) failed.push(channelLabel(channel) + (result.notice ? ": " + result.notice : ""));
        else sent.push(channelLabel(channel));
      } catch (error) {
        failed.push(channelLabel(channel) + ": " + (error.message || "ошибка"));
      }
    }));
    return {
      sent: sent,
      queued: queued,
      failed: failed,
      status: [sent.length ? "Отправлено: " + sent.join(", ") : "", queued.length ? "В очереди: " + queued.join(", ") : "", failed.length ? "Не отправлено: " + failed.join("; ") : ""].filter(Boolean).join(" · ")
    };
  }

  async function activate(code, button, errorNode) {
    button.disabled = true;
    button.classList.add("busy");
    button.textContent = "Входим…";
    errorNode.textContent = "";
    try {
      var data = await request("/activate", { body: JSON.stringify({ code: String(code || "").trim() }) });
      localStorage.setItem(STORAGE_KEY, data.device_token);
      ensureInbox();
      loadInbox(true);
      scheduleInboxPoll();
      await showChannelMenu();
    } catch (error) {
      errorNode.textContent = error.message || "Не удалось активировать устройство";
    } finally {
      button.classList.remove("busy");
      button.textContent = "Активировать";
      button.disabled = false;
    }
  }

  async function logoutDevice(button) {
    if (!window.confirm("Выйти из виджета на этом компьютере?")) return;
    button.disabled = true;
    button.classList.add("busy");
    button.textContent = "Выходим…";
    var token = localStorage.getItem(STORAGE_KEY) || "";
    try {
      if (token) await request("/logout", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(context()) });
    } catch (error) {}
    localStorage.removeItem(STORAGE_KEY);
    stopConversationPoll();
    if (inboxTimer) clearTimeout(inboxTimer);
    inboxTimer = null;
    conversationCache.clear();
    if (inbox) inbox.wrap.classList.remove("open");
    activationForm("Введите личный код сотрудника, чтобы снова войти.");
  }

  function inboxInitials(name) {
    return String(name || "К").trim().split(/\s+/).slice(0, 2).map(function (part) { return part.charAt(0); }).join("").toUpperCase().slice(0, 2) || "К";
  }

  function ensureInbox() {
    if (inbox) return inbox;
    var pair = shadowHost(INBOX_ID);
    pair.root.innerHTML = '<style>' + inboxCss() + '</style><div class="wrap"><button class="launcher" type="button" aria-label="Сообщения"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 15a4 4 0 0 1-4 4H9l-5 3 1.6-4.8A7 7 0 0 1 3 12V9a7 7 0 0 1 7-7h6a5 5 0 0 1 5 5z"/></svg><span class="badge" hidden></span></button><section class="panel" aria-label="Сообщения"><header class="panel-head"><button class="icon back" type="button" aria-label="Назад" hidden>←</button><b>Диалоги</b><button class="icon refresh" type="button" aria-label="Обновить">↻</button><button class="icon settings-button" type="button" aria-label="Настройки">⚙</button><button class="icon panel-close" type="button" aria-label="Закрыть">×</button></header><div class="list"><div class="empty inbox-loading"><span class="spinner" aria-hidden="true"></span><span>Загружаем диалоги…</span></div></div></section></div>';
    document.body.appendChild(pair.host);
    var wrap = pair.root.querySelector(".wrap");
    var launcher = pair.root.querySelector(".launcher");
    var list = pair.root.querySelector(".list");
    launcher.addEventListener("click", function () {
      wrap.classList.toggle("open");
      if (wrap.classList.contains("open")) loadInbox(false);
    });
    pair.root.querySelector(".panel-close").addEventListener("click", function () { wrap.classList.remove("open"); });
    pair.root.querySelector(".refresh").addEventListener("click", function () { loadInbox(false); });
    pair.root.querySelector(".back").addEventListener("click", function () {
      if (inbox && (inbox.view === "notification-settings" || inbox.view === "operations")) showInboxSettings();
      else showInboxList();
    });
    pair.root.querySelector(".settings-button").addEventListener("click", showInboxSettings);
    inbox = {
      host: pair.host,
      root: pair.root,
      wrap: wrap,
      body: list,
      list: list,
      badge: pair.root.querySelector(".badge"),
      launcher: launcher,
      title: pair.root.querySelector(".panel-head b"),
      back: pair.root.querySelector(".back"),
      settingsButton: pair.root.querySelector(".settings-button"),
      refresh: pair.root.querySelector(".refresh"),
      view: "list",
      items: [],
      channels: []
    };
    applyPrefs(inbox);
    return inbox;
  }

  function updateInboxBadge(count, unanswered) {
    var view = ensureInbox();
    var value = Math.max(0, Number(count) || 0);
    view.unread = value;
    view.badge.textContent = value > 99 ? "99+" : String(value);
    view.badge.hidden = !value;
    view.launcher.classList.toggle("attention", Number(unanswered) > 0);
  }

  function setInboxHeader(title, back, settings, refresh) {
    var view = ensureInbox();
    view.title.textContent = title;
    view.back.hidden = !back;
    view.settingsButton.hidden = !settings;
    view.refresh.hidden = !refresh;
  }

  function showInboxList() {
    var view = ensureInbox();
    view.view = "list";
    view.activeItem = null;
    view.activeChannel = null;
    conversationSignature = "";
    setInboxHeader("Диалоги", false, true, true);
    renderInbox({ items: view.items, unread: view.unread || 0, unanswered: view.unanswered || 0 }, true);
  }

  function writePrefs(next) {
    var prefs = Object.assign(readPrefs(), next || {});
    localStorage.setItem(PREFS_KEY, JSON.stringify(prefs));
    applyPrefs(inbox);
    applyDrawerPrefs(drawer);
    applyButtonPrefs();
  }

  function notificationApi(path, options) {
    var token = localStorage.getItem(STORAGE_KEY) || "";
    return request("/notifications" + path, Object.assign({
      headers: { "Authorization": "Bearer " + token }, body: "{}"
    }, options || {}));
  }

  function operationsApi() {
    var token = localStorage.getItem(STORAGE_KEY) || "";
    return request("/operations", {
      headers: { "Authorization": "Bearer " + token },
      body: JSON.stringify(context())
    });
  }

  function renderOperations(container, data, restore) {
    container.innerHTML = "";
    if (!container.classList.contains("settings")) {
      var toolbar = document.createElement("div");
      toolbar.className = "notify-actions";
      toolbar.style.marginBottom = "12px";
      toolbar.appendChild(notificationButton("Назад", "", restore));
      container.appendChild(toolbar);
    }
    var items = Array.isArray(data.items) ? data.items : [];
    if (!items.length) {
      var empty = document.createElement("div");
      empty.className = "operation-empty";
      empty.textContent = "Операций пока нет";
      container.appendChild(empty);
      return;
    }
    var list = document.createElement("div");
    list.className = "operation-list";
    items.forEach(function (item) {
      var row = document.createElement("article"), main = document.createElement("div"),
        title = document.createElement("b"), state = document.createElement("span"),
        actor = document.createElement("small"), client = document.createElement("div"),
        clientTitle = document.createElement("b"), stamp = document.createElement("small"),
        result = document.createElement("div"), resultText = document.createElement("span");
      row.className = "operation-row"; title.textContent = item.title || "Операция";
      state.className = "operation-state " + (item.status || "");
      if (item.status === "pending") { var spinner = document.createElement("span"); spinner.className = "spinner"; state.append(spinner, document.createTextNode("Выполняется")); }
      else state.textContent = item.status === "success" || item.status === "sent" || item.status === "delivered" ? "Готово" : item.status === "failed" || item.status === "dead" ? "Не доставлено" : item.status || "—";
      actor.textContent = "Кто: " + (item.admin_name || "—"); main.append(title, state, actor);
      clientTitle.textContent = item.client_name || "—"; stamp.textContent = gcFormatDate(item.created_at); client.append(clientTitle, stamp);
      result.className = "operation-result";
      if (item.error) { var error = document.createElement("div"); error.className = "operation-error"; error.textContent = item.error; result.appendChild(error); }
      resultText.textContent = item.result || "—"; result.appendChild(resultText);
      if (item.expires_at) { var closes = document.createElement("small"); closes.textContent = "Закроется: " + gcFormatDate(item.expires_at); result.appendChild(closes); }
      if (item.note_status === "pending") { var note = document.createElement("small"), noteSpinner = document.createElement("span"); note.className = "operation-state"; noteSpinner.className = "spinner"; note.append(noteSpinner, document.createTextNode("Записываем результат в сделку…")); result.appendChild(note); }
      row.append(main, client, result); list.appendChild(row);
    });
    container.appendChild(list);
    if (items.some(function (item) { return item.status === "pending" || item.note_status === "pending"; })) {
      setTimeout(function () { if (container.isConnected && container.querySelector(".operation-list")) loadOperations(container, restore); }, 5000);
    }
  }

  async function loadOperations(container, restore) {
    notificationLoading(container, "Загружаем журнал операций…");
    try { renderOperations(container, await operationsApi(), restore); }
    catch (error) {
      container.innerHTML = "";
      var message = document.createElement("p"), retry = notificationButton("Повторить загрузку", "", function () { loadOperations(container, restore); });
      message.className = "notify-help"; message.textContent = error.message || "Не удалось загрузить операции";
      container.append(message, retry);
    }
  }

  async function notificationBusy(button, label, action) {
    var original = button.textContent;
    button.disabled = true;
    button.classList.add("busy");
    button.textContent = label;
    try { return await action(); }
    finally { button.disabled = false; button.classList.remove("busy"); button.textContent = original; }
  }

  function notificationLoading(container, text) {
    container.innerHTML = '<div class="notify-loading"><span class="spinner" aria-hidden="true"></span><span></span></div>';
    container.querySelector(".notify-loading span:last-child").textContent = text;
  }

  function notificationButton(label, className, action) {
    var button = document.createElement("button");
    button.type = "button";
    button.className = "notify-action" + (className ? " " + className : "");
    button.textContent = label;
    button.addEventListener("click", action);
    return button;
  }

  function renderNotificationSettings(container, data, restore) {
    container.innerHTML = "";
    var toolbar = document.createElement("div");
    toolbar.className = "notify-actions";
    toolbar.style.marginBottom = "12px";
    toolbar.appendChild(notificationButton("Назад", "", restore));
    if (!container.classList.contains("settings")) container.appendChild(toolbar);
    [["telegram", "Telegram", "Личные уведомления через @attackpng_notify_bot."], ["vk", "VK", "Личные сообщения от подключённого сообщества VK."], ["browser", "Браузер", "Системные уведомления на этом компьютере. Клик открывает сделку amoCRM."]].forEach(function (item) {
      var provider = item[0];
      var destination = (data.destinations || {})[provider] || {};
      var card = document.createElement("section");
      card.className = "notify-card";
      var head = document.createElement("div");
      head.className = "notify-card-head";
      var title = document.createElement("b");
      title.textContent = item[1];
      var state = document.createElement("span");
      state.className = "notify-state" + (destination.connected && destination.enabled ? " ok" : "");
      state.textContent = destination.connected && destination.enabled ? "Подключено · " + (destination.label || "готово") : destination.connected ? "Нужно переподключить" : "Не подключено";
      head.append(title, state);
      var help = document.createElement("p");
      help.className = "notify-help";
      help.textContent = destination.last_error || item[2];
      var actions = document.createElement("div");
      actions.className = "notify-actions";
      function openBrowserPage(button) {
        notificationBusy(button, "Готовим страницу…", async function () {
          var result = await notificationApi("/browser/open", { body: "{}" });
          window.open(result.url, "_blank", "noopener");
          var spinner = document.createElement("span"), label = document.createElement("span");
          spinner.className = "spinner"; label.textContent = "Ожидаем разрешение браузера…";
          state.className = "notify-state waiting"; state.replaceChildren(spinner, label);
          for (var attempt = 0; attempt < 60 && state.isConnected; attempt += 1) {
            await new Promise(function (resolve) { setTimeout(resolve, 2000); });
            var next = await notificationApi("/settings");
            if (next.destinations && next.destinations.browser && next.destinations.browser.enabled) {
              renderNotificationSettings(container, next, restore);
              return;
            }
          }
          if (state.isConnected) { state.className = "notify-state"; state.textContent = "Разрешение пока не получено — нажмите «Подключить» ещё раз."; }
        }).catch(function (error) { state.textContent = error.message; state.className = "notify-state"; });
      }
      if (destination.connected && destination.enabled) {
        var test = provider === "browser" ? notificationButton("Открыть страницу", "", function () { openBrowserPage(test); }) : notificationButton("Проверить", "", function () {
          notificationBusy(test, "Отправляю…", async function () {
            var result = await notificationApi("/test", { body: JSON.stringify({ provider: provider }) });
            state.textContent = result.message || "Тест отправлен";
          }).catch(function (error) { state.textContent = error.message; state.className = "notify-state"; });
        });
        var disconnect = notificationButton("Отключить", "", function () {
          notificationBusy(disconnect, "Отключаю…", async function () {
            var next = await notificationApi("/disconnect", { body: JSON.stringify({ provider: provider }) });
            renderNotificationSettings(container, Object.assign({}, data, next), restore);
          }).catch(function (error) { state.textContent = error.message; state.className = "notify-state"; });
        });
        actions.append(test, disconnect);
      } else {
        var connect = notificationButton("Подключить", "primary", function () {
          if (provider === "browser") { openBrowserPage(connect); return; }
          notificationBusy(connect, "Готовлю подключение…", async function () {
            var pairing = await notificationApi("/pair", { body: JSON.stringify({ provider: provider }) });
            var pairBox = document.createElement("div");
            pairBox.className = "notify-pairing";
            var spinner = document.createElement("span");
            spinner.className = "spinner";
            var pairText = document.createElement("span");
            pairText.textContent = provider === "telegram"
              ? "1. В открывшемся боте нажмите Start. 2. Дождитесь сообщения «Уведомления Nexus подключены». Сейчас ожидаем подтверждение…"
              : "1. Нажмите код ниже — он скопируется. 2. В открывшемся диалоге VK вставьте код и отправьте его сообществу. Сейчас ожидаем подтверждение…";
            pairBox.append(spinner, pairText);
            if (provider === "vk") {
              var code = notificationButton(pairing.command, "notify-code", function () {
                var copy = navigator.clipboard ? navigator.clipboard.writeText(pairing.command) : Promise.reject();
                copy.then(function () { code.textContent = pairing.command + " · скопирован"; }).catch(function () { window.prompt("Скопируйте код", pairing.command); });
              });
              pairBox.appendChild(code);
              if (navigator.clipboard) navigator.clipboard.writeText(pairing.command).then(function () { code.textContent = pairing.command + " · скопирован"; }).catch(function () {});
            }
            card.appendChild(pairBox);
            window.open(pairing.url, "_blank", "noopener");
            var checks = 0;
            async function check() {
              if (!container.isConnected || checks >= 60) {
                pairBox.classList.remove("notify-loading");
                pairText.textContent = "Подтверждение пока не получено. Можно нажать «Подключить» ещё раз.";
                return;
              }
              checks += 1;
              try {
                var next = await notificationApi("/settings");
                var ready = next.destinations && next.destinations[provider] && next.destinations[provider].enabled;
                if (ready) { renderNotificationSettings(container, next, restore); return; }
              } catch (error) {
                pairBox.classList.remove("notify-loading");
                pairText.textContent = error.message || "Не удалось проверить подключение";
                return;
              }
              setTimeout(check, 2000);
            }
            setTimeout(check, 1500);
          }).catch(function (error) { state.textContent = error.message; state.className = "notify-state"; });
        });
        actions.appendChild(connect);
      }
      card.append(head, help, actions);
      container.appendChild(card);
    });
    var routing = document.createElement("details");
    routing.className = "notify-routing";
    routing.innerHTML = "<summary>Как Nexus выбирает нужного менеджера</summary><p>В amoCRM у сделки есть ответственный. В разделе Nexus «Сотрудники» его amoCRM ID привязан к сотруднику Nexus. Когда этот сотрудник открывает виджет, его устройство также принадлежит ему. Поэтому входящее сообщение получает тот менеджер, который назначен ответственным в сделке.</p><p>Если ответственный сменился, Nexus использует нового после следующего открытия карточки или обновления связи. Сообщения без найденного ответственного никому не рассылаются, кроме администраторов с резервной настройкой ниже.</p>";
    container.appendChild(routing);
    if (data.admin_role === "admin") {
      var fallback = document.createElement("label");
      fallback.className = "notify-fallback";
      var checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = !!data.fallback_unassigned;
      var fallbackText = document.createElement("span");
      fallbackText.textContent = "Получать резервные уведомления, если у клиента не найден ответственный менеджер amoCRM";
      fallback.append(checkbox, fallbackText);
      checkbox.addEventListener("change", async function () {
        checkbox.disabled = true;
        fallbackText.textContent = "Сохраняю настройку…";
        try {
          var next = await notificationApi("/settings", { method: "PUT", body: JSON.stringify({ fallback_unassigned: checkbox.checked }) });
          renderNotificationSettings(container, Object.assign({}, data, next, { admin_role: data.admin_role }), restore);
        } catch (error) {
          checkbox.checked = !checkbox.checked;
          fallbackText.textContent = error.message || "Не удалось сохранить";
        } finally { checkbox.disabled = false; }
      });
      container.appendChild(fallback);
    }
  }

  async function loadNotificationSettings(container, restore) {
    notificationLoading(container, "Загружаем подключения уведомлений…");
    try {
      var data = await notificationApi("/settings");
      renderNotificationSettings(container, data, restore);
    } catch (error) {
      container.innerHTML = "";
      var retry = notificationButton("Повторить загрузку", "", function () { loadNotificationSettings(container, restore); });
      var message = document.createElement("p");
      message.className = "notify-help";
      message.textContent = error.message || "Не удалось загрузить уведомления";
      container.append(message, retry);
    }
  }

  function showInboxNotificationSettings() {
    var view = ensureInbox();
    view.view = "notification-settings";
    setInboxHeader("Уведомления", true, false, false);
    view.body.className = "settings";
    loadNotificationSettings(view.body, showInboxSettings);
  }

  function showInboxOperations() {
    var view = ensureInbox(); view.view = "operations"; setInboxHeader("Операции", true, false, false);
    view.body.className = "settings"; loadOperations(view.body, showInboxSettings);
  }

  function showDrawerNotificationSettings() {
    stopConversationPoll();
    var d = ensureDrawer();
    d.root.querySelector(".title b").textContent = "Уведомления";
    d.subtitle.textContent = "Новые сообщения клиентов";
    d.channels.hidden = true;
    d.copy.hidden = true;
    d.sendAll.hidden = true;
    d.body.innerHTML = '<div class="drawer-preferences notification-settings-view"></div>';
    d.layer.classList.add("open");
    loadNotificationSettings(d.body.querySelector(".notification-settings-view"), showDrawerSettings);
  }

  function showDrawerOperations() {
    stopConversationPoll(); var d = ensureDrawer(); d.root.querySelector(".title b").textContent = "Операции";
    d.subtitle.textContent = "Сообщения, доступы и тестовые периоды"; d.channels.hidden = true; d.copy.hidden = true; d.sendAll.hidden = true;
    d.body.innerHTML = '<div class="drawer-preferences operations-view"></div>'; d.layer.classList.add("open");
    loadOperations(d.body.querySelector(".operations-view"), showDrawerSettings);
  }

  function showInboxSettings() {
    var view = ensureInbox();
    view.view = "settings";
    setInboxHeader("Настройки", true, false, false);
    var prefs = readPrefs();
    view.body.className = "settings";
    view.body.innerHTML = '<button class="reset notifications-settings" type="button">Уведомления</button><button class="reset operations-settings" type="button">Операции</button><button class="reset templates-settings" type="button">Шаблоны</button><button class="reset guide-settings" type="button">Инструкция</button><div class="field">Тема<div class="themes"></div></div><div class="field">Палитра<div class="palettes"></div></div><label class="field">Цвет<input class="color" type="color" value="' + prefs.color + '"></label><div class="field">Положение<div class="positions"></div></div><div class="field">Диалоги<div class="sizes inbox-sizes"></div></div><div class="field">Карточка<div class="sizes drawer-sizes"></div></div><button class="reset prefs-reset" type="button">Сбросить</button><button class="reset logout" type="button">Выйти из аккаунта</button>';
    wheelScrollY(view.body);
    view.body.querySelector(".templates-settings").addEventListener("click", showDrawerTemplateSettings);
    view.body.querySelector(".notifications-settings").addEventListener("click", showInboxNotificationSettings);
    view.body.querySelector(".operations-settings").addEventListener("click", showInboxOperations);
    view.body.querySelector(".guide-settings").addEventListener("click", openGuide);
    view.body.querySelector(".color").addEventListener("input", function (event) { writePrefs({ color: event.target.value }); });
    [["light", "Светлая"], ["gray", "Серая"], ["dark", "Тёмная"]].forEach(function (item) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "theme" + (prefs.theme === item[0] ? " active" : "");
      button.textContent = item[1];
      button.addEventListener("click", function () { writePrefs({ theme: item[0] }); showInboxSettings(); });
      view.body.querySelector(".themes").appendChild(button);
    });
    PALETTE_COLORS.forEach(function (color) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "palette" + (prefs.color.toLowerCase() === color ? " active" : "");
      button.style.background = color;
      button.setAttribute("aria-label", color);
      button.addEventListener("click", function () { writePrefs({ color: color }); showInboxSettings(); });
      view.body.querySelector(".palettes").appendChild(button);
    });
    var labels = { "top-left": "Сверху слева", "top-right": "Сверху справа", "bottom-left": "Снизу слева", "bottom-right": "Снизу справа" };
    Object.keys(labels).forEach(function (position) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "position" + (prefs.position === position ? " active" : "");
      button.textContent = labels[position];
      button.addEventListener("click", function () { writePrefs({ position: position }); showInboxSettings(); });
      view.body.querySelector(".positions").appendChild(button);
    });
    var sizeLabels = { small: "Маленький", medium: "Средний", large: "Большой" };
    function addSizes(selector, key) {
      Object.keys(sizeLabels).forEach(function (size) {
        var button = document.createElement("button");
        button.type = "button";
        button.className = "size" + (prefs[key] === size ? " active" : "");
        button.textContent = sizeLabels[size];
        button.addEventListener("click", function () { var next = {}; next[key] = size; writePrefs(next); showInboxSettings(); });
        view.body.querySelector(selector).appendChild(button);
      });
    }
    addSizes(".inbox-sizes", "inboxSize");
    addSizes(".drawer-sizes", "drawerSize");
    view.body.querySelector(".prefs-reset").addEventListener("click", function () { localStorage.removeItem(PREFS_KEY); applyPrefs(view); applyDrawerPrefs(drawer); applyButtonPrefs(); showInboxSettings(); });
    view.body.querySelector(".logout").addEventListener("click", function (event) { logoutDevice(event.currentTarget); });
  }

  function showDrawerSettings() {
    stopConversationPoll();
    var d = ensureDrawer();
    var prefs = readPrefs();
    d.root.querySelector(".title b").textContent = "Настройки";
    d.subtitle.textContent = "";
    d.channels.hidden = true;
    d.copy.hidden = true;
    d.sendAll.hidden = true;
    d.body.innerHTML = '<div class="drawer-preferences"><div class="template-toolbar"><button class="tool notifications" type="button">Уведомления</button><button class="tool operations" type="button">Операции</button><button class="tool templates" type="button">Шаблоны</button><button class="tool guide" type="button">Инструкция</button><button class="tool back" type="button">Назад</button></div><div class="field">Тема<div class="choices themes"></div></div><div class="field">Палитра<div class="palettes"></div></div><label class="field">Цвет<input class="color" type="color" value="' + prefs.color + '"></label><div class="field">Размер<div class="choices sizes"></div></div><button class="tool logout" type="button">Выйти из аккаунта</button></div>';
    d.layer.classList.add("open");
    d.body.querySelector(".templates").addEventListener("click", showDrawerTemplateSettings);
    d.body.querySelector(".notifications").addEventListener("click", showDrawerNotificationSettings);
    d.body.querySelector(".operations").addEventListener("click", showDrawerOperations);
    d.body.querySelector(".guide").addEventListener("click", openGuide);
    d.body.querySelector(".back").addEventListener("click", showChannelMenu);
    d.body.querySelector(".logout").addEventListener("click", function (event) { logoutDevice(event.currentTarget); });
    d.body.querySelector(".color").addEventListener("input", function (event) { writePrefs({ color: event.target.value }); });
    [["light", "Светлая"], ["gray", "Серая"], ["dark", "Тёмная"]].forEach(function (item) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "tool" + (prefs.theme === item[0] ? " active" : "");
      button.textContent = item[1];
      button.addEventListener("click", function () { writePrefs({ theme: item[0] }); showDrawerSettings(); });
      d.body.querySelector(".themes").appendChild(button);
    });
    PALETTE_COLORS.forEach(function (color) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "palette" + (prefs.color.toLowerCase() === color ? " active" : "");
      button.style.background = color;
      button.setAttribute("aria-label", color);
      button.addEventListener("click", function () { writePrefs({ color: color }); showDrawerSettings(); });
      d.body.querySelector(".palettes").appendChild(button);
    });
    [["small", "Маленький"], ["medium", "Средний"], ["large", "Большой"]].forEach(function (item) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "tool" + (prefs.drawerSize === item[0] ? " active" : "");
      button.textContent = item[1];
      button.addEventListener("click", function () { writePrefs({ drawerSize: item[0] }); showDrawerSettings(); });
      d.body.querySelector(".sizes").appendChild(button);
    });
  }

  function showDrawerTemplateSettings() {
    stopConversationPoll();
    var d = ensureDrawer();
    d.root.querySelector(".title b").textContent = "Шаблоны";
    d.subtitle.textContent = "Общие и личные";
    d.channels.hidden = true;
    d.copy.hidden = true;
    d.sendAll.hidden = true;
    d.body.innerHTML = '<div class="template-settings"><div class="template-toolbar"><button class="tool template-add" type="button">Создать</button><button class="tool template-back" type="button">Назад</button></div><div class="template-list"><div class="empty-chat">Загрузка…</div></div></div>';
    d.layer.classList.add("open");
    wheelScrollY(d.body.querySelector(".template-settings"));
    var token = localStorage.getItem(STORAGE_KEY) || "";
    var settings = d.body.querySelector(".template-settings");
    var templates = [];
    var canManageShared = false;
    var templateDragId = 0;
    var payload = function (extra) { return Object.assign({}, context(), extra || {}); };

    function restore() { showDrawerSettings(); }
    function button(label, action, className) {
      var node = document.createElement("button");
      node.type = "button";
      node.className = className || "tool";
      node.textContent = label;
      node.addEventListener("click", action);
      return node;
    }
    function renderList() {
      var list = settings.querySelector(".template-list");
      list.innerHTML = "";
      if (!templates.length) {
        list.innerHTML = '<div class="empty-chat">Шаблонов пока нет</div>';
        return;
      }
      templates.forEach(function (template) {
        var row = document.createElement("div");
        row.className = "template-row";
        row.draggable = true;
        row.dataset.templateId = template.id;
        var drag = document.createElement("span");
        drag.className = "template-drag";
        drag.textContent = "⋮⋮";
        drag.title = "Перетащите шаблон";
        row.appendChild(drag);
        row.addEventListener("dragstart", function (event) { templateDragId = Number(template.id); row.classList.add("dragging"); event.dataTransfer.effectAllowed = "move"; event.dataTransfer.setData("text/plain", String(template.id)); });
        row.addEventListener("dragend", function () { templateDragId = 0; row.classList.remove("dragging"); list.querySelectorAll(".drag-over").forEach(function (node) { node.classList.remove("drag-over"); }); });
        row.addEventListener("dragover", function (event) { event.preventDefault(); row.classList.add("drag-over"); });
        row.addEventListener("dragleave", function () { row.classList.remove("drag-over"); });
        row.addEventListener("drop", async function (event) {
          event.preventDefault(); row.classList.remove("drag-over");
          var sourceIndex = templates.findIndex(function (item) { return Number(item.id) === templateDragId; }), targetIndex = templates.findIndex(function (item) { return Number(item.id) === Number(template.id); });
          if (sourceIndex < 0 || targetIndex < 0 || sourceIndex === targetIndex) return;
          var source = templates.splice(sourceIndex, 1)[0], adjusted = templates.findIndex(function (item) { return Number(item.id) === Number(template.id); }), before = event.clientY < row.getBoundingClientRect().top + row.offsetHeight / 2;
          templates.splice(adjusted + (before ? 0 : 1), 0, source);
          list.innerHTML = '<div class="empty-chat"><span class="spinner" aria-hidden="true"></span><br>Сохраняем личный порядок шаблонов…</div>';
          try { await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "reorder", template_ids: templates.map(function (item) { return Number(item.id); }) })) }); renderList(); }
          catch (error) { window.alert(error.message || "Не удалось сохранить порядок"); load(); }
        });
        var text = document.createElement("div");
        var title = document.createElement("b");
        title.textContent = template.scope === "personal" ? "Личный · " + template.title : "Общий · " + (template.folder || "Без папки") + " · " + template.title;
        var preview = document.createElement("small");
        preview.textContent = String(template.body || "").replace(/\s+/g, " ");
        text.appendChild(title);
        text.appendChild(preview);
        row.appendChild(text);
        var actions = document.createElement("div");
        actions.className = "template-row-actions";
        var star = button(template.favorite ? "★" : "☆", async function () {
          star.disabled = true;
          try {
            var next = !template.favorite;
            await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "favorite", id: Number(template.id), favorite: next })) });
            template.favorite = next;
            renderList();
          } catch (error) { window.alert(error.message || "Не удалось изменить избранное"); }
          finally { star.disabled = false; }
        }, "template-star" + (template.favorite ? " active" : ""));
        star.setAttribute("aria-label", template.favorite ? "Убрать из избранного" : "Добавить в избранное");
        star.title = star.getAttribute("aria-label");
        actions.appendChild(star);
        if (template.editable) actions.appendChild(button("Изменить", function () { edit(template); }));
        row.appendChild(actions);
        list.appendChild(row);
      });
    }
    function edit(template) {
      var current = template || { scope: "personal", folder: "", title: "", body: "" };
      settings.innerHTML = '<div class="template-toolbar"><button class="tool template-back" type="button">← Шаблоны</button></div><form class="template-editor"><label>Тип<select name="scope"><option value="personal">Личный</option><option value="shared">Общий</option></select></label><label class="folder-field">Папка<input name="folder" maxlength="120"></label><label>Название<input name="title" maxlength="120" required></label><label>Содержание<textarea name="body" maxlength="20000" required></textarea></label><div class="variable-list"></div><div class="template-actions"><button class="tool" type="submit">Сохранить</button></div></form>';
      var form = settings.querySelector("form");
      var scope = form.elements.scope;
      scope.value = current.scope;
      scope.disabled = !!template;
      scope.querySelector('option[value="shared"]').disabled = !canManageShared;
      if (!canManageShared && scope.value === "shared") scope.value = "personal";
      var syncFolder = function () { form.querySelector(".folder-field").hidden = scope.value === "personal"; if (scope.value === "personal") form.elements.folder.value = ""; };
      scope.addEventListener("change", syncFolder);
      form.elements.folder.value = current.folder || "";
      syncFolder();
      form.elements.title.value = current.title || "";
      form.elements.body.value = current.body || "";
      variables.forEach(function (item) {
        var insert = button(item.label, function () {
          var area = form.elements.body, marker = "{{" + item.key + "}}", start = area.selectionStart, end = area.selectionEnd;
          area.setRangeText(marker, start, end, "end"); area.focus();
        });
        insert.textContent = item.label + " · {{" + item.key + "}}";
        insert.title = "{{" + item.key + "}}";
        form.querySelector(".variable-list").appendChild(insert);
      });
      settings.querySelector(".template-back").addEventListener("click", load);
      if (template) {
        var remove = button("Удалить", async function () {
          if (!window.confirm("Удалить шаблон «" + template.title + "»?")) return;
          await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "delete", id: template.id, scope: template.scope })) });
          load();
        }, "tool danger");
        form.querySelector(".template-actions").appendChild(remove);
      }
      form.addEventListener("submit", async function (event) {
        event.preventDefault();
        var save = form.querySelector('[type="submit"]');
        save.disabled = true;
        try {
          await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({
            action: template ? "update" : "create", id: template && template.id, scope: scope.value,
            folder: scope.value === "personal" ? "" : form.elements.folder.value.trim(), title: form.elements.title.value.trim(), body: form.elements.body.value.trim()
          })) });
          load();
        } catch (error) {
          window.alert(error.message || "Не удалось сохранить шаблон");
        } finally { save.disabled = false; }
      });
    }
    async function load() {
      settings.innerHTML = '<div class="template-toolbar"><button class="tool template-add" type="button">Создать</button><button class="tool template-back" type="button">Назад</button></div><div class="template-list"><div class="empty-chat">Загрузка…</div></div>';
      settings.querySelector(".template-add").addEventListener("click", function () { edit(null); });
      settings.querySelector(".template-back").addEventListener("click", restore);
      try {
        var data = await request("/templates", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(payload({ action: "list" })) });
        if (!settings.querySelector(".template-list")) return;
        templates = Array.isArray(data.templates) ? data.templates : [];
        variables = Array.isArray(data.variables) ? data.variables : [];
        canManageShared = !!data.can_manage_shared;
        renderList();
      } catch (error) {
        var list = settings.querySelector(".template-list");
        if (list) list.innerHTML = '<div class="empty-chat">Не удалось загрузить шаблоны</div>';
      }
    }
    load();
  }

  function inboxThreadPayload(channel, offset) {
    var item = inbox.activeItem;
    return {
      channel_id: channel.channel_id,
      transport: channel.transport,
      provider: channel.provider || "wazzup",
      thread_channel_id: item.channel_id,
      thread_chat_type: item.chat_type,
      thread_chat_id: item.chat_id,
      offset: Number(offset) || 0
    };
  }

  async function loadInboxChannels(item) {
    if (!item && inbox.channels.length) return inbox.channels;
    var token = localStorage.getItem(STORAGE_KEY) || "";
    var body = { scope: "inbox" };
    if (item) Object.assign(body, {
      thread_channel_id: item.channel_id,
      thread_chat_type: item.chat_type,
      thread_chat_id: item.chat_id
    });
    var data = await request("/channels", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(body) });
    var rows = Array.isArray(data.channels) ? data.channels : [];
    if (!item) {
      inbox.channels = rows;
      if (rows.length) {
        var saved = readPrefs().inboxChannels;
        var current = new Set(rows.map(function (row) { return String(row.channel_id || ""); }));
        var valid = saved.filter(function (channelId) { return current.has(channelId); });
        if (valid.length !== saved.length) writePrefs({ inboxChannels: valid });
      }
    }
    return rows;
  }

  function renderInboxChat(channel) {
    var view = ensureInbox();
    var item = view.activeItem;
    view.view = "chat";
    view.activeChannel = channel;
    setInboxHeader(item.name || item.phone || "Клиент", true, false, true);
    view.body.className = "chat";
    view.body.innerHTML = '<div class="chat-tools"><div class="channel-strip"></div></div><div class="message-feed"><div class="empty-chat inbox-loading"><span class="spinner" aria-hidden="true"></span><span>Загружаем переписку…</span></div></div><div class="composer"><textarea maxlength="4000" placeholder="Сообщение…"></textarea><button class="send" type="button">Отправить</button><div class="compose-error"></div></div>';
    var tools = view.body.querySelector(".chat-tools");
    var strip = view.body.querySelector(".channel-strip");
    wheelScrollX(strip);
    wheelScrollY(view.body.querySelector(".message-feed"));
    view.channels.forEach(function (row) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "channel" + (row.channel_id === channel.channel_id && row.transport === channel.transport ? " active" : "");
      button.textContent = String(row.label || row.name || row.transport).slice(0, 52);
      button.disabled = row.can_send === false;
      button.title = row.send_reason || "";
      button.addEventListener("click", function () { conversationSignature = ""; renderInboxChat(row); loadInboxConversation(false); });
      strip.appendChild(button);
    });
    if (item.getcourse_user_id) {
      var link = document.createElement("button");
      link.type = "button";
      link.className = "channel gc-channel busy";
      link.disabled = true;
      link.textContent = "GetCourse · проверяем покупку…";
      link.title = "Ищем оплаченный доступ в /streams/";
      tools.appendChild(link);
      request("/getcourse-card", { headers: { "Authorization": "Bearer " + (localStorage.getItem(STORAGE_KEY) || "") }, body: JSON.stringify({ platform: "getcourse", phone: item.phone || "", name: item.name || "", entity_type: "user", entity_id: item.getcourse_user_id || "" }) }).then(function (card) {
        link.disabled = false; link.classList.remove("busy"); link.textContent = "GetCourse";
        if (card.found && card.paid_access) { link.classList.add("active"); link.title = "Купивший найден в /streams/"; link.onclick = function () { showGetCourseMini(card); }; }
        else { link.title = "Открыть пользователя GetCourse"; link.onclick = function () { window.open("/user/control/user/update/id/" + encodeURIComponent(item.getcourse_user_id), "_blank", "noopener"); }; }
      }).catch(function () { link.disabled = false; link.classList.remove("busy"); link.textContent = "GetCourse"; link.onclick = function () { window.open("/user/control/user/update/id/" + encodeURIComponent(item.getcourse_user_id), "_blank", "noopener"); }; });
    }
    var send = view.body.querySelector(".send");
    var input = view.body.querySelector("textarea");
    var errorNode = view.body.querySelector(".compose-error");
    function payloadFor(target) {
      return Object.assign(
        { platform: "getcourse", phone: item.phone || "", name: item.name || "", entity_type: "user", entity_id: item.getcourse_user_id || "" },
        inboxThreadPayload(target || channel)
      );
    }
    attachTemplates(view.body.querySelector(".composer"), input, function () { return payloadFor(channel); });
    var feed = view.body.querySelector(".message-feed");
    view.conversationKey = conversationKey(inboxThreadPayload(channel));
    var cached = conversationCache.get(view.conversationKey);
    if (cached) {
      conversationSignature = "";
      renderMessageFeed(feed, cached);
    }
    enableHistoryScroll(feed, function (offset) { return loadInboxConversation(false, offset); });
    send.addEventListener("click", async function () {
      var text = input.value.trim();
      var attachment = { attachment_url: input.dataset.attachmentUrl || "", attachment_type: input.dataset.attachmentType || "" };
      if (!text && !attachment.attachment_url) return;
      send.disabled = true;
      send.classList.add("busy");
      send.textContent = "Отправляем…";
      errorNode.textContent = "";
      try {
        var token = localStorage.getItem(STORAGE_KEY) || "";
        var result = await sendComposerText(text, [channel], payloadFor, token, attachment);
        if (result.sent.length || result.queued.length) { input.value = ""; resizeComposerTextarea(input); delete input.dataset.attachmentUrl; delete input.dataset.attachmentType; }
        conversationSignature = "";
        send.textContent = "Отправка…";
        await Promise.all([loadInboxConversation(false), loadInbox(true, true)]);
        errorNode.textContent = result.status;
      } catch (error) {
        errorNode.textContent = error.message || "Ошибка отправки";
      } finally { send.classList.remove("busy"); send.textContent = "Отправить"; send.disabled = false; }
    });
  }

  async function loadInboxConversation(silent, offset) {
    if (!inbox || inbox.view !== "chat" || !inbox.activeChannel) return;
    var token = localStorage.getItem(STORAGE_KEY) || "";
    var channel = inbox.activeChannel;
    var key = conversationKey(inboxThreadPayload(channel));
    try {
      var data = await request("/conversation", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify(inboxThreadPayload(channel, offset)) });
      if (!inbox || inbox.view !== "chat" || inbox.conversationKey !== key) return;
      if (!offset) rememberConversation(key, data);
      renderMessageFeed(inbox.body.querySelector(".message-feed"), data);
      var send = inbox.body.querySelector(".send");
      var input = inbox.body.querySelector("textarea");
      var errorNode = inbox.body.querySelector(".compose-error");
      if (send && input) {
        send.disabled = data.can_send === false;
        input.disabled = data.can_send === false;
        errorNode.textContent = data.send_reason || "";
      }
    } catch (error) {
      if (!silent && inbox && inbox.view === "chat" && inbox.conversationKey === key) {
        var feed = inbox.body.querySelector(".message-feed");
        feed.innerHTML = "";
        var failure = document.createElement("div");
        failure.className = "empty-chat";
        failure.textContent = error.message || "Ошибка загрузки";
        feed.appendChild(failure);
      }
    }
  }

  async function openInboxItem(item) {
    var token = localStorage.getItem(STORAGE_KEY) || "";
    if (!token) return;
    var view = ensureInbox();
    var generation = ++inboxOpenGeneration;
    view.activeItem = item;
    view.view = "chat";
    setInboxHeader(item.name || item.phone || "Клиент", true, false, true);
    view.body.className = "chat";
    view.body.innerHTML = '<div class="empty">Открываем диалог…</div>';
    request("/inbox/read", {
      headers: { "Authorization": "Bearer " + token },
      body: JSON.stringify({ channel_id: item.channel_id, chat_type: item.chat_type, chat_id: item.chat_id })
    }).catch(function () {});
    try {
      var channels = await loadInboxChannels(item);
      if (!inbox || generation !== inboxOpenGeneration || inbox.activeItem !== item) return;
      view.channels = channels;
      var channel = channels.find(function (row) { return row.channel_id === item.channel_id && row.transport === item.chat_type; });
      if (!channel) { view.body.innerHTML = '<div class="empty">Канал недоступен</div>'; return; }
      conversationSignature = "";
      renderInboxChat(channel);
      await loadInboxConversation(false);
      loadInbox(true, true);
    } catch (error) {
      if (inbox && generation === inboxOpenGeneration && inbox.activeItem === item) {
        view.body.innerHTML = '<div class="empty"></div>';
        view.body.firstChild.textContent = error.message || "Не удалось открыть диалог";
      }
    }
  }

  function renderInbox(data, force) {
    var view = ensureInbox();
    var items = Array.isArray(data.items) ? data.items : [];
    view.items = items;
    view.unanswered = Number(data.unanswered) || 0;
    var signature = JSON.stringify([view.query || "", readPrefs().inboxChannels, data.unread, data.unanswered, items.map(function (item) { return [item.channel_id, item.chat_id, item.sent_at, item.unread, item.preview, item.needs_reply]; })]);
    updateInboxBadge(data.unread, data.unanswered);
    if (view.view !== "list") return;
    if (!force && signature === inboxSignature) return;
    inboxSignature = signature;
    view.body.className = "list-view";
    view.body.innerHTML = '<div class="inbox-filter"><input class="inbox-search" type="search" placeholder="Имя, телефон или ID"><button class="filter-button" type="button">Каналы</button><div class="channel-menu" hidden></div></div><div class="list"></div>';
    view.list = view.body.querySelector(".list");
    wheelScrollY(view.list);
    var search = view.body.querySelector(".inbox-search");
    search.value = view.query || "";
    search.addEventListener("input", function () {
      view.query = search.value.trim();
      clearTimeout(view.searchTimer);
      view.searchTimer = setTimeout(function () { inboxSignature = ""; loadInbox(false); }, 250);
    });
    var menu = view.body.querySelector(".channel-menu");
    view.body.querySelector(".filter-button").addEventListener("click", async function () {
      menu.hidden = !menu.hidden;
      if (menu.hidden) return;
      var channels = await loadInboxChannels();
      var selected = new Set(readPrefs().inboxChannels);
      menu.innerHTML = "";
      channels.forEach(function (channel) {
        var label = document.createElement("label");
        label.className = "channel-option";
        var checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = !selected.size || selected.has(channel.channel_id);
        checkbox.addEventListener("change", function () {
          var checked = Array.from(menu.querySelectorAll("input:checked")).map(function (node) { return node.value; });
          writePrefs({ inboxChannels: checked.length === channels.length ? [] : checked });
          inboxSignature = "";
          loadInbox(false);
        });
        checkbox.value = channel.channel_id;
        var text = document.createElement("span");
        text.textContent = channel.label || channel.name || channel.transport;
        label.appendChild(checkbox);
        label.appendChild(text);
        menu.appendChild(label);
      });
    });
    if (!items.length) {
      view.list.innerHTML = '<div class="empty">Диалоги не найдены</div>';
      return;
    }
    items.forEach(function (item) {
      var button = document.createElement("button");
      button.type = "button";
      button.className = "thread" + (item.unread ? " new" : "");
      var avatar = document.createElement("span");
      avatar.className = "avatar";
      avatar.textContent = inboxInitials(item.name);
      var copy = document.createElement("span");
      copy.className = "copy";
      var name = document.createElement("span");
      name.className = "name";
      name.textContent = item.name || item.phone || "Клиент";
      var preview = document.createElement("span");
      preview.className = "preview";
      preview.textContent = (item.direction === "outgoing" ? "Вы: " : "") + String(item.preview || "Сообщение");
      var meta = document.createElement("span");
      meta.className = "meta";
      meta.textContent = String(item.channel_label || item.chat_type || "Wazzup") + " · " + shortTime(item.sent_at);
      copy.appendChild(name);
      copy.appendChild(preview);
      copy.appendChild(meta);
      var count = document.createElement("span");
      count.className = "count";
      count.hidden = !item.unread;
      count.textContent = item.unread > 99 ? "99+" : String(item.unread || "");
      button.appendChild(avatar);
      button.appendChild(copy);
      button.appendChild(count);
      button.addEventListener("click", function () { openInboxItem(item); });
      view.list.appendChild(button);
    });
  }

  async function loadInbox(silent, skipConversation) {
    var token = localStorage.getItem(STORAGE_KEY) || "";
    if (!token) return;
    var view = ensureInbox();
    var refreshButton = view.refresh;
    if (!silent) {
      refreshButton.disabled = true;
      refreshButton.classList.add("busy");
      refreshButton.setAttribute("aria-busy", "true");
      if (view.view === "list") {
        view.body.className = "list";
        view.body.innerHTML = '<div class="empty inbox-loading"><span class="spinner" aria-hidden="true"></span><span>Загружаем диалоги…</span></div>';
        view.list = view.body;
      }
    }
    try {
      var data = await request("/inbox", {
        headers: { "Authorization": "Bearer " + token },
        body: JSON.stringify({ query: view.query || "", channel_ids: readPrefs().inboxChannels })
      });
      renderInbox(data, !silent);
      if (!skipConversation && inbox && inbox.view === "chat") await loadInboxConversation(true);
    } catch (error) {
      if (error.reauth) {
        localStorage.removeItem(STORAGE_KEY);
        if (inbox) inbox.host.remove();
        inbox = null;
      } else if (!silent) {
        var list = ensureInbox().list;
        list.innerHTML = '<div class="empty">Не удалось загрузить входящие<br><button class="reset inbox-retry" type="button">Повторить</button></div>';
        list.querySelector(".inbox-retry").addEventListener("click", function () { loadInbox(false); });
      }
    } finally {
      if (!silent && inbox && inbox.refresh === refreshButton) {
        refreshButton.disabled = false;
        refreshButton.classList.remove("busy");
        refreshButton.removeAttribute("aria-busy");
      }
    }
  }

  function scheduleInboxPoll() {
    if (inboxTimer) clearTimeout(inboxTimer);
    inboxTimer = setTimeout(async function poll() {
      if (!document.hidden) await loadInbox(true);
      scheduleInboxPoll();
    }, 5000);
  }

  async function registerCardLink() {
    if (!CARD_PAGE) return;
    var token = localStorage.getItem(STORAGE_KEY) || "";
    var ctx = context();
    if (!token || !ctx.phone) return;
    await request("/link", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(ctx) }).catch(function () {});
  }

  async function autoOpenConversation() {
    if (!CARD_PAGE) return;
    var raw = sessionStorage.getItem(AUTO_OPEN_KEY);
    if (!raw) return;
    sessionStorage.removeItem(AUTO_OPEN_KEY);
    var target;
    try { target = JSON.parse(raw); } catch (error) { return; }
    var token = localStorage.getItem(STORAGE_KEY) || "";
    if (!token || !target.channel_id) return;
    var d = ensureDrawer();
    requestAnimationFrame(function () { d.layer.classList.add("open"); });
    try {
      var data = await request("/channels", { headers: { "Authorization": "Bearer " + token }, body: "{}" });
      var channel = (data.channels || []).find(function (row) { return row.channel_id === target.channel_id && row.transport === target.transport; });
      if (!channel) throw new Error("Канал недоступен");
      await openConversation(channel);
    } catch (error) {
      setState("Не удалось открыть переписку", error.message || "Повторите попытку позже.");
    }
  }

  function renderChannels(channels, selected) {
    var d = ensureDrawer();
    d.channels.innerHTML = "";
    if (!Array.isArray(channels) || !channels.length) { d.channels.hidden = true; return; }
    channels.forEach(function (channel) {
      var button = document.createElement("button");
      button.className = "channel" + (selected && channel.channel_id === selected.channel_id && channel.transport === selected.transport ? " active" : "");
      button.type = "button";
      button.textContent = String(channel.label || channel.name || channel.transport).slice(0, 58);
      button.disabled = channel.can_send === false;
      button.title = channel.send_reason || "";
      button.addEventListener("click", function () { openConversation(channel); });
      d.channels.appendChild(button);
    });
    var ctx = context();
    if (getcourseCardLoading || getcourseCard || ctx.entity_type === "user" || ctx.entity_type === "order") {
      var gcButton = document.createElement("button");
      gcButton.className = "channel gc-channel" + (getcourseCard && getcourseCard.paid_access ? " active" : "") + (getcourseCardLoading ? " busy" : "");
      gcButton.type = "button";
      gcButton.disabled = getcourseCardLoading;
      gcButton.textContent = getcourseCardLoading ? getcourseCardLoadingText : getcourseCard && getcourseCard.error ? "GetCourse · повторить" : "GetCourse";
      gcButton.title = getcourseCard && getcourseCard.paid_access ? "Купивший найден в /streams/" : "Открыть пользователя GetCourse";
      gcButton.addEventListener("click", function () { if (getcourseCard && getcourseCard.found) showGetCourseMini(getcourseCard); else if (getcourseCard && getcourseCard.error) { getcourseCard = null; ensureGetCourseCard(ctx); } else { var id = ctx.entity_type === "user" ? ctx.entity_id : ""; if (id) window.open("/user/control/user/update/id/" + encodeURIComponent(id), "_blank", "noopener"); } });
      d.channels.appendChild(gcButton);
    }
    d.channels.hidden = false;
  }

  async function ensureGetCourseCard(ctx) {
    ctx = ctx || context();
    var key = [ctx.entity_type || "", ctx.entity_id || "", ctx.phone || "", ctx.email || ""].join("|");
    if (key !== getcourseCardKey) {
      getcourseCardKey = key;
      getcourseCard = null;
      getcourseCardLoading = false;
    }
    if (getcourseCardLoading || getcourseCard) return;
    var token = localStorage.getItem(STORAGE_KEY) || "";
    getcourseCardLoading = true;
    getcourseCardLoadingText = "Ищем пользователя GetCourse…";
    renderChannels(cardChannels, activeChannel);
    try {
      getcourseCard = await request("/getcourse-card", { headers: { "Authorization": "Bearer " + token }, body: JSON.stringify(ctx), timeoutMs: 20000 });
    } catch (error) {
      getcourseCard = { found: false, error: error.message || "GetCourse недоступен" };
    } finally {
      getcourseCardLoading = false;
      renderChannels(cardChannels, activeChannel);
    }
  }

  function gcNode(tag, className, text) { var node = document.createElement(tag); if (className) node.className = className; if (text !== undefined) node.textContent = String(text); return node; }
  function gcFormatDate(value) { if(!value)return"";var date=new Date(value);return Number.isNaN(date.getTime())?String(value):date.toLocaleString("ru-RU",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"}); }
  function showGcOperationNotice(message,state) {
    var d=ensureDrawer(),notice=d.root.querySelector(".gc-operation-toast");
    if(getcourseNoticeTimer)clearTimeout(getcourseNoticeTimer);
    if(!notice){notice=gcNode("div","gc-operation-toast");d.layer.appendChild(notice)}
    notice.className="gc-operation-toast"+(state==="error"?" error":"");notice.replaceChildren();notice.hidden=false;
    if(!state||state==="pending")notice.append(gcNode("span","spinner"));
    notice.append(document.createTextNode(message));
    if(state&&state!=="pending")getcourseNoticeTimer=setTimeout(function(){notice.hidden=true},12000);
  }
  function pollGetCourseTrial(operationContext,enrollmentId,attempt){
    if(getcourseTrialPollTimer)clearTimeout(getcourseTrialPollTimer);attempt=Number(attempt)||0;
    getcourseTrialPollTimer=setTimeout(async function(){try{var data=await request("/getcourse-test-period",{headers:{"Authorization":"Bearer "+localStorage.getItem(STORAGE_KEY)},body:JSON.stringify(Object.assign({},operationContext,{action:"status",enrollment_id:enrollmentId})),timeoutMs:15000}),status=data.test_period||{};if(getcourseCard&&getcourseCard.item&&getcourseCard.item.enrollment_id===enrollmentId){var trial=getcourseCard._trial||(getcourseCard._trial={});trial.status=status;if(getcourseMiniOpen)showGetCourseMini(getcourseCard)}if(status.operation_pending&&attempt<120){pollGetCourseTrial(operationContext,enrollmentId,attempt+1);return}if(status.status==="active")showGcOperationNotice("Тестовый период выдан. Точный срок подтверждён GetCourse.","success");else if(status.status==="completed")showGcOperationNotice("Тестовый период завершён, доступы закрыты.","success");else showGcOperationNotice(status.reason||"Тестовый период не выдан.","error")}catch(_){if(attempt<120)pollGetCourseTrial(operationContext,enrollmentId,attempt+1);else showGcOperationNotice("Не удалось получить подтверждение. Операция продолжает выполняться в Nexus.","error")}},attempt?5000:1200);
  }
  function pollGetCourseAccess(operationContext,enrollmentId,attempt){
    if(getcourseAccessPollTimer)clearTimeout(getcourseAccessPollTimer);attempt=Number(attempt)||0;
    getcourseAccessPollTimer=setTimeout(async function(){try{var data=await request("/getcourse-access",{headers:{"Authorization":"Bearer "+localStorage.getItem(STORAGE_KEY)},body:JSON.stringify(Object.assign({},operationContext,{action:"read",enrollment_id:enrollmentId,live:false})),timeoutMs:15000}),access=data.access||{};if(getcourseCard&&getcourseCard.item&&getcourseCard.item.enrollment_id===enrollmentId){getcourseCard.access=access;if(access.ok&&!access.pending)getcourseCard._draft={};if(getcourseMiniOpen)showGetCourseMini(getcourseCard)}if((access.pending||!access.ok)&&attempt<120){pollGetCourseAccess(operationContext,enrollmentId,attempt+1);return}if(access.ok)showGcOperationNotice("Доступы GetCourse применены и подтверждены.","success");else showGcOperationNotice("Не удалось подтвердить изменения. Nexus продолжит проверку в фоне.","error")}catch(_){if(attempt<120)pollGetCourseAccess(operationContext,enrollmentId,attempt+1);else showGcOperationNotice("Не удалось получить подтверждение. Операция продолжает выполняться в Nexus.","error")}},attempt?5000:1200);
  }
  function gcGroupLabel(row) { return row.group_kind === "module" ? String(row.module_index !== undefined && row.module_index !== null ? row.module_index : row.name || row.group_id) : ({ standard:"Стандарт", premium:"Премиум", vip:"ВИП", mentorship:"Наставничество", module_standard:"Помодульно" })[row.package_key] || row.name || row.group_id; }
  function gcLessonLabel(row) { const raw=String(row.label||row.key||"").trim(),call=raw.match(/^(?:ВИП|VIP|Созвон)\s*(\d+)$/i);return call?call[1]:raw.replace(/^(\d+)[.,]0$/,"$1"); }
  function gcLessonGroups(rows) {
    var groups={homework:[],calls:[],service:[]},seen={};
    (Array.isArray(rows)?rows:[]).forEach(function(row){var raw=String(row.label||row.key||"").trim(),label=gcLessonLabel(row),kind=/^(?:ВИП|VIP|Созвон)\s*\d+$/i.test(raw)?"calls":/^(?:Доб\.?(?:авим|авили)?\s+в\s+купивших|Чат)$/i.test(label)?"service":"homework",key=kind+":"+label.toLocaleLowerCase("ru-RU");if(seen[key]){seen[key].value=Boolean(seen[key].value||row.value);return}var item=Object.assign({},row,{label:label,value:Boolean(row.value)});seen[key]=item;groups[kind].push(item)});
    return groups;
  }
  function gcShowsProgress(card) { return Boolean(card&&card.paid_access)&&!(/^(?:стандарт|standard)$/i.test(String(card.item&&card.item.tariff||"").trim())); }
  function gcLessonSection(title,rows) { var section=gcNode("section","gc-section"),done=rows.filter(function(row){return row.value}).length,heading=gcNode("h3","",title+" · выполнено "+done+" из "+rows.length),list=gcNode("div","gc-chip-list");rows.forEach(function(row){list.append(gcNode("span","gc-chip"+(row.value?" on":""),(row.value?"✓ ":"○ ")+row.label))});section.append(heading,list);return section; }
  function gcFriendlyAccessError(value) { var text=String(value||""); return /снимок доступов не найден/i.test(text)?"Доступы ещё не загружены. Нажмите «Загрузить доступы».":text||"Доступы временно не загрузились."; }
  function gcAccessRequest(item,payload) { return request("/getcourse-access",{headers:{"Authorization":"Bearer "+localStorage.getItem(STORAGE_KEY)},body:JSON.stringify(Object.assign({},context(),payload,{enrollment_id:item.enrollment_id})),timeoutMs:60000}); }
  function gcTrialRequest(item,payload) { return request("/getcourse-test-period",{headers:{"Authorization":"Bearer "+localStorage.getItem(STORAGE_KEY)},body:JSON.stringify(Object.assign({},context(),payload,{enrollment_id:item.enrollment_id})),timeoutMs:60000}); }
  function gcLessonsRequest(item) { return request("/getcourse-lessons",{headers:{"Authorization":"Bearer "+localStorage.getItem(STORAGE_KEY)},body:JSON.stringify(Object.assign({},context(),{enrollment_id:item.enrollment_id})),timeoutMs:20000}); }
  async function loadGetCourseLessons(card) { if(!gcShowsProgress(card)){card._lessonsLoading=false;return}try { var data=await gcLessonsRequest(card.item);card.item.lessons=data.lessons||[];card._lessonsError=""; } catch(error) { card._lessonsError=error.message||"Не удалось загрузить ДЗ и созвоны"; } finally { card._lessonsLoading=false;if(getcourseMiniOpen)showGetCourseMini(card); } }
  async function loadGetCourseAccess(card) { try { var data=await gcAccessRequest(card.item,{action:"read",live:true});card.access=data.access;card._accessError=""; } catch(error) { card._accessError=error.message||"Не удалось проверить доступы"; } finally { card._accessLoading=false;if(getcourseMiniOpen)showGetCourseMini(card); } }
  function renderGetCourseTrial(card,root) {
    var trial=card._trial||(card._trial={open:false,loading:false,status:null,error:"",days:1,courses:{}});if(!trial.open)return;
    if(trial.loading){var loading=gcNode("div","gc-loading"),spinner=gcNode("span","spinner"),text=gcNode("span","","Проверяем возможность тестового периода…");loading.append(spinner,text);root.append(loading);return}
    var panel=gcNode("section","gc-confirm"),title=gcNode("h3","","Тестовый период");panel.append(title);
    if(trial.error){panel.append(gcNode("div","gc-notice bad",trial.error));var retryActions=gcNode("div","gc-confirm-actions"),closeError=gcNode("button","tool","Закрыть"),retry=gcNode("button","tool","Повторить");closeError.type=retry.type="button";closeError.onclick=function(){trial.open=false;showGetCourseMini(card)};retry.onclick=function(){openGetCourseTrial(card)};retryActions.append(closeError,retry);panel.append(retryActions);root.append(panel);return}
    if(trial.status&&!trial.status.can_issue&&!trial.repeat){var statusNotice=gcNode("div",trial.status.operation_pending?"gc-pending":"gc-notice");if(trial.status.operation_pending)statusNotice.append(gcNode("span","spinner"));var statusText=gcNode("span","",trial.status.reason||"Тестовый период уже выдавался");if(trial.status.expires_at&&!trial.status.operation_pending)statusText.append(gcNode("small","gc-trial-date","Закроется: "+gcFormatDate(trial.status.expires_at)));statusNotice.append(statusText);panel.append(statusNotice);var doneActions=gcNode("div","gc-confirm-actions"),done=gcNode("button","tool","Закрыть");done.type="button";done.onclick=function(){trial.open=false;trial.repeat=false;showGetCourseMini(card)};doneActions.append(done);if(trial.status.can_repeat){var repeat=gcNode("button","tool","Выдать повторно");repeat.type="button";repeat.onclick=function(){trial.repeat=true;trial.courses={};showGetCourseMini(card)};doneActions.append(repeat)}if(trial.status.status==="active"){var revoke=gcNode("button","tool danger","Забрать тестовый период");revoke.type="button";revoke.onclick=function(){revokeGetCourseTrial(card,revoke)};doneActions.append(revoke)}panel.append(doneActions);root.append(panel);return}
    title.textContent=trial.repeat?"Повторно выдать тестовый период":"Тестовый период";var grid=gcNode("div","gc-confirm-grid"),daysLabel=gcNode("label","","Количество дней"),days=gcNode("input","gc-trial-days");days.type="number";days.min="1";days.max="90";days.value=String(trial.days||1);days.oninput=function(){trial.days=Math.max(1,Math.min(90,Number(days.value)||1))};daysLabel.append(days);var courseBox=gcNode("div"),courseTitle=gcNode("b","","Курсы"),courseOptions=gcNode("div","gc-access-options");["puppy","dog"].forEach(function(key){var selected=Boolean(trial.courses[key]),button=gcNode("button","gc-chip"+(selected?" on":""),(selected?"✓ ":"")+(key==="puppy"?"Щенок":"Собака"));button.type="button";button.onclick=function(){trial.courses[key]=!selected;showGetCourseMini(card)};courseOptions.append(button)});courseBox.append(courseTitle,courseOptions);grid.append(daysLabel,courseBox);panel.append(grid);var actions=gcNode("div","gc-confirm-actions"),cancel=gcNode("button","tool","Отмена"),issue=gcNode("button","tool",trial.repeat?"Выдать повторно":"Выдать");cancel.type=issue.type="button";cancel.onclick=function(){trial.open=false;trial.repeat=false;showGetCourseMini(card)};issue.disabled=!Object.keys(trial.courses).some(function(key){return trial.courses[key]});issue.onclick=function(){issueGetCourseTrial(card,issue)};actions.append(cancel,issue);panel.append(actions);root.append(panel)
  }
  async function openGetCourseTrial(card){var trial=card._trial||(card._trial={days:1,courses:{}});trial.open=true;trial.repeat=false;trial.loading=true;trial.error="";showGetCourseMini(card);try{var data=await gcTrialRequest(card.item,{action:"status"});trial.status=data.test_period||{};trial.courses={}}catch(error){trial.error=error.message||"Не удалось проверить тестовый период"}finally{trial.loading=false;if(getcourseMiniOpen)showGetCourseMini(card)}}
  async function issueGetCourseTrial(card,button){var trial=card._trial,courses=Object.keys(trial.courses).filter(function(key){return trial.courses[key]}),enrollmentId=card.item.enrollment_id,operationContext=Object.assign({},context()),repeat=Boolean(trial.repeat);if(!courses.length)return;button.disabled=true;button.classList.add("busy");button.textContent="Принимаем задачу…";try{var data=await gcTrialRequest(card.item,{action:repeat?"repeat":"create",days:trial.days,courses:courses});trial.status=data.test_period||{can_issue:false,operation_pending:true,reason:"Команда принята. Тестовый период будет выдан в фоне."};trial.repeat=false;trial.error="";showGcOperationNotice("Задача принята. Окно можно закрыть — Nexus продолжит сам.","success");pollGetCourseTrial(operationContext,enrollmentId,0)}catch(error){trial.error=error.message||"Не удалось принять команду";showGcOperationNotice(trial.error,"error")}finally{if(getcourseMiniOpen)showGetCourseMini(card)}}
  async function revokeGetCourseTrial(card,button){if(!window.confirm("Забрать тестовый период и закрыть доступы сейчас?"))return;var trial=card._trial,enrollmentId=card.item.enrollment_id,operationContext=Object.assign({},context());button.disabled=true;button.classList.add("busy");button.textContent="Передаём команду…";try{var data=await gcTrialRequest(card.item,{action:"revoke"});trial.status=data.test_period||{can_issue:false,operation_pending:true,reason:"Команда принята. Доступы закрываются в фоне."};trial.error="";showGcOperationNotice(trial.status.reason||"Команда принята. Доступы закрываются в фоне.","pending");pollGetCourseTrial(operationContext,enrollmentId,0)}catch(error){trial.error=error.message||"Не удалось принять команду";showGcOperationNotice(trial.error,"error")}finally{if(getcourseMiniOpen)showGetCourseMini(card)}}
  function showGetCourseMini(card) {
    stopConversationPoll();
    getcourseMiniOpen=true;
    var showProgress=gcShowsProgress(card),startParts=!card._partsStarted;if(startParts){card._partsStarted=true;card._lessonsLoading=showProgress;card._accessLoading=true;card._lessonsError="";card._accessError="";}
    var d=ensureDrawer(),item=card.item||{},access=card.access||{},draft=card._draft||(card._draft={}),groups=Array.isArray(access.items)?access.items:[];
    d.root.querySelector(".title b").textContent="GetCourse · "+(item.name||"ученик");d.subtitle.textContent=item.email||item.phone||"";d.copy.hidden=true;d.sendAll.hidden=true;d.channels.hidden=false;
    d.body.innerHTML="";var root=gcNode("div","gc-widget"),toolbar=gcNode("div","template-toolbar"),back=gcNode("button","tool","← Диалоги"),profile=gcNode("a","tool",item.email||"Открыть GetCourse"),trialButton=gcNode("button","tool","Тестовый период");back.type=trialButton.type="button";back.onclick=showChannelMenu;trialButton.onclick=function(){openGetCourseTrial(card)};profile.href=card.profile_url||"#";profile.target="_blank";profile.rel="noopener";toolbar.append(back,profile,trialButton);root.append(toolbar);renderGetCourseTrial(card,root);
    var facts=gcNode("div","gc-facts");[["Курс",item.course_display||item.course],["Поток",item.stream_display||item.stream],["Куратор",item.curator_name||item.curator],["Тариф",item.tariff],["Телефон",item.phone],["Менеджер",item.manager_name]].forEach(function(pair){var fact=gcNode("div","gc-fact"),label=gcNode("span","",pair[0]),value=gcNode("b","",pair[1]||"—");fact.append(label,value);facts.append(fact)});root.append(facts);
    var pane=gcNode("div","gc-pane");root.append(pane);d.body.append(root);d.layer.classList.add("open");
    var lessons=gcLessonGroups(item.lessons),lessonSections=[["ДЗ · только просмотр",lessons.homework],["Созвоны · только просмотр",lessons.calls],["Этапы · только просмотр",lessons.service]];
    if(showProgress){if(card._lessonsLoading){var lessonLoading=gcNode("div","gc-loading"),lessonSpinner=gcNode("span","spinner"),lessonText=gcNode("span","","Загружаем ДЗ и созвоны…");lessonLoading.append(lessonSpinner,lessonText);pane.append(lessonLoading)}else if(card._lessonsError){var lessonFailure=gcNode("div","gc-notice bad",card._lessonsError),lessonRetry=gcNode("button","tool","Повторить загрузку ДЗ и созвонов");lessonRetry.type="button";lessonRetry.onclick=function(){card._lessonsLoading=true;card._lessonsError="";showGetCourseMini(card);loadGetCourseLessons(card)};lessonFailure.append(document.createElement("br"),document.createElement("br"),lessonRetry);pane.append(lessonFailure)}else{lessonSections.forEach(function(pair){if(pair[1].length)pane.append(gcLessonSection(pair[0],pair[1]))});if(!lessonSections.some(function(pair){return pair[1].length}))pane.append(gcLessonSection("ДЗ · только просмотр",[]))}}
    if(card._accessLoading){var accessLoading=gcNode("div","gc-loading"),accessSpinner=gcNode("span","spinner"),accessText=gcNode("span","","Проверяем актуальные доступы GetCourse…");accessLoading.append(accessSpinner,accessText);pane.append(accessLoading);if(startParts){if(showProgress)loadGetCourseLessons(card);loadGetCourseAccess(card)}return}
    if(card._accessError) pane.append(gcNode("div","gc-notice bad",card._accessError));
    function busy(button,text){button.disabled=true;button.classList.add("busy");button.textContent=text}
    function loadLive(button){busy(button,"Проверяем GetCourse…");gcAccessRequest(item,{action:"read",live:true}).then(function(data){card.access=data.access;card._draft={};card._preview=null;card._accessError="";showGetCourseMini(card)}).catch(function(error){card._accessError=error.message||"Не удалось загрузить доступы";showGetCourseMini(card)})}
    if(!access.ok){var failure=gcNode("div","gc-notice",card._accessError||gcFriendlyAccessError(access.error)),retry=gcNode("button","tool","Повторить проверку доступов");retry.type="button";retry.onclick=function(){card._accessLoading=true;card._accessError="";showGetCourseMini(card);loadGetCourseAccess(card)};failure.append(document.createElement("br"),document.createElement("br"),retry);pane.append(failure);if(startParts&&showProgress)loadGetCourseLessons(card);return}
    if(access.pending){var pending=gcNode("div","gc-pending"),pendingSpinner=gcNode("span","spinner"),pendingText=gcNode("span","","Изменения выполняются в GetCourse. Окно можно закрыть — Nexus продолжит сам.");pending.append(pendingSpinner,pendingText);pane.append(pending)}
    function accessChip(group,label){var key=String(group.group_id),value=Object.prototype.hasOwnProperty.call(draft,key)?draft[key]:Boolean(group.enabled),chip=gcNode("button","gc-chip"+(value?" on":"")+(Object.prototype.hasOwnProperty.call(draft,key)?" changed":""),(value?"✓ ":"")+(label||gcGroupLabel(group)));chip.type="button";chip.disabled=Boolean(access.pending)||(Boolean(group.inferred)&&value);chip.title=group.inferred?"Тариф определён по заказу с частичной оплатой":group.name||"";chip.onclick=function(){var next=!value;if(next&&group.group_kind==="package")groups.filter(function(row){return row.course_key===group.course_key&&row.group_kind==="package"&&String(row.group_id)!==key}).forEach(function(row){draft[String(row.group_id)]=false});draft[key]=next;Object.keys(draft).forEach(function(id){var original=groups.find(function(row){return String(row.group_id)===id});if(original&&Boolean(original.enabled)===draft[id])delete draft[id]});showGetCourseMini(card)};return chip}
    groups=groups.filter(function(row){return !(row.course_key==="puppy"&&row.package_key==="module_standard")});var layout=gcNode("div","gc-access-layout");["puppy","dog"].forEach(function(course){var courseBox=gcNode("section","gc-access-course"),heading=gcNode("h3","",course==="puppy"?"Щенок":"Собака");courseBox.append(heading);[["Тариф",groups.filter(function(row){return row.course_key===course&&row.group_kind==="package"})],["Модули",groups.filter(function(row){return row.course_key===course&&row.group_kind==="module"}).sort(function(a,b){return Number(a.module_index)-Number(b.module_index)})]].forEach(function(pair){var row=gcNode("div","gc-access-row"),label=gcNode("span","",pair[0]),options=gcNode("div","gc-access-options");if(pair[1].length)pair[1].forEach(function(group){options.append(accessChip(group))});else options.textContent="—";row.append(label,options);courseBox.append(row)});layout.append(courseBox)});
    var mini=gcNode("section","gc-access-minis"),miniTitle=gcNode("h3","","Мини-курсы"),miniOptions=gcNode("div","gc-access-options");[["4842617","Намордник"],["4842619","Намордник + ОС"],["4119459","Поводок"],["4217019","Послушание"],["4443745","За 15 минут"]].forEach(function(def){var group=groups.find(function(row){return String(row.group_id)===def[0]});if(group)miniOptions.append(accessChip(group,def[1]));else{var missing=gcNode("button","gc-chip",def[1]);missing.type="button";missing.disabled=true;miniOptions.append(missing)}});mini.append(miniTitle,miniOptions);layout.append(mini);pane.append(layout);
    var actions=gcNode("div","gc-actions"),refresh=gcNode("button","tool","Обновить"),apply=gcNode("button","tool","Проверить и применить");refresh.type=apply.type="button";refresh.disabled=Boolean(access.pending);apply.disabled=!Object.keys(draft).length||Boolean(access.pending);refresh.onclick=function(){card._accessLoading=true;showGetCourseMini(card);loadGetCourseAccess(card)};apply.onclick=async function(){var enrollmentId=item.enrollment_id,operationContext=Object.assign({},context());busy(apply,"Принимаем задачу…");try{var changes=Object.entries(draft).map(function(pair){return{group_id:pair[0],enabled:pair[1]}}),preview=await gcAccessRequest(item,{action:"preview",changes:changes}),result=await gcAccessRequest(item,{action:"apply",request_id:preview.request_id,changes:changes});card.access=result.access||preview.access||card.access;card._draft={};card._accessError="";showGcOperationNotice("Задача принята. Окно можно закрыть — Nexus продолжит сам.","success");showGetCourseMini(card);pollGetCourseAccess(operationContext,enrollmentId,0)}catch(error){card._accessError=error.message||"Не удалось принять команду";showGcOperationNotice(card._accessError,"error");showGetCourseMini(card)}};actions.append(refresh,apply);pane.append(actions);if(startParts){if(showProgress)loadGetCourseLessons(card);loadGetCourseAccess(card)}
  }

  async function showChannelMenu() {
    stopConversationPoll();
    getcourseMiniOpen = false;
    var d = ensureDrawer();
    var ctx = context();
    d.subtitle.textContent = ctx.phone || "Телефон в карточке не найден";
    d.copy.disabled = !ctx.phone;
    var token = localStorage.getItem(STORAGE_KEY) || "";
    if (!token) {
      activationForm();
      return;
    }
    setState("Каналы", "Выберите канал.", '<div class="spinner"></div>');
    try {
      var data = await request("/channels", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify(ctx) });
      var channels = Array.isArray(data.channels) ? data.channels : [];
      if (!channels.length) throw new Error("Нет доступных каналов.");
      cardChannels = channels;
      renderChannels(cardChannels, null);
      ensureGetCourseCard(ctx);
      var preferred = channels.find(function (channel) { return channel.has_chat && channel.can_send !== false; }) || channels.find(function (channel) { return channel.can_send !== false; });
      if (preferred) {
        await openConversation(preferred);
        return;
      }
      var card = setState("Нет доступного канала", ctx.phone || "");
      var list = document.createElement("div");
      list.className = "channel-list";
      channels.forEach(function (channel) {
        var button = document.createElement("button");
        button.className = "submit";
        button.type = "button";
        button.textContent = String(channel.label || channel.transport || channel.name).slice(0, 72);
        button.disabled = true;
        button.title = channel.send_reason || "Канал недоступен";
        button.addEventListener("click", function () { openConversation(channel); });
        list.appendChild(button);
      });
      card.appendChild(list);
    } catch (error) {
      var failure = setState("Не удалось получить каналы", error.message || "Повторите попытку позже.");
      var retry = document.createElement("button");
      retry.className = "submit";
      retry.type = "button";
      retry.textContent = "Повторить";
      retry.addEventListener("click", showChannelMenu);
      failure.appendChild(retry);
    }
  }

  function conversationPayload(channel, offset) {
    return Object.assign({}, context(), { channel_id: channel.channel_id, transport: channel.transport, provider: channel.provider || "wazzup", offset: Number(offset) || 0 });
  }

  function shortTime(value) {
    var date = new Date(value);
    return Number.isNaN(date.getTime()) ? "" : date.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
  }

  function enableHistoryScroll(feed, loader) {
    feed.addEventListener("scroll", async function () {
      if (feed.scrollTop > 40 || feed._historyLoading || !feed._nextHistoryOffset) return;
      feed._historyLoading = true;
      var oldHeight = feed.scrollHeight;
      try {
        await loader(feed._nextHistoryOffset);
        feed.scrollTop = Math.max(1, feed.scrollHeight - oldHeight);
      } catch (error) {
        var failure = document.createElement("div");
        failure.className = "history-note";
        failure.textContent = error.message || "Ошибка загрузки";
        feed.insertBefore(failure, feed.firstChild);
      } finally {
        feed._historyLoading = false;
      }
    });
  }

  function renderMessageFeed(feed, data) {
    var messages = Array.isArray(data.messages) ? data.messages : [];
    var offset = Number(data.offset) || 0;
    var nextOffset = Number(data.next_offset) || 0;
    if (data.has_more && !feed._historyComplete) feed._nextHistoryOffset = Math.max(Number(feed._nextHistoryOffset) || 0, nextOffset);
    else if (!data.has_more && nextOffset >= (Number(feed._nextHistoryOffset) || 0)) {
      feed._nextHistoryOffset = 0;
      feed._historyComplete = true;
    }
    var signature = JSON.stringify([data.history_status, data.can_send, data.send_reason, messages.map(function (item) { return [item.external_id, item.status, item.text, item.sent_at]; })]);
    if (!offset && signature === conversationSignature) return;
    if (!offset) conversationSignature = signature;
    var pinned = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 80;
    var existing = null;
    if (offset) {
      existing = document.createDocumentFragment();
      while (feed.firstChild) existing.appendChild(feed.firstChild);
    }
    feed.innerHTML = "";
    if (!offset && !data.history_complete) {
      var note = document.createElement("div");
      note.className = "history-note";
      if (data.history_status === "syncing") note.textContent = "Загружаем последние сообщения. Можно писать сразу.";
      else if (data.history_status === "no_access") note.textContent = "Нет доступа к чатам Wazzup. Назначьте сотруднику роль «Руководитель».";
      else if (data.history_status === "error") note.textContent = "Wazzup временно не отдал старую историю. Новые сообщения продолжат поступать через webhook; при следующем открытии интеграция попробует снова.";
      else if (data.history_status === "not_found") note.textContent = "Wazzup не нашёл диалог этого номера в выбранном канале. Если клиент напишет или вы начнёте диалог, он привяжется автоматически.";
      else if (data.history_status === "not_started") note.textContent = "Истории с сообществом нет.";
      else note.textContent = "Проверяем существующую историю Wazzup. Новые сообщения и статусы появляются здесь автоматически.";
      feed.appendChild(note);
    }
    if (!offset && !messages.length) {
      var empty = document.createElement("div");
      empty.className = "empty-chat";
      empty.textContent = "Сообщений по этому каналу пока нет. Можно начать диалог ниже.";
      feed.appendChild(empty);
    }
    if (!offset && data.send_reason) {
      var sendNote = document.createElement("div");
      sendNote.className = "history-note";
      sendNote.textContent = data.send_reason;
      feed.appendChild(sendNote);
    }
    messages.forEach(function (message) {
      var row = document.createElement("div");
      row.className = "message-row " + (message.direction === "outgoing" ? "outgoing" : "incoming");
      var bubble = document.createElement("div");
      bubble.className = "bubble";
      var attachments = Array.isArray(message.attachments) && message.attachments.length ? message.attachments : [{ content_uri: message.content_uri, content_type: message.content_type, filename: message.filename }];
      var hasImage = false;
      attachments.forEach(function (file) {
      var contentUri = String(file.content_uri || "");
      var contentType = String(file.content_type || "").toLowerCase();
      var isImage = /^image(?:\/|$)/.test(contentType) || /\.(?:jpe?g|png|gif|webp|bmp)(?:\?|$)/i.test(contentUri);
      hasImage = hasImage || isImage;
      if (isImage && /^https:\/\//i.test(contentUri)) {
        var imageLink = document.createElement("a");
        imageLink.className = "message-image-link";
        imageLink.href = contentUri;
        imageLink.target = "_blank";
        imageLink.rel = "noopener noreferrer";
        var image = document.createElement("img");
        image.className = "message-image";
        image.src = contentUri;
        image.alt = String(file.filename || "Изображение");
        image.loading = "lazy";
        imageLink.appendChild(image);
        bubble.appendChild(imageLink);
      } else if (contentUri && /^https:\/\//i.test(contentUri)) {
        var attachment = document.createElement("a");
        attachment.className = "attachment";
        attachment.href = contentUri;
        attachment.target = "_blank";
        attachment.rel = "noopener noreferrer";
        attachment.textContent = file.filename || "Открыть вложение";
        bubble.appendChild(attachment);
      }
      });
      var visibleText = String(message.text || "");
      if (!(hasImage && /^\[Вложение:/i.test(visibleText))) {
        var text = document.createElement("div");
        text.textContent = visibleText || (attachments.some(function (file) { return !!file.content_uri; }) ? "Вложение" : "Сообщение без текста");
        bubble.appendChild(text);
      }
      var meta = document.createElement("div");
      meta.className = "message-meta";
      meta.textContent = [message.author_name || "", shortTime(message.sent_at), message.status || ""].filter(Boolean).join(" · ");
      bubble.appendChild(meta);
      row.appendChild(bubble);
      feed.appendChild(row);
    });
    if (existing) feed.appendChild(existing);
    if (!offset && (pinned || !messages.length)) feed.scrollTop = feed.scrollHeight;
  }

  async function fetchConversation(channel, feed, silent, offset) {
    var token = localStorage.getItem(STORAGE_KEY) || "";
    var payload = conversationPayload(channel, offset);
    try {
      var data = await request("/conversation", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify(payload) });
      if (!offset) rememberConversation(conversationKey(payload), data);
      renderMessageFeed(feed, data);
      return data;
    } catch (error) {
      if (!silent) throw error;
      return null;
    }
  }

  function scheduleConversationPoll(channel, feed) {
    if (channel.provider === "salebot") return;
    if (conversationTimer) clearTimeout(conversationTimer);
    conversationTimer = setTimeout(async function poll() {
      if (!drawer || activeChannel !== channel) return;
      if (!document.hidden) await fetchConversation(channel, feed, true);
      scheduleConversationPoll(channel, feed);
    }, 5000);
  }

  async function openConversation(channel) {
    getcourseMiniOpen = false;
    stopConversationPoll();
    activeChannel = channel;
    var generation = ++conversationGeneration;
    conversationSignature = "";
    var d = ensureDrawer();
    var ctx = context();
    var key = conversationKey(conversationPayload(channel));
    d.subtitle.textContent = (ctx.phone || "Номер не найден") + " · " + String(channel.transport || "").toUpperCase();
    renderChannels(cardChannels, channel);
    var cached = conversationCache.get(key);
    if (!cached) setState("Загружаем переписку", "Получаем сохранённые сообщения и статусы.", '<div class="spinner"></div>');
    try {
      var initial = cached || await fetchConversation(channel, document.createElement("div"), false);
      if (!drawer || activeChannel !== channel || generation !== conversationGeneration) return;
      d.body.innerHTML = "";
      var shell = document.createElement("div");
      shell.className = "chat-shell";
      var tools = document.createElement("div");
      tools.className = "chat-tools";
      var back = document.createElement("button");
      back.className = "tool";
      back.type = "button";
      back.textContent = "Каналы";
      back.addEventListener("click", showChannelMenu);
      var channelName = document.createElement("div");
      channelName.className = "channel-name";
      channelName.textContent = channel.label || (String(channel.transport || "").toUpperCase() + " · " + (channel.name || "Wazzup"));
      var nativeButton = document.createElement("button");
      nativeButton.className = "tool";
      nativeButton.type = "button";
      nativeButton.textContent = channel.provider === "vk" ? "Открыть VK" : channel.provider === "salebot" ? "Обновить" : "Открыть Wazzup";
      nativeButton.addEventListener("click", function () {
        if (channel.provider === "vk" && initial.chat_id) window.open("https://vk.com/im?sel=" + encodeURIComponent(initial.chat_id), "_blank", "noopener");
        else if (channel.provider === "salebot") { conversationCache.delete(conversationKey(conversationPayload(channel))); openConversation(channel); }
        else loadFrame(channel);
      });
      tools.appendChild(back);
      tools.appendChild(channelName);
      tools.appendChild(nativeButton);
      var feed = document.createElement("div");
      feed.className = "message-feed";
      wheelScrollY(feed);
      enableHistoryScroll(feed, function (offset) { return fetchConversation(channel, feed, false, offset); });
      var composer = document.createElement("div");
      composer.className = "composer";
      var input = document.createElement("textarea");
      input.maxLength = 4000;
      input.placeholder = "Введите сообщение…";
      var send = document.createElement("button");
      send.className = "send";
      send.type = "button";
      send.textContent = "Отправить";
      var errorNode = document.createElement("div");
      errorNode.className = "compose-error";
      if (initial.can_send === false || context().read_only) {
        input.disabled = true;
        send.disabled = true;
        errorNode.textContent = context().read_only ? "Откройте карточку клиента, чтобы ответить." : (initial.send_reason || "Канал недоступен для нового диалога.");
      }
      send.addEventListener("click", async function () {
        var text = input.value.trim();
        var attachment = { attachment_url: input.dataset.attachmentUrl || "", attachment_type: input.dataset.attachmentType || "" };
        if (!text && !attachment.attachment_url) return;
        send.disabled = true;
        send.classList.add("busy");
        send.textContent = "Отправляем…";
        errorNode.textContent = "";
        try {
          var token = localStorage.getItem(STORAGE_KEY) || "";
          var targets = d.sendAll.querySelector("input").checked ? cardChannels : [channel];
          var result = await sendComposerText(text, targets, conversationPayload, token, attachment);
          if (result.sent.length || result.queued.length) { input.value = ""; resizeComposerTextarea(input); delete input.dataset.attachmentUrl; delete input.dataset.attachmentType; }
          conversationSignature = "";
          send.textContent = "Отправка…";
          await Promise.all([fetchConversation(channel, feed, false), loadInbox(true, true)]);
          errorNode.textContent = result.status;
        } catch (error) {
          errorNode.textContent = error.message || "Не удалось отправить сообщение";
        } finally { send.classList.remove("busy"); send.textContent = "Отправить"; send.disabled = false; }
      });
      composer.appendChild(input);
      composer.appendChild(send);
      composer.appendChild(errorNode);
      attachTemplates(composer, input, context);
      shell.appendChild(tools);
      shell.appendChild(feed);
      shell.appendChild(composer);
      d.body.appendChild(shell);
      d.sendAll.hidden = false;
      conversationSignature = "";
      renderMessageFeed(feed, initial);
      scheduleConversationPoll(channel, feed);
      if (cached) fetchConversation(channel, feed, true);
      setTimeout(function () { input.focus(); }, 30);
    } catch (error) {
      var card = setState("Не удалось открыть переписку", error.message || "Повторите попытку позже.");
      var retry = document.createElement("button");
      retry.className = "submit";
      retry.type = "button";
      retry.textContent = "Повторить";
      retry.addEventListener("click", function () { openConversation(channel); });
      card.appendChild(retry);
    }
  }

  async function loadFrame(channel) {
    stopConversationPoll();
    var d = ensureDrawer();
    var ctx = context();
    setState("Открываем Wazzup", "Передаём номер в выбранный канал.", '<div class="spinner"></div>');
    try {
      var token = localStorage.getItem(STORAGE_KEY) || "";
      var requestContext = Object.assign({}, ctx, channel ? { transport: channel.transport, channel_id: channel.channel_id } : {});
      var data = await request("/iframe-link", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify(requestContext) });
      d.subtitle.textContent = (data.phone || ctx.phone || "Номер не найден") + " · " + (data.transport ? String(data.transport).toUpperCase() + " · " : "") + (data.admin_name || "Wazzup");
      renderChannels(data.channels, data.transport);
      d.body.innerHTML = "";
      var frame = document.createElement("iframe");
      frame.className = "frame";
      frame.title = "Чаты Wazzup";
      frame.allow = "microphone *; clipboard-write *";
      frame.referrerPolicy = "strict-origin-when-cross-origin";
      frame.src = data.url;
      d.body.appendChild(frame);
    } catch (error) {
      if (error.reauth) {
        localStorage.removeItem(STORAGE_KEY);
        activationForm("Срок доступа закончился или устройство отозвано. Введите код из панели интеграции.");
      } else {
        var card = setState("Не удалось открыть Wazzup", error.message || "Повторите попытку позже.");
        var retry = document.createElement("button");
        retry.className = "submit";
        retry.type = "button";
        retry.textContent = "Повторить";
        retry.addEventListener("click", showChannelMenu);
        card.appendChild(retry);
      }
    }
  }

  async function copyPhone() {
    var phone = context().phone;
    if (!phone) return;
    try {
      await navigator.clipboard.writeText(phone);
      drawer.copy.textContent = "Номер скопирован";
      setTimeout(function () { if (drawer) drawer.copy.textContent = "Скопировать номер"; }, 1600);
    } catch (error) {
      var area = document.createElement("textarea");
      area.value = phone;
      document.body.appendChild(area);
      area.select();
      document.execCommand("copy");
      area.remove();
    }
  }

  function openDrawer(event) {
    forcedContext = null;
    lastFocus = event && event.currentTarget;
    var d = ensureDrawer();
    requestAnimationFrame(function () { d.layer.classList.add("open"); });
    document.documentElement.style.setProperty("--nexus-wazzup-open", "1");
    showChannelMenu();
  }

  function closeDrawer() {
    if (!drawer) return;
    getcourseMiniOpen = false;
    stopConversationPoll();
    drawer.layer.classList.remove("open");
    var frame = drawer.body.querySelector("iframe");
    if (frame) frame.src = "about:blank";
    setTimeout(function () {
      if (!drawer || drawer.layer.classList.contains("open")) return;
      drawer.host.remove();
      drawer = null;
    }, 190);
    if (lastFocus && lastFocus.focus) lastFocus.focus();
    forcedContext = null;
  }

  document.addEventListener("keydown", function (event) { if (event.key === "Escape" && drawer) closeDrawer(); });

  function boot() {
    if (!isAdminShell()) return;
    if (localStorage.getItem(STORAGE_KEY)) {
      ensureInbox();
      loadInbox(true);
      scheduleInboxPoll();
    }
    if (!CARD_PAGE && !STAFF_PAGE) return;
    if (STAFF_PAGE ? placeStaffButton() : placeButton()) {
      registerCardLink();
      autoOpenConversation();
      return;
    }
    var observer = new MutationObserver(function () {
      if (STAFF_PAGE ? placeStaffButton() : placeButton()) {
        observer.disconnect();
        registerCardLink();
        autoOpenConversation();
      }
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
    setTimeout(function () { observer.disconnect(); }, 30000);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot, { once: true });
  else boot();
})();
