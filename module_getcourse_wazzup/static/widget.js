(function () {
  "use strict";

  var script = document.currentScript;
  var API = String((script && script.dataset && script.dataset.nexusWazzupApi) || "").replace(/\/$/, "");
  var TEST_MODE = !!(script && script.dataset && script.dataset.nexusWazzupTest === "1");
  var TEST_SOURCE_URL = String((script && script.dataset && script.dataset.nexusWazzupSourceUrl) || "");
  var SUPPORTED = /\/(?:user\/control\/user\/update\/id|sales\/control\/deal\/update\/id)\/\d+(?:\/|$)/i;
  var STAFF_PAGE = /\/user\/control\/user(?:\/index)?\/?$/i.test(location.pathname);
  var STORAGE_KEY = "nexus:getcourse-wazzup:device-token:v1";
  var BUTTON_ID = "nexus-getcourse-wazzup-button";
  var DRAWER_ID = "nexus-getcourse-wazzup-drawer";
  if (!API || (!TEST_MODE && !SUPPORTED.test(location.pathname) && !STAFF_PAGE) || document.getElementById(BUTTON_ID)) return;

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

  function context() {
    return { phone: findPhone(), name: findName(), source_url: TEST_MODE ? TEST_SOURCE_URL : location.href };
  }

  function shadowHost(id) {
    var host = document.createElement("div");
    host.id = id;
    var root = host.attachShadow ? host.attachShadow({ mode: "open" }) : host;
    return { host: host, root: root };
  }

  function buttonCss() {
    return "*{box-sizing:border-box}.button{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:34px;padding:7px 12px;border:1px solid #2576b9;border-radius:3px;background:#337ab7;color:#fff;font:600 13px/1.2 Arial,sans-serif;cursor:pointer;box-shadow:none}.button:hover{background:#286090;border-color:#204d74}.button:focus{outline:2px solid rgba(51,122,183,.3);outline-offset:2px}.mark{width:7px;height:7px;border-radius:50%;background:#67d391}";
  }

  function placeButton() {
    if (document.getElementById(BUTTON_ID)) return true;
    var pair = shadowHost(BUTTON_ID);
    pair.root.innerHTML = '<style>' + buttonCss() + '</style><button class="button" type="button"><span class="mark"></span>Написать через Wazzup</button>';
    pair.root.querySelector("button").addEventListener("click", openDrawer);
    var actions = Array.prototype.slice.call(document.querySelectorAll("button,a")).find(function (node) {
      return /написать пользователю/i.test(String(node.textContent || ""));
    });
    var phoneNode = document.querySelector(".user-call-to-phone,a[href^='tel:'],.user-phone");
    var target = actions || phoneNode || document.querySelector(".user-card,.gc-user-user-info,.standard-logo");
    if (!target || !target.parentNode) return false;
    target.insertAdjacentElement("afterend", pair.host);
    pair.host.style.display = "inline-block";
    pair.host.style.margin = "8px 0 4px 8px";
    pair.host.style.verticalAlign = "middle";
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
      if (!token) { window.alert("Сначала откройте любую карточку пользователя GetCourse и активируйте браузер одноразовым кодом."); return; }
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
      ".layer{position:fixed;inset:0;z-index:2147483600;pointer-events:none;font-family:Arial,sans-serif;color:#17212b}",
      ".backdrop{position:absolute;inset:0;background:rgba(10,16,24,.38);opacity:0;transition:opacity .16s;pointer-events:auto}",
      ".drawer{position:absolute;top:0;right:0;width:50vw;height:100dvh;background:#fff;border-left:1px solid #bac4ce;box-shadow:-18px 0 55px rgba(15,23,42,.2);display:grid;grid-template-rows:auto minmax(0,1fr);transform:translateX(100%);transition:transform .18s ease;pointer-events:auto}",
      ".layer.open .backdrop{opacity:1}.layer.open .drawer{transform:none}",
      ".head{min-height:56px;display:flex;align-items:center;gap:10px;padding:9px 12px;border-bottom:1px solid #d9e0e7;background:#f6f8fa}",
      ".title{min-width:0;flex:1}.title b{display:block;font-size:14px;line-height:1.25}.title span{display:block;margin-top:2px;color:#66788a;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
      ".copy,.close,.submit,.channel{height:32px;border:1px solid #b8c4ce;border-radius:3px;background:#fff;color:#263746;font:600 12px Arial,sans-serif;cursor:pointer}.copy{padding:0 10px}.close{width:32px;font-size:20px;line-height:1}.copy:hover,.close:hover,.channel:hover{background:#edf2f6}.channels{display:flex;gap:5px;min-width:0;overflow:auto}.channel{padding:0 8px;white-space:nowrap}.channel.active{border-color:#2576b9;background:#337ab7;color:#fff}",
      ".body{min-height:0;position:relative;background:#eef2f5}.frame{display:block;width:100%;height:100%;border:0;background:#fff}",
      ".state{position:absolute;inset:0;display:grid;place-items:center;padding:24px;background:#eef2f5;text-align:center}.state-card{width:min(360px,100%);color:#526475;font:13px/1.45 Arial,sans-serif}.state-card b{display:block;margin-bottom:6px;color:#17212b;font-size:15px}.state-card p{margin:0 0 14px}.channel-list{display:grid;gap:8px;margin-top:12px}.channel-list .submit{margin:0;text-align:left;padding:0 12px}",
      ".chat-shell{height:100%;display:grid;grid-template-rows:42px minmax(0,1fr) auto;background:#eef2f5}.chat-tools{display:flex;align-items:center;gap:7px;padding:5px 8px;border-bottom:1px solid #d4dde4;background:#fff}.tool{height:30px;padding:0 9px;border:1px solid #b8c4ce;border-radius:2px;background:#fff;color:#334b5c;font:600 12px Arial,sans-serif;cursor:pointer}.tool:hover{background:#edf2f6}.channel-name{min-width:0;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#526475;font-size:12px}.message-feed{min-height:0;overflow:auto;padding:14px;scrollbar-color:#8a9aa7 #e6ebef}.history-note,.empty-chat{margin:8px auto;padding:8px 10px;width:min(440px,100%);border:1px solid #cad4dc;background:#f7f9fa;color:#66788a;text-align:center;font-size:11px;line-height:1.4}.message-row{display:flex;margin:7px 0}.message-row.outgoing{justify-content:flex-end}.bubble{max-width:78%;padding:8px 10px;border:1px solid #d5dde3;border-radius:3px;background:#fff;color:#17212b;font:13px/1.45 Arial,sans-serif;white-space:pre-wrap;overflow-wrap:anywhere}.outgoing .bubble{border-color:#b8d9c1;background:#e6f5e9}.message-meta{display:flex;justify-content:flex-end;gap:6px;margin-top:4px;color:#81909b;font-size:10px}.attachment{display:block;margin-top:6px;color:#337ab7;overflow-wrap:anywhere}.composer{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;padding:9px;border-top:1px solid #ccd6de;background:#fff}.composer textarea{min-height:42px;max-height:130px;resize:vertical;padding:9px;border:1px solid #aab7c2;border-radius:2px;color:#17212b;font:13px/1.4 Arial,sans-serif;outline:none}.composer textarea:focus{border-color:#337ab7}.send{min-width:104px;border:1px solid #27743f;border-radius:2px;background:#3f9659;color:#fff;font:700 12px Arial,sans-serif;cursor:pointer}.send:disabled{opacity:.55;cursor:default}.compose-error{grid-column:1/-1;min-height:0;color:#a23a3a;font-size:11px}",
      ".code{width:100%;height:38px;padding:0 10px;border:1px solid #aab7c2;border-radius:3px;background:#fff;color:#17212b;text-align:center;text-transform:uppercase;font:600 15px/1 Arial,sans-serif;letter-spacing:.08em}.submit{width:100%;height:38px;margin-top:8px;border-color:#2576b9;background:#337ab7;color:#fff}.submit:hover{background:#286090}.submit:disabled{opacity:.55;cursor:default}",
      ".error{min-height:18px;margin-top:8px;color:#a23a3a;font-size:12px}.spinner{width:24px;height:24px;margin:0 auto 12px;border:2px solid #c9d3dc;border-top-color:#337ab7;border-radius:50%;animation:spin .75s linear infinite}",
      "@keyframes spin{to{transform:rotate(360deg)}}",
      "@media(max-width:900px){.drawer{width:100vw;border-left:0}.copy{max-width:150px}}",
      "@media(prefers-reduced-motion:reduce){.backdrop,.drawer{transition:none}.spinner{animation:none}}"
    ].join("");
  }

  var drawer = null;
  var lastFocus = null;
  var activeChannel = null;
  var conversationTimer = null;
  var conversationSignature = "";

  function stopConversationPoll() {
    if (conversationTimer) clearTimeout(conversationTimer);
    conversationTimer = null;
    activeChannel = null;
  }

  function ensureDrawer() {
    if (drawer) return drawer;
    var pair = shadowHost(DRAWER_ID);
    pair.root.innerHTML = '<style>' + drawerCss() + '</style><div class="layer" role="dialog" aria-modal="true" aria-label="Wazzup"><div class="backdrop"></div><section class="drawer"><header class="head"><div class="title"><b>Wazzup</b><span class="subtitle">Подготовка…</span></div><div class="channels" hidden></div><button class="copy" type="button">Скопировать номер</button><button class="close" type="button" aria-label="Закрыть">×</button></header><main class="body"><div class="state"><div class="state-card"><div class="spinner"></div><b>Открываем Wazzup</b><p>Получаем защищённую ссылку на окно чатов.</p></div></div></main></section></div>';
    document.body.appendChild(pair.host);
    var layer = pair.root.querySelector(".layer");
    pair.root.querySelector(".backdrop").addEventListener("click", closeDrawer);
    pair.root.querySelector(".close").addEventListener("click", closeDrawer);
    pair.root.querySelector(".copy").addEventListener("click", copyPhone);
    drawer = { host: pair.host, root: pair.root, layer: layer, body: pair.root.querySelector(".body"), subtitle: pair.root.querySelector(".subtitle"), channels: pair.root.querySelector(".channels"), copy: pair.root.querySelector(".copy") };
    return drawer;
  }

  function setState(title, text, extra) {
    var d = ensureDrawer();
    d.body.innerHTML = '<div class="state"><div class="state-card"><b></b><p></p>' + (extra || "") + '<div class="error"></div></div></div>';
    d.body.querySelector("b").textContent = title;
    d.body.querySelector("p").textContent = text;
    return d.body.querySelector(".state-card");
  }

  function activationForm(message) {
    var card = setState("Активация администратора", message || "Введите одноразовый код, созданный в модуле Nexus.", '<input class="code" autocomplete="one-time-code" inputmode="text" maxlength="16" placeholder="XXXX-XXXX-XXXX"><button class="submit" type="button">Активировать</button>');
    var input = card.querySelector(".code");
    var submit = card.querySelector(".submit");
    submit.addEventListener("click", function () { activate(input.value, submit, card.querySelector(".error")); });
    input.addEventListener("keydown", function (event) { if (event.key === "Enter") submit.click(); });
    setTimeout(function () { input.focus(); }, 30);
  }

  async function request(path, options) {
    var settings = Object.assign({ method: "POST", mode: TEST_MODE ? "same-origin" : "cors", credentials: TEST_MODE ? "same-origin" : "omit" }, options || {});
    settings.headers = Object.assign(
      { "Content-Type": "application/json" },
      TEST_MODE ? { "X-Nexus-Wazzup-Test": "1" } : {},
      (options && options.headers) || {}
    );
    var response = await fetch(API + path, settings);
    var data = await response.json().catch(function () { return {}; });
    if (!response.ok || data.ok === false) {
      var error = new Error(data.error || "HTTP " + response.status);
      error.reauth = !!data.reauth || response.status === 401;
      throw error;
    }
    return data;
  }

  async function activate(code, button, errorNode) {
    button.disabled = true;
    errorNode.textContent = "";
    try {
      var data = await request("/activate", { body: JSON.stringify({ code: String(code || "").trim() }) });
      localStorage.setItem(STORAGE_KEY, data.device_token);
      await showChannelMenu();
    } catch (error) {
      errorNode.textContent = error.message || "Не удалось активировать устройство";
      button.disabled = false;
    }
  }

  function renderChannels(channels, selected) {
    var d = ensureDrawer();
    d.channels.innerHTML = "";
    if (!Array.isArray(channels) || channels.length < 2) { d.channels.hidden = true; return; }
    channels.forEach(function (channel) {
      var button = document.createElement("button");
      button.className = "channel" + (channel.transport === selected ? " active" : "");
      button.type = "button";
      button.textContent = "Написать в " + String(channel.name || channel.transport).slice(0, 28);
      button.addEventListener("click", function () { openConversation(channel); });
      d.channels.appendChild(button);
    });
    d.channels.hidden = false;
  }

  async function showChannelMenu() {
    stopConversationPoll();
    var d = ensureDrawer();
    var ctx = context();
    d.subtitle.textContent = ctx.phone || "Телефон в карточке не найден";
    d.copy.disabled = !ctx.phone;
    var token = localStorage.getItem(STORAGE_KEY) || "";
    if (!token) {
      activationForm();
      return;
    }
    setState("Каналы Wazzup", "Выберите канал. Переписка откроется прямо в Nexus.", '<div class="spinner"></div>');
    try {
      var data = await request("/channels", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: "{}" });
      var channels = Array.isArray(data.channels) ? data.channels : [];
      if (!channels.length) throw new Error("В Wazzup нет активных каналов. Подключите канал и повторите попытку.");
      var card = setState("Каналы Wazzup", "Выберите канал для клиента " + (ctx.phone || "без номера") + ".");
      var list = document.createElement("div");
      list.className = "channel-list";
      channels.forEach(function (channel) {
        var button = document.createElement("button");
        button.className = "submit";
        button.type = "button";
        button.textContent = "Написать в " + String(channel.transport || channel.name).toUpperCase() + (channel.name ? " · " + String(channel.name).slice(0, 36) : "");
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

  function conversationPayload(channel) {
    return Object.assign({}, context(), { channel_id: channel.channel_id, transport: channel.transport });
  }

  function shortTime(value) {
    var date = new Date(value);
    return Number.isNaN(date.getTime()) ? "" : date.toLocaleString("ru-RU", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
  }

  function renderMessageFeed(feed, data) {
    var messages = Array.isArray(data.messages) ? data.messages : [];
    var signature = JSON.stringify(messages.map(function (item) { return [item.external_id, item.status, item.text, item.sent_at]; }));
    if (signature === conversationSignature) return;
    conversationSignature = signature;
    var pinned = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 80;
    feed.innerHTML = "";
    if (!data.history_complete) {
      var note = document.createElement("div");
      note.className = "history-note";
      if (data.history_status === "error") note.textContent = "Wazzup временно не отдал старую историю. Новые сообщения продолжат поступать через webhook; при следующем открытии Nexus попробует снова.";
      else if (data.history_status === "not_found") note.textContent = "Wazzup не нашёл диалог этого номера в выбранном канале. Если клиент напишет или вы начнёте диалог, он привяжется автоматически.";
      else note.textContent = "Проверяем существующую историю Wazzup. Новые сообщения и статусы появляются здесь автоматически.";
      feed.appendChild(note);
    }
    if (!messages.length) {
      var empty = document.createElement("div");
      empty.className = "empty-chat";
      empty.textContent = "Сообщений по этому каналу пока нет. Можно начать диалог ниже.";
      feed.appendChild(empty);
    }
    messages.forEach(function (message) {
      var row = document.createElement("div");
      row.className = "message-row " + (message.direction === "outgoing" ? "outgoing" : "incoming");
      var bubble = document.createElement("div");
      bubble.className = "bubble";
      var text = document.createElement("div");
      text.textContent = message.text || (message.content_uri ? "Вложение" : "Сообщение без текста");
      bubble.appendChild(text);
      if (message.content_uri && /^https:\/\//i.test(message.content_uri)) {
        var attachment = document.createElement("a");
        attachment.className = "attachment";
        attachment.href = message.content_uri;
        attachment.target = "_blank";
        attachment.rel = "noopener noreferrer";
        attachment.textContent = "Открыть вложение";
        bubble.appendChild(attachment);
      }
      var meta = document.createElement("div");
      meta.className = "message-meta";
      meta.textContent = [message.author_name || "", shortTime(message.sent_at), message.status || ""].filter(Boolean).join(" · ");
      bubble.appendChild(meta);
      row.appendChild(bubble);
      feed.appendChild(row);
    });
    if (pinned || !messages.length) feed.scrollTop = feed.scrollHeight;
  }

  async function fetchConversation(channel, feed, silent) {
    var token = localStorage.getItem(STORAGE_KEY) || "";
    try {
      var data = await request("/conversation", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify(conversationPayload(channel)) });
      renderMessageFeed(feed, data);
      return data;
    } catch (error) {
      if (!silent) throw error;
      return null;
    }
  }

  function scheduleConversationPoll(channel, feed) {
    if (conversationTimer) clearTimeout(conversationTimer);
    conversationTimer = setTimeout(async function poll() {
      if (!drawer || activeChannel !== channel) return;
      if (!document.hidden) await fetchConversation(channel, feed, true);
      scheduleConversationPoll(channel, feed);
    }, 5000);
  }

  async function openConversation(channel) {
    stopConversationPoll();
    activeChannel = channel;
    conversationSignature = "";
    var d = ensureDrawer();
    var ctx = context();
    d.subtitle.textContent = (ctx.phone || "Номер не найден") + " · " + String(channel.transport || "").toUpperCase();
    d.channels.hidden = true;
    setState("Загружаем переписку", "Получаем сохранённые сообщения и статусы.", '<div class="spinner"></div>');
    try {
      var initial = await fetchConversation(channel, document.createElement("div"), false);
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
      channelName.textContent = String(channel.transport || "").toUpperCase() + " · " + (channel.name || "Wazzup");
      var nativeButton = document.createElement("button");
      nativeButton.className = "tool";
      nativeButton.type = "button";
      nativeButton.textContent = "Открыть Wazzup";
      nativeButton.addEventListener("click", function () { loadFrame(channel); });
      tools.appendChild(back);
      tools.appendChild(channelName);
      tools.appendChild(nativeButton);
      var feed = document.createElement("div");
      feed.className = "message-feed";
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
      send.addEventListener("click", async function () {
        var text = input.value.trim();
        if (!text) return;
        send.disabled = true;
        errorNode.textContent = "";
        try {
          var token = localStorage.getItem(STORAGE_KEY) || "";
          await request("/send", { headers: { "Content-Type": "application/json", "Authorization": "Bearer " + token }, body: JSON.stringify(Object.assign({}, conversationPayload(channel), { text: text })) });
          input.value = "";
          conversationSignature = "";
          await fetchConversation(channel, feed, false);
        } catch (error) {
          errorNode.textContent = error.message || "Не удалось отправить сообщение";
        } finally { send.disabled = false; }
      });
      composer.appendChild(input);
      composer.appendChild(send);
      composer.appendChild(errorNode);
      shell.appendChild(tools);
      shell.appendChild(feed);
      shell.appendChild(composer);
      d.body.appendChild(shell);
      conversationSignature = "";
      renderMessageFeed(feed, initial);
      scheduleConversationPoll(channel, feed);
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
        activationForm("Срок доступа закончился или устройство отозвано. Введите новый код из Nexus.");
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
    lastFocus = event && event.currentTarget;
    var d = ensureDrawer();
    requestAnimationFrame(function () { d.layer.classList.add("open"); });
    document.documentElement.style.setProperty("--nexus-wazzup-open", "1");
    showChannelMenu();
  }

  function closeDrawer() {
    if (!drawer) return;
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
  }

  document.addEventListener("keydown", function (event) { if (event.key === "Escape" && drawer) closeDrawer(); });

  function boot() {
    if (STAFF_PAGE ? placeStaffButton() : placeButton()) return;
    var observer = new MutationObserver(function () {
      if (STAFF_PAGE ? placeStaffButton() : placeButton()) observer.disconnect();
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
    setTimeout(function () { observer.disconnect(); }, 30000);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot, { once: true });
  else boot();
})();
