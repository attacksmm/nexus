import json
import re

import aiosqlite
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from orchestrator.auth import can_access_module, verify_token_from_request

router = APIRouter()
_db_path = None
MODULE_ID = "salebot-senler-button"

DEFAULT_GROUP_ID = "225075265"
DEFAULT_BUTTON_ID = "senlerBtn-1779702884"
DEFAULT_TARGET = ".salebot_tilda_block"
DEFAULT_REDIRECT_URL = "https://vk.com/app6013442_-225075265?form_id=1#form_id=1"


async def _require_panel_user(request: Request) -> dict:
    user = await verify_token_from_request(request)
    if not user or not can_access_module(user, MODULE_ID):
        raise HTTPException(401, "unauthorized")
    return user


def setup(ctx):
    global _db_path
    _db_path = ctx.db_path
    import asyncio
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_init_db())
    else:
        loop.run_until_complete(_init_db())


async def _init_db():
    async with aiosqlite.connect(_db_path) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS presets (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL,
                button_id       TEXT NOT NULL UNIQUE,
                group_id        TEXT NOT NULL,
                subscription_id TEXT NOT NULL DEFAULT '',
                button_text     TEXT NOT NULL DEFAULT 'Записаться в ВКонтакте',
                target_selector TEXT NOT NULL DEFAULT '.salebot_tilda_block',
                redirect_url    TEXT NOT NULL DEFAULT 'https://vk.com/app6013442_-225075265?form_id=1#form_id=1',
                note            TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
            );
        """)
        cur = await db.execute("PRAGMA table_info(presets)")
        columns = {row[1] for row in await cur.fetchall()}
        if "redirect_url" not in columns:
            await db.execute("ALTER TABLE presets ADD COLUMN redirect_url TEXT NOT NULL DEFAULT 'https://vk.com/app6013442_-225075265?form_id=1#form_id=1'")
        await _ensure_default_preset(
            db,
            name="Подзыв. Основная группа подписки",
            button_id="senlerBtn-recall",
            group_id="225075265",
            subscription_id="3705308",
            note="Senler список: Подзыв",
        )
        await _ensure_default_preset(
            db,
            name="Укусы. Основная группа подписки",
            button_id="senlerBtn-bites",
            group_id="225075265",
            subscription_id="3705307",
            note="Senler список: Укусы",
        )
        await _ensure_default_preset(
            db,
            name="Щенок. Основная группа подписки",
            button_id="senlerBtn-puppy",
            group_id="225075265",
            subscription_id="3700634",
            note="Senler список: Щенок",
        )
        await _ensure_default_preset(
            db,
            name="Собака. Основная группа подписки",
            button_id="senlerBtn-dog",
            group_id="225075265",
            subscription_id="3701247",
            note="Senler список: Собака",
        )
        await _ensure_default_preset(
            db,
            name="Нельзя. Основная группа подписки",
            button_id="senlerBtn-poslushanie",
            group_id="225075265",
            subscription_id="3729954",
            redirect_url="https://vk.com/app5898182_-225075265#s=3729954",
            note="Senler список: Нельзя / послушание",
        )
        await db.commit()


async def _ensure_default_preset(db, **preset):
    cur = await db.execute("SELECT id FROM presets WHERE button_id=?", (preset["button_id"],))
    if await cur.fetchone():
        return
    await db.execute(
        """
        INSERT INTO presets(name, button_id, group_id, subscription_id, button_text, target_selector, redirect_url, note)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            preset["name"],
            preset["button_id"],
            preset["group_id"],
            preset["subscription_id"],
            "Записаться в ВКонтакте",
            DEFAULT_TARGET,
            preset.get("redirect_url", DEFAULT_REDIRECT_URL),
            preset.get("note", ""),
        ),
    )


def _js(value: str) -> str:
    return json.dumps(value or "", ensure_ascii=False)


def _bool_param(request: Request, key: str, default: bool) -> bool:
    raw = str(request.query_params.get(key, "")).strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _numeric(value: str, default: str = "") -> str:
    value = str(value or "").strip()
    return value if re.fullmatch(r"\d{1,24}", value) else default


def _identifier(value: str, default: str) -> str:
    value = str(value or "").strip()
    return value if re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,100}", value) else default


def _clean_text(value: str, default: str = "") -> str:
    value = str(value or "").strip()
    return value[:500] if value else default


def _clean_url(value: str, default: str = "") -> str:
    value = str(value or "").strip()
    if not value:
        return default
    if not re.match(r"^https://(vk\.com|vk\.ru)/", value):
        return default
    return value[:1000]


def _direct_href(value: str) -> str:
    value = str(value or "").strip()
    if re.match(r"^https://(vk\.com|vk\.ru)/", value):
        return value[:1000].replace("https://vk.com/app", "https://vk.ru/app")
    return "https://vk.ru/app5898182_-225075265#s=3700634"


def _direct_ids_from_href(href: str) -> tuple[str, str]:
    group_id = DEFAULT_GROUP_ID
    subscription_id = ""
    match = re.search(r"app\d+_-(\d+)", href or "")
    if match:
        group_id = match.group(1)
    match = re.search(r"(?:[?#&]|#)s=(\d+)", href or "")
    if match:
        subscription_id = match.group(1)
    return group_id, subscription_id


def _snippet(*, base_url: str, button_id: str, group_id: str, subscription_id: str, button_text: str, target_selector: str, redirect_url: str) -> str:
    cfg = json.dumps(
        {
            "buttonId": button_id,
            "groupId": group_id,
            "subscriptionId": subscription_id,
            "text": button_text or "Записаться в ВКонтакте",
            "targetSelector": target_selector or DEFAULT_TARGET,
            "redirectUrl": redirect_url or DEFAULT_REDIRECT_URL,
        },
        ensure_ascii=False,
    )
    return (
        '<style data-nexus-salebot-senler-early>'
        '.salebot_tilda_block .vk_link,.form_integration_block .vk_link{visibility:hidden!important}'
        f'#{button_id}{{position:absolute!important;left:-10000px!important;top:auto!important;width:1px!important;height:1px!important;overflow:hidden!important;opacity:0!important;pointer-events:none!important}}'
        '</style>\n'
        '<script src="https://senler.ru/dist/web/js/senler.js?9"></script>\n'
        '<script>\n'
        f'  try {{ Senler.ButtonSubscribe("{button_id}"); }} catch (e) {{ console.log(e); }}\n'
        '</script>\n'
        f'<div id="{button_id}" data-vk_group_id="{group_id}" data-subscription_id="{subscription_id}" '
        f'data-text="{button_text}" data-alt_text=""></div>\n'
        '<script>\n'
        f'(function(){{"use strict";var cfg={cfg};'
        'var source=document.getElementById(cfg.buttonId);'
        'function css(){if(document.getElementById("nexus-salebot-senler-inline-style"))return;var s=document.createElement("style");s.id="nexus-salebot-senler-inline-style";s.textContent=".nexus-salebot-senler-source{position:absolute!important;left:-10000px!important;top:auto!important;width:1px!important;height:1px!important;overflow:hidden!important;opacity:0!important;pointer-events:none!important}.nexus-salebot-senler-pending-vk{visibility:hidden!important}.nexus-salebot-senler-btn.senler-btn,.nexus-salebot-senler-btn.senler-btn-alt{display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important;flex:0 1 auto!important;width:var(--nexus-senler-width,auto)!important;max-width:100%!important;height:auto!important;min-height:var(--nexus-senler-height,44px)!important;padding:10px 18px!important;box-sizing:border-box!important;border-radius:999px!important;border:0!important;background:#4b86c9!important;color:#fff!important;font-family:inherit!important;font-size:inherit!important;line-height:1.15!important;font-weight:700!important;text-align:center!important;text-decoration:none!important;cursor:pointer!important;box-shadow:none!important;white-space:normal!important;overflow-wrap:anywhere!important;word-break:normal!important;letter-spacing:0!important;margin:0 auto!important}.nexus-salebot-senler-btn.senler-btn:before,.nexus-salebot-senler-btn.senler-btn-alt:before{content:\\"VK\\";display:inline-flex!important;align-items:center!important;justify-content:center!important;font-size:.9em!important;font-weight:900!important;line-height:1!important;color:#fff!important;font-family:Arial,Helvetica,sans-serif!important;letter-spacing:0!important}";(document.head||document.documentElement).appendChild(s)}'
        'function hideSource(){source=document.getElementById(cfg.buttonId);if(source)source.classList.add("nexus-salebot-senler-source")}'
        'function load(cb){if(window.Senler&&typeof Senler.ButtonSubscribe==="function")return cb();var old=document.querySelector("script[src*=\'senler.ru/dist/web/js/senler.js\']");if(old){old.addEventListener("load",cb,{once:true});return setTimeout(cb,700)}var s=document.createElement("script");s.src="https://senler.ru/dist/web/js/senler.js?9";s.async=true;s.onload=cb;(document.head||document.documentElement).appendChild(s)}'
        'function isVk(el){var t=((el.innerText||el.textContent||"")+" "+(el.className||"")+" "+(el.href||el.getAttribute("href")||"")).toLowerCase();return t.indexOf("вконтакте")>-1||t.indexOf(" vk")>-1||t.indexOf("вк")>-1||t.indexOf("vk.com")>-1||t.indexOf("vk.ru")>-1}'
        'function roots(){var list=[].slice.call(document.querySelectorAll(cfg.targetSelector));return list.length?list:[document.body]}'
        'function params(){var out=[];function add(raw){String(raw||"").replace(/^[?#]/,"").split("&").forEach(function(p){var k=p.split("=")[0];if(!k||k==="s")return;if(!out.some(function(x){return x.split("=")[0]===k}))out.push(p)})}function cv(n){var m=("; "+document.cookie).match("; "+n+"=([^;]*)");return m?decodeURIComponent(m[1]):""}function ap(k,v){if(v)add(k+"="+encodeURIComponent(v))}add(location.search);add(location.hash);ap("yclid",cv("yclid"));ap("_ym_uid",cv("_ym_uid"));return out}'
        'function hasParam(u,k){return new RegExp("([?#&])"+k+"=").test(u)}'
        'function addParams(url){var ps=params();if(!ps.length||!/vk\\.(com|ru)\\/app\\d+_/i.test(url||""))return url;var h="",i=url.indexOf("#");if(i>=0){h=url.slice(i);url=url.slice(0,i)}ps.forEach(function(p){var k=p.split("=")[0];if(!hasParam(url+h,k))url+=(url.indexOf("?")<0?"?":"&")+p});return url+h}'
        'function appUrl(n){var u=cfg.redirectUrl||"",g=(n&&n.getAttribute("data-vk_group_id"))||cfg.groupId||(source&&source.getAttribute("data-vk_group_id"))||"",sub=(n&&n.getAttribute("data-subscription_id"))||cfg.subscriptionId||(source&&source.getAttribute("data-subscription_id"))||"";if(!u&&g)u="https://vk.com/app6013442_-"+g+"?form_id=1#form_id=1";return addParams(u)}'
        'function bindRedirect(n){if(!n||n.getAttribute("data-nexus-salebot-senler-redirect-bound"))return;n.setAttribute("data-nexus-salebot-senler-redirect-bound","1");n.addEventListener("click",function(){var u=appUrl(n),w=null;if(!u)return;try{w=window.open(u,"_blank")}catch(e){}if(!w)location.href=u},false)}'
        'if(!window.__nexusSenlerOpenPatched){window.__nexusSenlerOpenPatched=true;var open=window.open;if(typeof open==="function")window.open=function(u,t,f){if(typeof u==="string")u=addParams(u);return open.call(window,u,t,f)}}'
        'function tg(a){var q=".tg_link,a[href*=\\"t.me\\"],a[href*=\\"telegram\\"],button[class*=\\"tg\\"],a[class*=\\"tg\\"],button[class*=\\"telegram\\"]",r=a&&a.closest&&a.closest(cfg.targetSelector),t=r&&r.querySelector(q),p=a&&a.parentElement,i=0;while(!t&&p&&i++<4){t=p.querySelector(q);p=p.parentElement}return t&&t!==a?t:null}'
        'function fit(n,a){a=tg(a)||a;if(!n||!a)return;var r=a.getBoundingClientRect(),w=a.offsetWidth||r.width,h=a.offsetHeight||r.height,cs=getComputedStyle(a);if(w>80){n.style.setProperty("--nexus-senler-width",Math.round(w)+"px");n.style.setProperty("width",Math.round(w)+"px","important")}if(h>30)n.style.setProperty("--nexus-senler-height",Math.round(h)+"px");if(cs.fontSize)n.style.fontSize=cs.fontSize;var p=a.parentElement;if(p){p.style.setProperty("text-align","center","important");if((getComputedStyle(p).display||"").indexOf("flex")!==-1){p.style.setProperty("flex-direction","column","important");p.style.setProperty("align-items","center","important");p.style.setProperty("justify-content","center","important");if(!getComputedStyle(p).rowGap||getComputedStyle(p).rowGap==="0px"||getComputedStyle(p).rowGap==="normal")p.style.setProperty("row-gap","12px","important")}}}'
        'function mount(vk,i){if(vk.getAttribute("data-nexus-salebot-senler-bound"))return;vk.setAttribute("data-nexus-salebot-senler-bound","1");vk.classList.add("nexus-salebot-senler-pending-vk");var id=cfg.buttonId+"-"+(i+1),n=document.getElementById(id)||document.createElement("div");n.id=id;n.classList.add("nexus-salebot-senler-btn");n.setAttribute("data-nexus-salebot-senler","1");n.setAttribute("data-vk_group_id",cfg.groupId||(source&&source.getAttribute("data-vk_group_id"))||"");n.setAttribute("data-subscription_id",cfg.subscriptionId||(source&&source.getAttribute("data-subscription_id"))||"");n.setAttribute("data-text",cfg.text);n.setAttribute("data-alt_text","");vk.parentNode.insertBefore(n,vk.nextSibling);fit(n,vk);load(function(){try{if(!n.classList.contains("senler-btn"))Senler.ButtonSubscribe(id)}catch(e){console.log(e)}n.classList.add("nexus-salebot-senler-btn");if(n.textContent.trim()!==cfg.text)n.textContent=cfg.text;fit(n,vk);bindRedirect(n);[250,800,2000,5000].forEach(function(d){setTimeout(function(){fit(n,vk)},d)});var a=tg(vk);if(a&&window.ResizeObserver)new ResizeObserver(function(){fit(n,vk)}).observe(a);setTimeout(function(){vk.style.display="none";vk.classList.remove("nexus-salebot-senler-pending-vk")},250)})}'
        'var count=0;function scan(){css();hideSource();roots().forEach(function(r){[].slice.call(r.querySelectorAll("a,button,[role=button],input[type=button],input[type=submit]")).forEach(function(el){var vk=el.closest("a,button,[role=button]")||el;if(isVk(vk))mount(vk,count++)})})}'
        'if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",scan);else scan();var mo=new MutationObserver(scan);mo.observe(document.documentElement,{childList:true,subtree:true});setTimeout(function(){mo.disconnect()},30000)'
        '})();\n'
        '</script>'
    )


@router.get("/health")
async def health():
    return {"ok": True, "module": "salebot-senler-button"}


@router.get("/presets")
async def list_presets(request: Request):
    await _require_panel_user(request)
    base_url = str(request.base_url).rstrip("/")
    async with aiosqlite.connect(_db_path) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM presets ORDER BY id")
        rows = [dict(row) for row in await cur.fetchall()]
    for row in rows:
        row["snippet"] = _snippet(
            base_url=base_url,
            button_id=row["button_id"],
            group_id=row["group_id"],
            subscription_id=row["subscription_id"],
            button_text=row["button_text"],
            target_selector=row["target_selector"],
            redirect_url=row.get("redirect_url") or DEFAULT_REDIRECT_URL,
        )
    return rows


@router.post("/presets")
async def save_preset(request: Request):
    await _require_panel_user(request)
    data = await request.json()
    preset_id = int(data.get("id") or 0)
    name = _clean_text(data.get("name"), "Новая кнопка")
    button_id = _identifier(data.get("button_id"), "")
    group_id = _numeric(data.get("group_id"), "")
    subscription_id = _numeric(data.get("subscription_id"), "")
    button_text = _clean_text(data.get("button_text"), "Записаться в ВКонтакте")
    target_selector = _clean_text(data.get("target_selector"), DEFAULT_TARGET)
    redirect_url = _clean_url(data.get("redirect_url"), DEFAULT_REDIRECT_URL)
    note = _clean_text(data.get("note"), "")
    if not button_id or not group_id:
        return JSONResponse({"error": "button_id и group_id обязательны"}, status_code=400)
    async with aiosqlite.connect(_db_path) as db:
        try:
            if preset_id:
                await db.execute(
                    """
                    UPDATE presets
                    SET name=?, button_id=?, group_id=?, subscription_id=?, button_text=?,
                        target_selector=?, redirect_url=?, note=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now')
                    WHERE id=?
                    """,
                    (name, button_id, group_id, subscription_id, button_text, target_selector, redirect_url, note, preset_id),
                )
                next_id = preset_id
            else:
                cur = await db.execute(
                    """
                    INSERT INTO presets(name, button_id, group_id, subscription_id, button_text, target_selector, redirect_url, note)
                    VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (name, button_id, group_id, subscription_id, button_text, target_selector, redirect_url, note),
                )
                next_id = cur.lastrowid
            await db.commit()
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
    return {"ok": True, "id": next_id}


@router.delete("/presets/{preset_id}")
async def delete_preset(preset_id: int, request: Request):
    await _require_panel_user(request)
    async with aiosqlite.connect(_db_path) as db:
        await db.execute("DELETE FROM presets WHERE id=?", (preset_id,))
        await db.commit()
    return {"ok": True}


@router.get("/direct.js")
async def direct_js(request: Request):
    href = _direct_href(request.query_params.get("href"))
    text = _clean_text(request.query_params.get("text"), "Записаться в ВКонтакте")
    base_id = _identifier(request.query_params.get("id"), "nexusVkDirectBtn")
    target = "_self"
    parsed_group_id, parsed_subscription_id = _direct_ids_from_href(href)
    group_id = _numeric(request.query_params.get("group_id"), parsed_group_id)
    subscription_id = _numeric(request.query_params.get("subscription_id"), parsed_subscription_id)
    script = f"""
(function(){{
  'use strict';
  var cfg={{baseId:{_js(base_id)},href:{_js(href)},text:{_js(text)},target:{_js(target)},groupId:{_js(group_id)},subscriptionId:{_js(subscription_id)},rootSelector:'.salebot_tilda_block,.form_integration_block'}};
  var mounted=0;
  function ensureStyle(){{
    if(document.getElementById('nexus-vk-direct-style')) return;
    var s=document.createElement('style');
    s.id='nexus-vk-direct-style';
    s.textContent='.nexus-vk-direct-btn{{display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important;flex:0 1 auto!important;width:var(--nexus-vk-direct-width,100%)!important;max-width:100%!important;height:auto!important;min-height:var(--nexus-vk-direct-height,44px)!important;padding:10px 18px!important;box-sizing:border-box!important;border-radius:999px!important;background:#4b86c9!important;color:#fff!important;font-family:inherit!important;font-size:inherit!important;line-height:1.15!important;font-weight:700!important;text-align:center!important;text-decoration:none!important;margin:0 auto!important;white-space:normal!important;overflow-wrap:anywhere!important;word-break:normal!important;letter-spacing:0!important}}.nexus-vk-direct-btn:before{{content:"VK";font:900 .9em/1 Arial,Helvetica,sans-serif;color:#fff;letter-spacing:0!important}}.nexus-vk-direct-senler-fallback.senler-btn,.nexus-vk-direct-senler-fallback.senler-btn-alt{{display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important;flex:0 1 auto!important;width:var(--nexus-vk-direct-width,100%)!important;max-width:100%!important;height:auto!important;min-height:var(--nexus-vk-direct-height,44px)!important;padding:10px 18px!important;box-sizing:border-box!important;border-radius:999px!important;background:#4b86c9!important;color:#fff!important;font-family:inherit!important;font-size:inherit!important;line-height:1.15!important;font-weight:700!important;text-align:center!important;text-decoration:none!important;margin:0 auto!important;white-space:normal!important;overflow-wrap:anywhere!important;word-break:normal!important;letter-spacing:0!important}}.nexus-vk-direct-pending-vk{{visibility:hidden!important}}@media(max-width:640px){{.nexus-vk-direct-btn,.nexus-vk-direct-senler-fallback.senler-btn,.nexus-vk-direct-senler-fallback.senler-btn-alt{{min-height:42px!important;padding:9px 14px!important}}}}';
    (document.head||document.documentElement).appendChild(s);
  }}
  function collect(){{
    var out=[];
    function add(raw){{
      String(raw||'').replace(/^[?#]/,'').split('&').forEach(function(p){{
        var k=(p.split('=')[0]||'').toLowerCase();
        if(!k) return;
        if(out.some(function(x){{return (x.split('=')[0]||'').toLowerCase()===k;}})) return;
        out.push(p);
      }});
    }}
    function cookieValue(name){{
      var m=('; '+document.cookie).match('; '+name+'=([^;]*)');
      return m ? decodeURIComponent(m[1]) : '';
    }}
    function addPair(key,value){{
      if(!value) return;
      add(key+'='+encodeURIComponent(value));
    }}
    add(location.search); add(location.hash);
    addPair('yclid', cookieValue('yclid'));
    addPair('_ym_uid', cookieValue('_ym_uid'));
    return out;
  }}
  function hasParam(url,key){{
    try {{ return new RegExp('([?#&])'+key+'=').test(url); }}
    catch(e){{ return url.indexOf(key+'=')!==-1; }}
  }}
  function withParams(url){{
    var hash='',i=url.indexOf('#');
    if(i>=0){{ hash=url.slice(i+1); url=url.slice(0,i); }}
    collect().forEach(function(p){{
      var key=(p.split('=')[0]||'').toLowerCase();
      if(!key || hasParam(url+'#'+hash,key)) return;
      if(hash) hash += '&' + p;
      else url += (url.indexOf('?')<0?'?':'&') + p;
    }});
    return url + (hash ? '#' + hash : '');
  }}
  function isVk(el){{
    if(!el || (el.classList && el.classList.contains('nexus-vk-direct-btn'))) return false;
    if(el.getAttribute && el.getAttribute('data-nexus-vk-direct-created')) return false;
    var t=((el.innerText||el.textContent||'')+' '+(el.className||'')+' '+(el.href||el.getAttribute('href')||'')).toLowerCase();
    return t.indexOf('вконтакте')>-1||t.indexOf(' vk')>-1||t.indexOf('вк')>-1||t.indexOf('vk.com')>-1||t.indexOf('vk.ru')>-1;
  }}
  function findTgAnchor(vk){{
    if(vk && vk.matches && vk.matches(cfg.rootSelector)){{
      return vk.querySelector('.tg_link,a[href*="t.me"],a[href*="telegram"],button[class*="tg"],a[class*="tg"],button[class*="telegram"]');
    }}
    var root=vk && vk.closest && vk.closest(cfg.rootSelector);
    if(root){{
      var rootTg=root.querySelector('.tg_link,a[href*="t.me"],a[href*="telegram"],button[class*="tg"],a[class*="tg"],button[class*="telegram"]');
      if(rootTg && rootTg!==vk) return rootTg;
    }}
    var p=vk && vk.parentElement;
    for(var i=0;p&&i<4;i++,p=p.parentElement){{
      var tg=p.querySelector('.tg_link,a[href*="t.me"],a[href*="telegram"],button[class*="tg"],a[class*="tg"],button[class*="telegram"]');
      if(tg && tg!==vk) return tg;
    }}
    return null;
  }}
  function fit(link,vk){{
    var anchor=findTgAnchor(vk)||vk;
    var r=anchor.getBoundingClientRect(),w=anchor.offsetWidth||r.width,h=anchor.offsetHeight||r.height,cs=getComputedStyle(anchor);
    if(w>80){{
      link.style.setProperty('--nexus-vk-direct-width',Math.round(w)+'px');
      link.style.setProperty('width',Math.round(w)+'px','important');
    }}
    if(h>30){{
      link.style.setProperty('--nexus-vk-direct-height',Math.round(h)+'px');
      link.style.setProperty('min-height',Math.round(h)+'px','important');
    }}
    if(cs.fontSize) link.style.fontSize=cs.fontSize;
    var p=anchor.parentElement;
    if(p) p.style.setProperty('text-align','center','important');
    if(p && (getComputedStyle(p).display||'').indexOf('flex')!==-1){{
      p.style.setProperty('flex-direction','column','important');
      p.style.setProperty('align-items','center','important');
      p.style.setProperty('justify-content','center','important');
      if(!getComputedStyle(p).rowGap || getComputedStyle(p).rowGap==='0px' || getComputedStyle(p).rowGap==='normal') p.style.setProperty('row-gap','12px','important');
    }}
  }}
  function refitMounted(){{
    [].slice.call(document.querySelectorAll(cfg.rootSelector)).forEach(function(root){{
      var anchor=findTgAnchor(root);
      if(!anchor) return;
      [].slice.call(root.querySelectorAll('.nexus-vk-direct-btn,.nexus-vk-direct-senler-fallback')).forEach(function(btn){{
        fit(btn,anchor);
      }});
      if(window.ResizeObserver&&!root.__nexusVkDirectResizeObserver){{
        root.__nexusVkDirectResizeObserver=new ResizeObserver(function(){{
          [].slice.call(root.querySelectorAll('.nexus-vk-direct-btn,.nexus-vk-direct-senler-fallback')).forEach(function(btn){{fit(btn,anchor);}});
        }});
        root.__nexusVkDirectResizeObserver.observe(anchor);
      }}
    }});
  }}
  function loadSenler(cb){{
    if(window.Senler&&typeof window.Senler.ButtonSubscribe==='function') return cb();
    var old=document.querySelector('script[src*="senler.ru/dist/web/js/senler.js"]');
    if(old){{ old.addEventListener('load',cb,{{once:true}}); return; }}
    var s=document.createElement('script');
    s.src='https://senler.ru/dist/web/js/senler.js?9';
    s.async=true;
    s.onload=cb;
    (document.head||document.documentElement).appendChild(s);
  }}
  function mount(vk){{
    if(!vk || vk.getAttribute('data-nexus-vk-direct-bound')) return;
    vk.setAttribute('data-nexus-vk-direct-bound','1');
    vk.classList.add('nexus-vk-direct-pending-vk');
    var link=document.createElement('a');
    link.id=cfg.baseId+'-'+(++mounted);
    link.className='nexus-vk-direct-btn';
    link.setAttribute('data-nexus-vk-direct-created','1');
    link.setAttribute('data-nexus-vk-direct-bound','1');
    link.href=withParams(cfg.href);
    link.target=cfg.target;
    link.rel='noopener';
    link.textContent=cfg.text;
    link.addEventListener('click',function(){{link.href=withParams(cfg.href);}},true);
    vk.parentNode.insertBefore(link,vk.nextSibling);
    fit(link,vk);
    vk.style.display='none';
    vk.classList.remove('nexus-vk-direct-pending-vk');
  }}
  function mountFallback(root){{
    if(!root || root.getAttribute('data-nexus-vk-direct-fallback')) return;
    root.setAttribute('data-nexus-vk-direct-fallback','1');
    root.style.setProperty('display','flex','important');
    root.style.setProperty('flex-direction','column','important');
    root.style.setProperty('align-items','center','important');
    root.style.setProperty('justify-content','center','important');
    root.style.setProperty('row-gap','12px','important');
    root.style.setProperty('text-align','center','important');
    var tg=findTgAnchor(root);
    var node=document.createElement('div');
    node.id=cfg.baseId+'-senler-fallback-'+(++mounted);
    node.className='nexus-vk-direct-senler-fallback';
    node.setAttribute('data-vk_group_id',cfg.groupId);
    node.setAttribute('data-subscription_id',cfg.subscriptionId);
    node.setAttribute('data-text',cfg.text);
    node.setAttribute('data-alt_text','');
    if(tg && tg.parentNode) tg.parentNode.insertBefore(node,tg);
    else root.appendChild(node);
    if(tg) fit(node,tg);
    loadSenler(function(){{
      try{{ window.Senler.ButtonSubscribe(node.id); }}catch(e){{ try{{console.log(e);}}catch(_){{}} }}
      setTimeout(function(){{
        var btn=node.matches('.senler-btn,.senler-btn-alt')?node:(node.querySelector&&node.querySelector('.senler-btn,.senler-btn-alt'))||node;
        if(tg) fit(btn,tg);
      }},250);
    }});
  }}
  function scan(){{
    ensureStyle();
    var roots=[].slice.call(document.querySelectorAll(cfg.rootSelector));
    if(!roots.length) roots=[document.body];
    roots.forEach(function(root){{
      [].slice.call(root.querySelectorAll('a,button,[role=button],input[type=button],input[type=submit]')).forEach(function(el){{
        var vk=el.closest('a,button,[role=button]')||el;
        if(isVk(vk)) mount(vk);
      }});
    }});
    refitMounted();
  }}
  function fallbackScan(){{
    var roots=[].slice.call(document.querySelectorAll(cfg.rootSelector));
    roots.forEach(function(root){{
      if(root.querySelector('.nexus-vk-direct-btn')) return;
      if(root.querySelector('.nexus-vk-direct-senler-fallback,.senler-btn,.senler-btn-alt')) return;
      var controls=[].slice.call(root.querySelectorAll('a,button,[role=button],input[type=button],input[type=submit]'));
      if(controls.some(function(el){{return isVk(el.closest('a,button,[role=button]')||el);}})) return;
      if(controls.length && !findTgAnchor(root)) return;
      mountFallback(root);
    }});
  }}
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',scan); else scan();
  var mo=new MutationObserver(scan);
  mo.observe(document.documentElement,{{childList:true,subtree:true}});
  setTimeout(fallbackScan,2500);
  setTimeout(fallbackScan,6000);
  setTimeout(refitMounted,800);
  setTimeout(refitMounted,2000);
  setTimeout(refitMounted,5000);
  window.addEventListener('load',function(){{ setTimeout(refitMounted,300); setTimeout(refitMounted,1500); }});
  window.addEventListener('resize',refitMounted);
  setTimeout(function(){{mo.disconnect();}},30000);
}})();
""".lstrip()
    return PlainTextResponse(
        script,
        headers={"Access-Control-Allow-Origin": "*"},
        media_type="application/javascript; charset=utf-8",
    )


@router.get("/replace.js")
async def replace_js(request: Request):
    group_id = _numeric(str(request.query_params.get("group_id") or DEFAULT_GROUP_ID), DEFAULT_GROUP_ID)
    subscription_id = _numeric(str(request.query_params.get("subscription_id") or ""), "")
    button_id = _identifier(str(request.query_params.get("button_id") or DEFAULT_BUTTON_ID), DEFAULT_BUTTON_ID)
    target = str(request.query_params.get("target") or DEFAULT_TARGET).strip() or DEFAULT_TARGET
    text = str(request.query_params.get("text") or "Записаться в ВКонтакте").strip()
    alt_text = str(request.query_params.get("alt_text") or "").strip()
    redirect_url = _clean_url(request.query_params.get("redirect_url"), DEFAULT_REDIRECT_URL)
    hide_vk = _bool_param(request, "hide_vk", True)

    script = f"""
(function() {{
  'use strict';
  var cfg = {{
    groupId: {_js(group_id)},
    subscriptionId: {_js(subscription_id)},
    buttonId: {_js(button_id)},
    targetSelector: {_js(target)},
    text: {_js(text)},
    altText: {_js(alt_text)},
    redirectUrl: {_js(redirect_url)},
    hideVk: {str(hide_vk).lower()}
  }};
  window.__nexusSalebotSenlerButtonInstalled = window.__nexusSalebotSenlerButtonInstalled || {{}};
  if (window.__nexusSalebotSenlerButtonInstalled[cfg.buttonId]) return;
  window.__nexusSalebotSenlerButtonInstalled[cfg.buttonId] = true;
  var mountedCount = 0;

  function log() {{
    try {{ console.log.apply(console, ['[SalebotSenler]'].concat([].slice.call(arguments))); }} catch (e) {{}}
  }}

  function ensureStyles() {{
    if (document.getElementById('nexus-salebot-senler-style')) return;
    var style = document.createElement('style');
    style.id = 'nexus-salebot-senler-style';
    style.textContent = [
      '.nexus-salebot-senler-source{{position:absolute!important;left:-10000px!important;top:auto!important;width:1px!important;height:1px!important;overflow:hidden!important;opacity:0!important;pointer-events:none!important;}}',
      '.nexus-salebot-senler-pending-vk{{visibility:hidden!important;}}',
      '.nexus-salebot-senler-btn.senler-btn,.nexus-salebot-senler-btn.senler-btn-alt{{',
      'display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important;',
      'flex:0 1 auto!important;width:var(--nexus-senler-width,auto)!important;max-width:100%!important;',
      'height:auto!important;min-height:var(--nexus-senler-height,44px)!important;padding:10px 18px!important;',
      'box-sizing:border-box!important;border-radius:999px!important;border:0!important;',
      'background:#4b86c9!important;color:#fff!important;font-family:inherit!important;',
      'font-size:inherit!important;line-height:1.15!important;font-weight:700!important;',
      'text-align:center!important;text-decoration:none!important;cursor:pointer!important;',
      'box-shadow:none!important;white-space:normal!important;overflow-wrap:anywhere!important;',
      'word-break:normal!important;letter-spacing:0!important;margin:0 auto!important;',
      '}}',
      '.nexus-salebot-senler-btn.senler-btn::before,.nexus-salebot-senler-btn.senler-btn-alt::before{{',
      'content:"VK";display:inline-flex!important;align-items:center!important;justify-content:center!important;',
      'font-size:.9em!important;font-weight:900!important;line-height:1!important;color:#fff!important;',
      'font-family:Arial,Helvetica,sans-serif!important;letter-spacing:0!important;',
      '}}',
      '@media (max-width:640px){{.nexus-salebot-senler-btn.senler-btn,.nexus-salebot-senler-btn.senler-btn-alt{{min-height:42px!important;padding:9px 14px!important;}}}}'
    ].join('');
    (document.head || document.documentElement).appendChild(style);
  }}

  function fitToAnchor(node, anchor) {{
    if (!node || !anchor) return;
    var scope = anchor.closest && anchor.closest(cfg.targetSelector);
    var telegram = (scope && findFallbackAnchor(scope)) || findFallbackAnchor(anchor.parentElement || document);
    if (telegram && telegram !== node) anchor = telegram;
    var rect = anchor.getBoundingClientRect();
    var width = anchor.offsetWidth || rect.width;
    var height = anchor.offsetHeight || rect.height;
    if (width > 80) {{
      node.style.setProperty('--nexus-senler-width', Math.round(width) + 'px');
      node.style.setProperty('width', Math.round(width) + 'px', 'important');
    }}
    if (height > 30) node.style.setProperty('--nexus-senler-height', Math.round(height) + 'px');
    var csAnchor = getComputedStyle(anchor);
    if (csAnchor.fontSize) node.style.fontSize = csAnchor.fontSize;
    var parent = anchor.parentElement;
    if (parent) {{
      var cs = getComputedStyle(parent);
      if ((cs.display || '').indexOf('flex') !== -1) {{
        parent.style.flexDirection = 'column';
        parent.style.alignItems = 'center';
        parent.style.justifyContent = 'center';
        if (!cs.rowGap || cs.rowGap === '0px') parent.style.rowGap = '12px';
      }} else {{
        parent.style.textAlign = 'center';
        node.style.marginBottom = '12px';
      }}
    }}
  }}

  function loadSenler(callback) {{
    if (window.Senler && typeof window.Senler.ButtonSubscribe === 'function') {{
      callback();
      return;
    }}
    var existing = document.querySelector('script[src*="senler.ru/dist/web/js/senler.js"]');
    if (existing) {{
      existing.addEventListener('load', callback, {{ once: true }});
      setTimeout(callback, 700);
      return;
    }}
    var s = document.createElement('script');
    s.src = 'https://senler.ru/dist/web/js/senler.js?9';
    s.async = true;
    s.onload = callback;
    s.onerror = function() {{ log('senler.js load failed'); }};
    (document.head || document.documentElement).appendChild(s);
  }}

  function isVkAction(el) {{
    if (!el) return false;
    var text = ((el.innerText || el.textContent || '') + ' ' + (el.getAttribute('aria-label') || '') + ' ' + (el.title || '')).toLowerCase();
    var href = '';
    try {{ href = String(el.href || el.getAttribute('href') || '').toLowerCase(); }} catch (e) {{}}
    return text.indexOf('вконтакте') !== -1 || text.indexOf(' vk') !== -1 || text.indexOf('вк') !== -1 ||
      href.indexOf('vk.com') !== -1 || href.indexOf('vk.ru') !== -1 || href.indexOf('vkontakte') !== -1;
  }}

  function findVkAction(root) {{
    var nodes = root.querySelectorAll('a,button,[role="button"],input[type="button"],input[type="submit"]');
    for (var i = 0; i < nodes.length; i += 1) {{
      if (isVkAction(nodes[i])) return nodes[i].closest('a,button,[role="button"]') || nodes[i];
    }}
    return null;
  }}

  function findAllVkActions(root) {{
    var result = [];
    var nodes = root.querySelectorAll('a,button,[role="button"],input[type="button"],input[type="submit"]');
    for (var i = 0; i < nodes.length; i += 1) {{
      var node = nodes[i].closest('a,button,[role="button"]') || nodes[i];
      if (isVkAction(node) && result.indexOf(node) === -1) result.push(node);
    }}
    return result;
  }}

  function templateAttr(name) {{
    var source = document.getElementById(cfg.buttonId);
    return source ? (source.getAttribute(name) || '') : '';
  }}

  function hideSourceNode() {{
    var source = document.getElementById(cfg.buttonId);
    if (source) source.classList.add('nexus-salebot-senler-source');
    return source;
  }}

  function prepareSenlerNode(node, id) {{
    node.id = id || node.id || cfg.buttonId;
    node.setAttribute('data-nexus-salebot-senler', '1');
    node.classList.add('nexus-salebot-senler-btn');
    var groupId = cfg.groupId || templateAttr('data-vk_group_id');
    var subscriptionId = cfg.subscriptionId || templateAttr('data-subscription_id');
    if (!node.getAttribute('data-vk_group_id') && groupId) node.setAttribute('data-vk_group_id', groupId);
    if (!node.getAttribute('data-subscription_id') && subscriptionId) node.setAttribute('data-subscription_id', subscriptionId);
    if (cfg.text) node.setAttribute('data-text', cfg.text);
    else if (!node.getAttribute('data-text')) node.setAttribute('data-text', 'Записаться в ВКонтакте');
    if (cfg.altText) node.setAttribute('data-alt_text', cfg.altText);
    return node;
  }}

  function makeSenlerNode(index) {{
    hideSourceNode();
    var id = cfg.buttonId + '-' + (index + 1);
    var node = document.getElementById(id);
    if (node) return prepareSenlerNode(node, id);
    node = document.createElement('div');
    node.id = id;
    node.setAttribute('data-vk_group_id', cfg.groupId || templateAttr('data-vk_group_id'));
    node.setAttribute('data-subscription_id', cfg.subscriptionId || templateAttr('data-subscription_id'));
    node.setAttribute('data-text', cfg.text);
    node.setAttribute('data-alt_text', cfg.altText);
    return prepareSenlerNode(node, id);
  }}

  function looksRendered(node) {{
    if (!node) return false;
    if (node.querySelector('iframe,a,button')) return true;
    if ((node.textContent || '').trim()) return true;
    var rect = node.getBoundingClientRect();
    return rect.width > 20 && rect.height > 12;
  }}

  function findFallbackAnchor(root) {{
    return root.querySelector('.link_group .tg_link') || root.querySelector('.tg_link');
  }}

  function collectPageParams() {{
    var pairs = [];
    function add(raw) {{
      raw = String(raw || '').replace(/^[?#]/, '');
      if (!raw) return;
      raw.split('&').forEach(function(part) {{
        if (!part) return;
        var key = part.split('=')[0] || '';
        if (!key) return;
        for (var i = 0; i < pairs.length; i += 1) {{
          if ((pairs[i].split('=')[0] || '') === key) return;
        }}
        pairs.push(part);
      }});
    }}
    function cookieValue(name) {{
      var m = ('; ' + document.cookie).match('; ' + name + '=([^;]*)');
      return m ? decodeURIComponent(m[1]) : '';
    }}
    function addPair(key, value) {{
      if (!value) return;
      add(key + '=' + encodeURIComponent(value));
    }}
    add(window.location.search);
    add(window.location.hash);
    addPair('yclid', cookieValue('yclid'));
    addPair('_ym_uid', cookieValue('_ym_uid'));
    return pairs;
  }}

  function urlHasParam(url, key) {{
    try {{ return new RegExp('([?#&])' + key + '=').test(url); }}
    catch (e) {{ return url.indexOf(key + '=') !== -1; }}
  }}

  function appendParamsToUrl(url) {{
    var params = collectPageParams().filter(function(part) {{
      var key = (part.split('=')[0] || '').toLowerCase();
      return key && key !== 's';
    }});
    if (!params.length || !url || !/vk\\.(com|ru)\\/app\\d+_/i.test(url)) return url;
    var hashIndex = url.indexOf('#');
    var hash = hashIndex === -1 ? '' : url.slice(hashIndex);
    var before = hashIndex === -1 ? url : url.slice(0, hashIndex);
    params.forEach(function(part) {{
      var key = part.split('=')[0] || '';
      if (key && !urlHasParam(before + hash, key)) before += (before.indexOf('?') === -1 ? '?' : '&') + part;
    }});
    return before + hash;
  }}

  function senlerAppUrl(node) {{
    var groupId = (node && node.getAttribute('data-vk_group_id')) || cfg.groupId || templateAttr('data-vk_group_id');
    if (!groupId && !cfg.redirectUrl) return '';
    var url = cfg.redirectUrl || ('https://vk.com/app6013442_-' + groupId + '?form_id=1#form_id=1');
    return appendParamsToUrl(url);
  }}

  function bindAppRedirect(node) {{
    if (!node || node.getAttribute('data-nexus-salebot-senler-redirect-bound')) return;
    node.setAttribute('data-nexus-salebot-senler-redirect-bound', '1');
    node.addEventListener('click', function() {{
      var url = senlerAppUrl(node);
      if (!url) return;
      var opened = null;
      try {{ opened = window.open(url, '_blank'); }} catch (e) {{}}
      if (!opened) window.location.href = url;
    }}, false);
  }}

  function patchParamPreservation() {{
    if (window.__nexusSalebotSenlerWindowOpenPatched) return;
    window.__nexusSalebotSenlerWindowOpenPatched = true;
    var originalOpen = window.open;
    if (typeof originalOpen === 'function') {{
      window.open = function(url, target, features) {{
        if (typeof url === 'string') url = appendParamsToUrl(url);
        return originalOpen.call(window, url, target, features);
      }};
    }}
  }}

  function updateNestedLinks(node) {{
    if (!node) return;
    var links = node.querySelectorAll('a[href*="vk.com/app5898182_"],a[href*="vk.ru/app5898182_"]');
    for (var i = 0; i < links.length; i += 1) {{
      links[i].href = appendParamsToUrl(links[i].href);
    }}
  }}

  function mountButton(vk, index) {{
    if (!vk || vk.getAttribute('data-nexus-salebot-senler-bound')) return false;
    vk.setAttribute('data-nexus-salebot-senler-bound', '1');
    vk.classList.add('nexus-salebot-senler-pending-vk');
    var senlerNode = makeSenlerNode(index);
    var anchor = vk;
    ensureStyles();
    senlerNode.classList.remove('nexus-salebot-senler-source');
    senlerNode.setAttribute('data-nexus-salebot-senler-mounted', '1');
    if (senlerNode.parentNode !== anchor.parentNode || senlerNode.previousElementSibling !== anchor) {{
      anchor.parentNode.insertBefore(senlerNode, anchor.nextSibling);
    }}
    fitToAnchor(senlerNode, anchor);
    [250, 800, 2000, 5000].forEach(function(delay) {{
      setTimeout(function() {{ fitToAnchor(senlerNode, anchor); }}, delay);
    }});

    loadSenler(function() {{
      if (!senlerNode.classList.contains('senler-btn')) {{
        try {{ window.Senler.ButtonSubscribe(senlerNode.id); }}
        catch (e) {{ log('ButtonSubscribe failed', e); }}
      }}
      senlerNode.classList.add('nexus-salebot-senler-btn');
      if (cfg.text && senlerNode.textContent.trim() !== cfg.text) senlerNode.textContent = cfg.text;
      fitToAnchor(senlerNode, anchor);
      patchParamPreservation();
      updateNestedLinks(senlerNode);
      bindAppRedirect(senlerNode);

      var started = Date.now();
      var timer = setInterval(function() {{
        updateNestedLinks(senlerNode);
        bindAppRedirect(senlerNode);
        if (looksRendered(senlerNode)) {{
          clearInterval(timer);
          if (cfg.hideVk) {{
            vk.style.display = 'none';
            vk.setAttribute('data-nexus-salebot-senler-hidden', '1');
          }} else {{
            vk.classList.remove('nexus-salebot-senler-pending-vk');
          }}
          return;
        }}
        if (Date.now() - started > 5000) {{
          clearInterval(timer);
          vk.classList.remove('nexus-salebot-senler-pending-vk');
          if (!looksRendered(senlerNode) && senlerNode.parentNode) senlerNode.parentNode.removeChild(senlerNode);
          log('Senler did not render; VK button left visible');
        }}
      }}, 200);
    }});
    return true;
  }}

  function mountOnce(allowFallback) {{
    var root = document.querySelector(cfg.targetSelector) || document.body;
    if (!root) return false;
    var roots = Array.prototype.slice.call(document.querySelectorAll(cfg.targetSelector));
    if (!roots.length) roots = [root];
    var changed = false;
    roots.forEach(function(scope) {{
      findAllVkActions(scope).forEach(function(vk) {{
        if (mountButton(vk, mountedCount)) {{
          mountedCount += 1;
          changed = true;
        }}
      }});
    }});
    if (allowFallback && !changed && mountedCount === 0) {{
      var fallback = findFallbackAnchor(root);
      if (fallback) {{
        var fakeVk = fallback;
        if (!fakeVk.getAttribute('data-nexus-salebot-senler-bound')) changed = mountButton(fakeVk, mountedCount);
        if (changed) mountedCount += 1;
      }}
    }}
    return changed || mountedCount > 0;
  }}

  function boot() {{
    var started = Date.now();
    hideSourceNode();
    function tick() {{
      mountOnce(Date.now() - started > 2500);
    }}
    tick();
    var timer = setInterval(function() {{
      tick();
      if (Date.now() - started > 15000) clearInterval(timer);
    }}, 300);
    var observer = new MutationObserver(tick);
    observer.observe(document.documentElement, {{ childList: true, subtree: true }});
    setTimeout(function() {{ observer.disconnect(); }}, 30000);
  }}

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
}})();
""".lstrip()
    return PlainTextResponse(
        script,
        headers={"Access-Control-Allow-Origin": "*"},
        media_type="application/javascript; charset=utf-8",
    )


@router.get("/embed", response_class=HTMLResponse)
async def embed(request: Request):
    base = str(request.base_url).rstrip("/")
    script_url = f"{base}/salebot-senler-button/api/replace.js"
    snippet = f'<script src="{script_url}" async></script>'
    return HTMLResponse(
        f"<!doctype html><meta charset='utf-8'><pre>{snippet.replace('<', '&lt;').replace('>', '&gt;')}</pre>"
    )
