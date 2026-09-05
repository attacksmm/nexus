(()=>{'use strict';
  const $=id=>document.getElementById(id);
  const match=location.pathname.match(/\/api\/mobile\/lead\/(\d+)\/([A-Za-z0-9_-]{24})$/);
  const leadId=match&&match[1],signature=match&&match[2];
  const API=location.pathname.replace(/\/mobile\/lead\/.*$/,'/widget');
  const FRAME=location.pathname.replace(/\/api\/mobile\/lead\/.*$/,'/static/amocrm.html');
  const TOKEN_KEY='nexus:messenger-widget:mobile-amocrm:token';
  const USER_KEY='nexus:messenger-widget:mobile-amocrm:user';
  const PREF_KEY='nexus:messenger-widget:fullscreen:prefs';
  let token=localStorage.getItem(TOKEN_KEY)||'',context=null,frameReady=false,lastPostedContext='';

  function readAppearance(){try{return{theme:'dark',scale:'large',...JSON.parse(localStorage.getItem(PREF_KEY)||'{}')}}catch(_){return{theme:'dark',scale:'large'}}}
  function applyAppearance(next={}){const prefs={...readAppearance(),...next};if(!['light','gray','dark'].includes(prefs.theme))prefs.theme='dark';if(!['normal','large','xlarge'].includes(prefs.scale))prefs.scale='large';localStorage.setItem(PREF_KEY,JSON.stringify(prefs));document.documentElement.dataset.theme=prefs.theme;document.documentElement.dataset.uiScale=prefs.scale;document.querySelectorAll('[data-theme-choice]').forEach(button=>button.classList.toggle('active',button.dataset.themeChoice===prefs.theme));document.querySelectorAll('[data-scale-choice]').forEach(button=>button.classList.toggle('active',button.dataset.scaleChoice===prefs.scale));return prefs}
  function postAppearance(){if(frameReady&&$('frame').contentWindow){const prefs=readAppearance();$('frame').contentWindow.postMessage({type:'nexus-messenger-fullscreen-preferences',theme:prefs.theme,scale:prefs.scale},location.origin)}}
  function postContext(){if(frameReady&&context&&$('frame').contentWindow){const signature=JSON.stringify(context);if(signature!==lastPostedContext){$('frame').contentWindow.postMessage({type:'nexus-messenger-context',context,completeness:'enriched'},location.origin);lastPostedContext=signature}postAppearance()}}
  applyAppearance();

  function show(id){for(const name of ['loading','login','failure','workspace'])$(name).hidden=name!==id}
  function showFailure(message){$('failureText').textContent=message||'Сервис временно не ответил.';show('failure')}
  function sessionIdentity(){return{platform_user_id:(context&&context.platform_user_id)||localStorage.getItem(USER_KEY)||'',platform_user_email:(context&&context.platform_user_email)||''}}
  async function request(path,body={},auth=true){const timeoutMs=15000,controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeoutMs);try{const response=await fetch(API+path,{method:'POST',credentials:'omit',cache:'no-store',headers:{'Content-Type':'application/json','X-Nexus-Messenger-Platform':'amocrm',...(auth&&token?{'Authorization':'Bearer '+token}:{})},body:JSON.stringify({lead_id:leadId,signature,...(auth?sessionIdentity():{}),...body}),signal:controller.signal});const data=await response.json().catch(()=>({}));if(!response.ok||data.ok===false){const failure=new Error(data.error||`HTTP ${response.status}`);failure.status=response.status;failure.reauth=!!data.reauth||response.status===401;throw failure}return data}catch(failure){if(failure.name==='AbortError')throw new Error('Сервер отвечает дольше 15 секунд.');throw failure}finally{clearTimeout(timer)}}
  function amocrmTokenKey(userId){return`nexus:messenger-widget:amocrm:${userId}:token`}
  function saveSession(nextToken,userId){token=nextToken;localStorage.setItem(TOKEN_KEY,nextToken);localStorage.setItem(USER_KEY,String(userId||''));if(userId)localStorage.setItem(amocrmTokenKey(userId),nextToken)}
  function clearSession(){const userId=localStorage.getItem(USER_KEY)||'';localStorage.removeItem(TOKEN_KEY);localStorage.removeItem(USER_KEY);if(userId)localStorage.removeItem(amocrmTokenKey(userId));token=''}
  function initials(name){const parts=String(name||'К').trim().split(/\s+/).filter(Boolean);return(parts.slice(0,2).map(part=>part[0]).join('')||'К').toUpperCase()}
  function closeAppearance(){if($('appearance').hidden)return;$('appearance').hidden=true;$('conversationSettings').setAttribute('aria-expanded','false')}
  function toggleAppearance(){const open=$('appearance').hidden;$('appearance').hidden=!open;$('conversationSettings').setAttribute('aria-expanded',String(open));if(open)applyAppearance()}

  function openWorkspace(data){context=data.context;saveSession(token,context.platform_user_id);$('clientName').textContent=context.name||'Клиент';$('clientAvatar').textContent=initials(context.name);$('dealName').textContent=`Сделка #${leadId}${data.admin_name?' · '+data.admin_name:''}`;$('frameLoading').hidden=false;$('frame').classList.remove('ready');show('workspace');if(!$('frame').getAttribute('src'))$('frame').src=FRAME+'?standalone=1&v=5215';else postContext()}
  async function loadContext(){return request('/mobile-context',{})}
  async function boot(){if(!leadId||!signature)return showFailure('Ссылка недействительна. Откройте поле «Переписка» заново из сделки amoCRM.');show('loading');$('loadingText').textContent='Проверяем безопасный доступ…';if(!token){show('login');$('code').focus();return}try{openWorkspace(await loadContext())}catch(failure){if(failure.reauth){clearSession();$('loginError').textContent='Сессия закончилась. Введите личный код ещё раз.';show('login');$('code').focus();return}showFailure(failure.message)}}

  $('loginForm').onsubmit=async event=>{event.preventDefault();const button=$('activate'),code=$('code').value.trim();$('loginError').textContent='';button.disabled=true;button.classList.add('busy');button.textContent='Проверяем…';try{const data=await request('/mobile-activate',{code},false);saveSession(data.device_token,data.platform_user_id);show('loading');$('loadingText').textContent='Загружаем данные сделки…';openWorkspace(await loadContext())}catch(failure){$('loginError').textContent=failure.message||'Не удалось войти';show('login')}finally{button.disabled=false;button.classList.remove('busy');button.textContent='Войти'}};
  $('retry').onclick=boot;
  $('back').onclick=()=>{if(history.length>1)history.back();else location.href='https://sobakovodpro.amocrm.ru/'};
  $('conversationSettings').onclick=toggleAppearance;
  $('appearance').onclick=event=>{if(event.target===$('appearance'))closeAppearance()};
  document.querySelectorAll('[data-theme-choice]').forEach(button=>button.onclick=()=>{applyAppearance({theme:button.dataset.themeChoice});postAppearance()});
  document.querySelectorAll('[data-scale-choice]').forEach(button=>button.onclick=()=>{applyAppearance({scale:button.dataset.scaleChoice});postAppearance()});
  document.addEventListener('keydown',event=>{if(event.key==='Escape')closeAppearance()});
  $('logout').onclick=async()=>{const button=$('logout'),controller=new AbortController(),timer=setTimeout(()=>controller.abort(),5000);button.disabled=true;button.classList.add('busy');button.setAttribute('aria-busy','true');button.setAttribute('aria-label','Выходим…');try{if(context)await fetch(API+'/logout',{method:'POST',credentials:'omit',cache:'no-store',headers:{'Content-Type':'application/json','X-Nexus-Messenger-Platform':'amocrm','Authorization':'Bearer '+token},body:JSON.stringify(context),signal:controller.signal})}catch(_){}finally{clearTimeout(timer)}clearSession();context=null;frameReady=false;lastPostedContext='';$('frame').removeAttribute('src');$('frame').classList.remove('ready');$('code').value='';$('loginError').textContent='';show('login');button.classList.remove('busy');button.removeAttribute('aria-busy');button.setAttribute('aria-label','Выйти');button.disabled=false;$('code').focus()};
  $('frame').onload=()=>{frameReady=true;lastPostedContext='';postContext()};
  addEventListener('message',event=>{if(event.origin!==location.origin||event.source!==$('frame').contentWindow||!event.data)return;if(event.data.type==='nexus-messenger-ready'){frameReady=true;postContext();return}if(event.data.type==='nexus-messenger-theme'&&['light','gray','dark'].includes(event.data.theme)){applyAppearance({theme:event.data.theme});return}if(event.data.type==='nexus-messenger-painted'){$('frameLoading').hidden=true;$('frame').classList.add('ready');return}if(event.data.type==='nexus-messenger-close'&&history.length>1)history.back()});
  addEventListener('pageshow',event=>{if(event.persisted&&context){show('workspace');postContext()}});
  boot();
})();
