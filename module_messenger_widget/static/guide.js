(()=>{
  "use strict";
  const form=document.getElementById("articleSearch"),input=document.getElementById("articleSearchInput"),results=document.getElementById("articleSearchResults");
  if(!form||!input||!results)return;
  const compact=value=>String(value||"").replace(/\s+/g," ").replace(/\s+([.,!?;:])/g,"$1").trim();
  const normalize=value=>compact(value).toLocaleLowerCase("ru").replace(/ё/g,"е").replace(/[^a-zа-я0-9]+/gi," ").trim();
  const articles=[...document.querySelectorAll("main section[id]")].map(section=>{
    const heading=section.querySelector("h2"),title=heading?heading.textContent.trim():section.id;
    const bodyParts=[],walker=document.createTreeWalker(section,NodeFilter.SHOW_TEXT);while(walker.nextNode())if(!walker.currentNode.parentElement.closest("h2"))bodyParts.push(walker.currentNode.nodeValue);const body=compact(bodyParts.join(" "));
    if(heading){heading.tabIndex=-1;const anchor=document.createElement("a");anchor.className="heading-anchor";anchor.href="#"+section.id;anchor.textContent="#";anchor.setAttribute("aria-label","Ссылка на раздел «"+title+"»");heading.append(anchor)}
    return{id:section.id,title,body,search:normalize(title+" "+body)};
  });
  let matches=[],selected=-1;
  const close=()=>{results.hidden=true;input.setAttribute("aria-expanded","false");selected=-1};
  const select=index=>{selected=Math.max(0,Math.min(index,matches.length-1));[...results.querySelectorAll("a")].forEach((link,position)=>link.setAttribute("aria-selected",String(position===selected)));const active=results.querySelector('a[aria-selected="true"]');if(active)active.scrollIntoView({block:"nearest"})};
  const goTo=article=>{if(!article)return;close();input.value=article.title.replace(/^\d+\.\s*/,"");const section=document.getElementById(article.id),heading=section&&section.querySelector("h2");if(location.hash!=="#"+article.id)history.pushState(null,"","#"+article.id);section.scrollIntoView({behavior:matchMedia("(prefers-reduced-motion: reduce)").matches?"auto":"smooth",block:"start"});if(heading)heading.focus({preventScroll:true})};
  const snippetFor=(article,words)=>{const source=article.body||article.title,normalized=source.toLocaleLowerCase("ru").replace(/ё/g,"е");const positions=words.map(word=>normalized.indexOf(word)).filter(position=>position>=0),hit=positions.length?Math.min(...positions):0;let start=Math.max(0,hit-48),end=Math.min(source.length,hit+116);if(start){const space=source.indexOf(" ",start);if(space>=0&&space<hit)start=space+1}if(end<source.length){const space=source.lastIndexOf(" ",end);if(space>hit)end=space}return(start?"…":"")+source.slice(start,end)+(end<source.length?"…":"")};
  const appendHighlighted=(node,text,words)=>{const normalized=String(text).toLocaleLowerCase("ru").replace(/ё/g,"е");let cursor=0;while(cursor<text.length){let position=-1,word="";for(const candidate of words){const found=normalized.indexOf(candidate,cursor);if(found>=0&&(position<0||found<position)){position=found;word=candidate}}if(position<0){node.append(document.createTextNode(text.slice(cursor)));break}if(position>cursor)node.append(document.createTextNode(text.slice(cursor,position)));const mark=document.createElement("mark");mark.textContent=text.slice(position,position+word.length);node.append(mark);cursor=position+word.length}};
  const render=()=>{const query=normalize(input.value),words=[...new Set(query.split(" ").filter(Boolean))];if(!words.length){close();results.replaceChildren();matches=[];return}matches=articles.filter(article=>words.every(word=>article.search.includes(word))).map(article=>({...article,snippet:snippetFor(article,words)}));results.replaceChildren();selected=-1;if(!matches.length){const empty=document.createElement("div");empty.className="search-empty";empty.textContent="Раздел не найден";results.append(empty)}else matches.forEach((article,index)=>{const link=document.createElement("a"),title=document.createElement("span"),snippet=document.createElement("span");link.href="#"+article.id;link.role="option";title.className="search-result-title";title.textContent=article.title;snippet.className="search-result-snippet";appendHighlighted(snippet,article.snippet,words);link.append(title,snippet);link.setAttribute("aria-selected","false");link.addEventListener("click",event=>{event.preventDefault();goTo(article)});results.append(link);if(index===0)select(0)});results.hidden=false;input.setAttribute("aria-expanded","true")};
  input.addEventListener("focus",render);input.addEventListener("input",render);
  input.addEventListener("keydown",event=>{if(event.key==="Escape"){close();input.blur();return}if(event.key==="ArrowDown"){event.preventDefault();if(results.hidden)render();else select(selected+1);return}if(event.key==="ArrowUp"){event.preventDefault();select(selected-1);return}if(event.key==="Enter"){event.preventDefault();if(results.hidden)render();if(matches.length)goTo(matches[Math.max(selected,0)])}});
  form.addEventListener("submit",event=>{event.preventDefault();if(matches.length)goTo(matches[Math.max(selected,0)])});
  document.addEventListener("pointerdown",event=>{if(!form.contains(event.target))close()});
})();


(()=>{
  const mediaBase='../static/guide-media/';
  document.querySelectorAll('.guide-recording[data-recording]').forEach(figure=>{
    const name=figure.dataset.recording,title=figure.dataset.title||'Учебный пример',summary=figure.dataset.summary||'';
    const caption=document.createElement('figcaption'),heading=document.createElement('h3'),kind=document.createElement('p');
    heading.textContent=title;kind.textContent='Запись настоящего виджета · учебные данные';caption.append(heading,kind);
    const frame=document.createElement('div');frame.className='recording-frame';
    const video=document.createElement('video');video.controls=true;video.playsInline=true;video.preload='none';video.poster=mediaBase+name+'-poster.png';video.width=920;video.height=560;video.setAttribute('aria-label',title);
    const source=document.createElement('source');source.src=mediaBase+name+'.webm';source.type='video/webm';
    const track=document.createElement('track');track.kind='captions';track.src=mediaBase+name+'.vtt';track.srclang='ru';track.label='Шаги';
    video.append(source,track,document.createTextNode('Ваш браузер не воспроизводит видео. '));
    const fallback=document.createElement('a');fallback.href=source.src;fallback.textContent='Скачать запись';video.append(fallback);
    const overlay=document.createElement('div');overlay.className='recording-loading';overlay.hidden=true;overlay.setAttribute('role','status');
    const spinner=document.createElement('span');spinner.className='recording-spinner';spinner.setAttribute('aria-hidden','true');overlay.append(spinner,document.createTextNode('Загружаем видео…'));
    frame.append(video,overlay);figure.append(caption,frame);
    const copy=document.createElement('p');copy.className='recording-summary';copy.textContent=summary;figure.append(copy);
    const hint=document.createElement('p');hint.className='recording-hint';hint.textContent='Нажмите ▶. Курсор, клики и ввод показаны плавно; реальные действия с клиентами не выполняются.';figure.append(hint);
  });
  document.querySelectorAll('.guide-recording video').forEach(video=>{
    const frame=video.parentElement,overlay=frame&&frame.querySelector('.recording-loading'),source=video.querySelector('source');
    if(!overlay||!source)return;
    let requested=false,errorShown=false;
    const resetOverlay=()=>{if(!errorShown)overlay.replaceChildren(Object.assign(document.createElement('span'),{className:'recording-spinner'}),document.createTextNode('Загружаем видео…'))};
    const show=()=>{if(requested&&!errorShown)overlay.hidden=false};
    const hide=()=>{if(!errorShown)overlay.hidden=true};
    video.addEventListener('play',()=>{requested=true;resetOverlay();if(video.readyState<3)show()});
    video.addEventListener('waiting',show);video.addEventListener('playing',hide);video.addEventListener('canplay',hide);video.addEventListener('pause',hide);video.addEventListener('ended',hide);
    video.addEventListener('error',()=>{
      errorShown=true;overlay.hidden=false;overlay.replaceChildren(document.createTextNode('Видео не загрузилось. '));
      const retry=document.createElement('button');retry.type='button';retry.textContent='Повторить';retry.addEventListener('click',()=>{errorShown=false;requested=true;resetOverlay();overlay.hidden=false;video.load();video.play().catch(()=>{})},{once:true});overlay.append(retry);
      const figure=video.closest('figure');if(figure&&!figure.querySelector('.recording-download')){const link=document.createElement('a');link.className='recording-download';link.href=source.src;link.textContent='Скачать видео';figure.append(link)}
    });
  });
})();
