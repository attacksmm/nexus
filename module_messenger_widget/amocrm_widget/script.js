define(['jquery'], function ($) {
  'use strict';
  const Widget = function () {
    const self = this;
    const DEFAULT_URL = 'https://junior.sobakovod.pro/nexus/messenger-widget/static/amocrm.html?v=583';
    const AMO_REQUEST_TIMEOUT = 1500;
    const CONTEXT_TIMEOUT = 2000;

    function currentUser() {
      const app = window.APP || window.AMOCRM || {};
      const user = (app.constant && app.constant('user')) || {};
      return {platform_user_id: String(user.id || ''), platform_user_email: String(user.login || user.email || '').toLowerCase()};
    }

    function json(value) {
      if (!value || typeof value !== 'object') return {};
      if (typeof value.toJSON === 'function') return value.toJSON() || {};
      if (value.model) return json(value.model);
      return value.attributes || value;
    }

    function pageCard() {
      const match = String(location.pathname || '').match(/^\/(leads|contacts)\/detail\/(\d+)/);
      const app = window.APP || window.AMOCRM || {};
      const current = json(app.data && app.data.current_card);
      const area = String(self.system && self.system().area || '');
      return {
        entity: match ? match[1] : area === 'ccard' ? 'contacts' : 'leads',
        id: String((match && match[2]) || current.id || '')
      };
    }

    function fieldValue(field) {
      const values = field && Array.isArray(field.values) ? field.values : [];
      const value = values[0] && (typeof values[0] === 'object' ? values[0].value : values[0]);
      return value === null || value === undefined ? '' : String(value);
    }

    function addCustomFields(fields, custom, namespace, overwrite) {
      (Array.isArray(custom) ? custom : []).forEach(function (field) {
        const value = fieldValue(field);
        if (!value) return;
        const keys = [field.field_name, field.name, field.field_code, field.code, field.field_id].filter(Boolean).map(String);
        keys.forEach(function (key) {
          if (overwrite || !fields[key]) fields[key] = value;
          fields[namespace + '.' + key] = value;
        });
      });
    }

    function scalarFields(value, prefix, output) {
      if (!value || typeof value !== 'object') return output;
      Object.keys(value).slice(0, 100).forEach(function (key) {
        const item = value[key], path = (prefix ? prefix + '.' : '') + key;
        if (item !== null && item !== '' && typeof item !== 'object' && !/token|secret|password|cookie/i.test(path)) output[path] = String(item).slice(0, 2000);
      });
      return output;
    }

    function amoJSON(url) {
      return $.ajax({url:url, dataType:'json', timeout:AMO_REQUEST_TIMEOUT});
    }

    async function amoTemplates() {
      const templates = [];
      for (let page = 1; page <= 20; page += 1) {
        const data = await amoJSON('/api/v4/chats/templates?limit=50&page=' + page);
        const rows = data && data._embedded && Array.isArray(data._embedded.chat_templates) ? data._embedded.chat_templates : [];
        rows.forEach(function (row) {
          templates.push({id:row.id, name:row.name, content:row.content, type:row.type, buttons:row.buttons, attachment:row.attachment});
        });
        if (rows.length < 50 || !(data._links && data._links.next)) break;
      }
      return templates;
    }

    async function cardContext() {
      const page = pageCard();
      const app = window.APP || window.AMOCRM || {};
      const constantCard = (app.constant && app.constant('card')) || {};
      let attrs = Object.assign({}, json(app.data && app.data.current_card), json(constantCard));
      try {
        attrs = Object.assign(attrs, await amoJSON('/api/v4/' + page.entity + '/' + encodeURIComponent(page.id) + (page.entity === 'leads' ? '?with=contacts' : '')));
      } catch (error) {}
      const fields = scalarFields(attrs, '', {});
      addCustomFields(fields, attrs.custom_fields_values || attrs.custom_fields, page.entity === 'contacts' ? 'contact' : 'lead', true);
      let phone = '', email = '', contactId = '', contact = {}, name = page.entity === 'contacts' ? String(attrs.name || '').trim() : '';
      if (page.entity === 'contacts') {
        contactId = page.id;
        contact = attrs;
      } else {
        const contacts = (attrs._embedded && attrs._embedded.contacts) || attrs.contacts || [];
        let contactRef = contacts.find(function (item) { return item.is_main; }) || contacts[0] || {};
        if (!contactRef.id) {
          try {
            const links = await amoJSON('/api/v4/leads/' + encodeURIComponent(page.id) + '/links');
            const rows = links._embedded && links._embedded.links || [];
            contactRef = rows.find(function (item) { return item.to_entity_type === 'contacts' && item.metadata && item.metadata.main_contact; }) || rows.find(function (item) { return item.to_entity_type === 'contacts'; }) || {};
            contactRef.id = contactRef.id || contactRef.to_entity_id;
          } catch (error) {}
        }
        contactId = String(contactRef.id || attrs.main_contact_id || '');
        if (contactId) {
          try { contact = await amoJSON('/api/v4/contacts/' + encodeURIComponent(contactId)); } catch (error) {}
        }
        name = String(contact.name || contactRef.name || attrs.contact_name || '').trim();
      }
      addCustomFields(fields, contact.custom_fields_values || contact.custom_fields, 'contact', false);
      (contact.custom_fields_values || contact.custom_fields || []).forEach(function (field) {
        const code = String(field.field_code || field.code || '').toUpperCase(), value = fieldValue(field);
        if (code === 'PHONE' && !phone) phone = value;
        if (code === 'EMAIL' && !email) email = value.toLowerCase();
      });
      fields.responsible_user_id = String(attrs.responsible_user_id || contact.responsible_user_id || '');
      fields.contact_id = contactId;
      return Object.assign(currentUser(), {
        platform: 'amocrm',
        entity_type: page.entity === 'contacts' ? 'contact' : 'lead',
        entity_id: page.id,
        name: name,
        phone: phone,
        email: email,
        source_url: location.href,
        fields: fields
      });
    }

    function basicContext() {
      const page = pageCard(), app = window.APP || window.AMOCRM || {};
      const attrs = Object.assign({}, json(app.data && app.data.current_card), json((app.constant && app.constant('card')) || {}));
      const fields = scalarFields(attrs, '', {});
      addCustomFields(fields, attrs.custom_fields_values || attrs.custom_fields, page.entity === 'contacts' ? 'contact' : 'lead', true);
      return Object.assign(currentUser(), {
        platform: 'amocrm', entity_type:page.entity === 'contacts' ? 'contact' : 'lead', entity_id:page.id,
        name:String(attrs.name || '').trim(), phone:'', email:'', source_url:location.href, fields:fields
      });
    }

    function fastContext() {
      const fallback = basicContext();
      return Promise.race([
        cardContext().catch(function () { return fallback; }),
        new Promise(function (resolve) { setTimeout(function () { resolve(fallback); }, CONTEXT_TIMEOUT); })
      ]);
    }
    self.__nexusCardContext = cardContext;
    self.__nexusFastContext = fastContext;
    self.__nexusAmoTemplates = amoTemplates;

    function widgetUrl() {
      const settings = typeof self.get_settings === 'function' ? self.get_settings() : {};
      const value = String(settings.nexus_url || '').trim();
      return /^https:\/\//i.test(value) ? value.replace(/\/$/, '') : DEFAULT_URL;
    }

    function closeModal() {
      $('#nexus-messenger-modal').remove();
      $(document).off('keydown.nexusMessenger');
    }

    function openModal() {
      closeModal();
      const frameUrl = widgetUrl();
      const layer = $('<div id="nexus-messenger-modal" role="dialog" aria-modal="true"></div>').css({position:'fixed',inset:0,zIndex:10000,background:'rgba(20,29,35,.48)',display:'grid',placeItems:'center',padding:'22px'});
      const shell = $('<div></div>').css({width:'min(1180px,96vw)',height:'min(760px,92vh)',minWidth:'680px',minHeight:'460px',maxWidth:'96vw',maxHeight:'92vh',resize:'both',overflow:'hidden',background:'#111c25',boxShadow:'0 18px 70px rgba(0,0,0,.35)',position:'relative'});
      const loading = $('<div>Загрузка…</div>').css({position:'absolute',inset:0,display:'grid',placeItems:'center',color:'#8193a0',background:'#111c25'});
      const frame = $('<iframe title="Виджет мессенжеров"></iframe>').attr('src', frameUrl).css({width:'100%',height:'100%',border:0,background:'#111c25',opacity:0});
      if (window.innerWidth <= 680) { layer.css('padding', 0); shell.css({width:'100vw',height:'100dvh',minWidth:0,minHeight:0,maxWidth:'none',maxHeight:'none',resize:'none'}); }
      const targetOrigin = new URL(frameUrl).origin;
      let contextDelivered = false;
      let frameDeadline;
      function paint() { clearTimeout(frameDeadline); frame.css('opacity', 1); loading.remove(); }
      function armFrameDeadline() {
        clearTimeout(frameDeadline);
        frameDeadline = setTimeout(function () {
          if (!loading.is(':visible')) return;
          const retry = $('<button type="button">Повторить</button>').css({marginTop:'12px',padding:'7px 14px',cursor:'pointer'});
          loading.empty().append($('<div>Виджет не загрузился</div>'), retry);
          retry.on('click', function () {
            contextDelivered = false;
            loading.text('Загрузка…');
            frame.attr('src', frameUrl);
            armFrameDeadline();
          });
        }, 15000);
      }
      async function sendContext() {
        if (contextDelivered || !frame[0].contentWindow) return;
        contextDelivered = true;
        frame[0].contentWindow.postMessage({type:'nexus-messenger-context', context:await fastContext()}, targetOrigin);
        setTimeout(paint, 1200);
      }
      async function ready(event) {
        if (event.source !== frame[0].contentWindow || event.origin !== targetOrigin || !event.data) return;
        if (event.data.type === 'nexus-messenger-close') { closeModal(); return; }
        if (event.data.type === 'nexus-messenger-painted') { paint(); return; }
        if (event.data.type === 'nexus-messenger-amo-template-export') {
          try {
            frame[0].contentWindow.postMessage({type:'nexus-messenger-amo-template-export-result', request_id:event.data.request_id, templates:await amoTemplates()}, targetOrigin);
          } catch (error) {
            frame[0].contentWindow.postMessage({type:'nexus-messenger-amo-template-export-result', request_id:event.data.request_id, error:'Не удалось прочитать шаблоны amoCRM'}, targetOrigin);
          }
          return;
        }
        if (event.data.type === 'nexus-messenger-ready') sendContext().catch(function () { contextDelivered = false; });
      }
      window.addEventListener('message', ready, {once:false});
      frame.on('load', function () { sendContext().catch(function () { contextDelivered = false; }); });
      shell.append(loading, frame); layer.append(shell).on('click', function (event) { if (event.target === layer[0]) closeModal(); }); $('body').append(layer);
      armFrameDeadline();
      layer.on('remove', function () { clearTimeout(frameDeadline); window.removeEventListener('message', ready); });
      $(document).on('keydown.nexusMessenger', function (event) { if (event.key === 'Escape') closeModal(); });
    }

    this.callbacks = {
      render: function () {
        if (!/^lcard|^ccard/.test(String(self.system && self.system().area || ''))) return true;
        self.render_template({caption:{class_name:'nexus-messenger-caption'},body:'',render:'<button type="button" class="button-input button-input_blue js-nexus-messenger-open" style="width:100%">Открыть мессенджеры</button>'}, {});
        return true;
      },
      bind_actions: function () {
        $(document).off('click.nexusMessenger').on('click.nexusMessenger', '.js-nexus-messenger-open', openModal);
        return true;
      },
      init: function () { return true; },
      settings: function () { return true; },
      onSave: function () { return true; },
      destroy: function () { $(document).off('.nexusMessenger'); closeModal(); }
    };
    return this;
  };
  return Widget;
});
