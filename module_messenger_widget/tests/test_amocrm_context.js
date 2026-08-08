const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

let Widget;
const calls = [];
const currentCard = {id: 15462823};
currentCard.self = currentCard;
const responses = {
  '/api/v4/leads/15462823?with=contacts': {
    id: 15462823,
    responsible_user_id: 6269974,
    custom_fields_values: [
      {field_name: 'utm_term', field_id: 1, values: [{value: '789663225'}]},
      {field_name: 'platform_id', field_id: 2, values: [{value: '789663225'}]},
    ],
    _embedded: {contacts: [{id: 991, is_main: true}]},
  },
  '/api/v4/contacts/991': {
    id: 991,
    name: 'Никита',
    custom_fields_values: [
      {field_code: 'PHONE', field_name: 'Телефон', values: [{value: '+7 (996) 415-85-37'}]},
      {field_code: 'EMAIL', field_name: 'Email', values: [{value: 'attack.smm@gmail.com'}]},
      {field_name: 'salebot_id', field_id: 3, values: [{value: 'sb-991'}]},
    ],
  },
  '/api/v4/chats/templates?limit=50&page=1': {
    _embedded: {chat_templates: [{id: 7, name: 'Приветствие', content: 'Здравствуйте, {{contact.name}}!', type: 'amocrm'}]},
    _links: {},
  },
};
function jquery() { throw new Error('DOM access is not expected'); }
jquery.ajax = async options => {
  const path = options.url;
  assert.equal(options.timeout, 1500);
  calls.push(path);
  if (!responses[path]) throw new Error('Unexpected request: ' + path);
  return responses[path];
};
const sandbox = {
  define: (_, factory) => { Widget = factory(jquery); },
  window: {APP: {data: {current_card: currentCard}, constant: key => key === 'user' ? {id: 6269974, login: 'manager@example.test'} : {}}},
  location: {pathname: '/leads/detail/15462823', href: 'https://sobakovodpro.amocrm.ru/leads/detail/15462823'},
  URL,
  setTimeout,
};
vm.runInNewContext(fs.readFileSync(process.argv[2], 'utf8'), sandbox);
const widget = Object.create(Widget.prototype);
widget.system = () => ({area: 'lcard'});
Widget.call(widget);

(async () => {
  const context = await widget.__nexusCardContext();
  assert.equal(context.entity_type, 'lead');
  assert.equal(context.entity_id, '15462823');
  assert.equal(context.name, 'Никита');
  assert.equal(context.phone, '+7 (996) 415-85-37');
  assert.equal(context.fields.utm_term, '789663225');
  assert.equal(context.fields.platform_id, '789663225');
  assert.equal(context.fields.salebot_id, 'sb-991');
  assert.equal(context.fields.responsible_user_id, '6269974');
  assert.deepEqual(calls, ['/api/v4/leads/15462823?with=contacts', '/api/v4/contacts/991']);

  const templates = await widget.__nexusAmoTemplates();
  assert.equal(templates.length, 1);
  assert.equal(templates[0].name, 'Приветствие');
  assert.deepEqual(calls, [
    '/api/v4/leads/15462823?with=contacts', '/api/v4/contacts/991',
    '/api/v4/chats/templates?limit=50&page=1',
  ]);

  jquery.ajax = () => new Promise(() => {});
  const started = Date.now();
  const fallback = await widget.__nexusFastContext();
  assert.equal(fallback.entity_id, '15462823');
  assert(Date.now() - started < 2500, 'context fallback exceeded its deadline');
})().catch(error => { console.error(error); process.exitCode = 1; });
