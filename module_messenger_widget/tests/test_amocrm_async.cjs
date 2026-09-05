// Exercise the shipped iframe handlers with controlled network completion order.
const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../static/amocrm.html'),'utf8');
const functionSource=name=>html.slice(html.indexOf('  async function '+name+'('),html.indexOf('\n  ',html.indexOf('  async function '+name+'(')+3));
const sendHandler=html.slice(html.indexOf("  $('send').onclick=async()=>{"),html.indexOf('\n  setInterval(refreshActive'));
function deferred(){let resolve;const promise=new Promise(done=>resolve=done);return{promise,resolve}}
function fixture(){
  const elements=Object.fromEntries(['message','send','emailSubject','sendAll','feed','templateSettings'].map(id=>[id,{value:'',dataset:{},disabled:false,classList:{add(){},remove(){}},closest(){return{hidden:false}},hidden:true}]));
  elements.message.value='Hello';elements.emailSubject.value='Subject';
  const state={requests:0,refreshes:0,renders:0,status:'',confirm:async()=>true,send:async()=>({sent:['VK'],queued:[],failed:[],status:'Отправлено: VK'})};
  const sandbox={console,setTimeout,clearTimeout,Boolean,String,Number,Promise,$:id=>elements[id],sending:false,channelGeneration:1,context:{},active:{provider:'vk',channel_id:'vk'},cardChannels:[{provider:'vk',channel_id:'vk'}],STANDALONE:true,bootGeneration:1,widgetVisible:true,document:{hidden:false},polling:false,openingConversation:false,loadingOlder:false,olderPagesLoaded:false,nextOffset:12,hasMore:true,
    cardCacheId:()=> 'lead:1',channelKey:r=>r.channel_id,conversationKey:r=>r.channel_id,sendTargets:rows=>rows,emailNeedsSubject:()=>false,hasEmailTarget:()=>false,emailNeedsRecommendations:()=>false,syncEmailSubject(){},showError:value=>state.status=value,confirmEmailRecommendations:()=>state.confirm(),sendText:()=>{state.requests++;return state.send()},sizeMessageInput(){},clearAttachment(){},saveDraft(){},readCardSnapshot:()=>null,writeCardSnapshot(){},refreshActive:()=>{state.refreshes++;return new Promise(()=>{})},loadProfileLinks(){},request:()=>state.network.promise,channelBody:r=>r,rememberConversation(){},renderMessages(){state.renders++},setRefreshTask(){}};
  const context=vm.createContext(sandbox);vm.runInContext(sendHandler,context);return{context,elements,state};
}
test('send completion releases button and reports success without waiting for history',async()=>{
  const {elements,state}=fixture();await elements.send.onclick();assert.equal(state.refreshes,1);assert.equal(state.status,'Отправлено: VK');assert.equal(elements.send.disabled,false);assert.equal(elements.message.value,'');
});
test('repeated send during Email confirmation is ignored',async()=>{
  const {context,elements,state}=fixture(),confirmation=deferred();context.emailNeedsRecommendations=()=>true;state.confirm=()=>confirmation.promise;
  const first=elements.send.onclick();await elements.send.onclick();assert.equal(state.requests,0);assert.equal(elements.send.disabled,true);confirmation.resolve(true);await first;assert.equal(state.requests,1);
});
test('late send result does not clear a newly typed message',async()=>{
  const {elements,state}=fixture(),network=deferred();state.send=()=>network.promise;const sending=elements.send.onclick();elements.message.value='Next message';network.resolve({sent:['VK'],queued:[],failed:[],status:'sent'});await sending;assert.equal(elements.message.value,'Next message');
});
test('Email subject is captured with the message before confirmation',async()=>{
  const {context,elements,state}=fixture(),confirmation=deferred();context.emailNeedsRecommendations=()=>true;state.confirm=()=>confirmation.promise;
  let subject;context.sendText=async(text,rows,attachment,value)=>{subject=value;return{sent:['Email'],queued:[],failed:[],status:'sent'}};
  const sending=elements.send.onclick();elements.emailSubject.value='Next subject';confirmation.resolve(true);await sending;assert.equal(subject,'Subject');
});
test('confirmation cannot send into a different conversation after navigation',async()=>{
  const {context,elements,state}=fixture(),confirmation=deferred();context.emailNeedsRecommendations=()=>true;state.confirm=()=>confirmation.promise;
  const sending=elements.send.onclick();context.channelGeneration++;context.active={provider:'email',channel_id:'email'};confirmation.resolve(true);await sending;assert.equal(state.requests,0);assert.equal(elements.send.disabled,false);
});
test('late older-history response cannot appear in another channel',async()=>{
  const {context,state}=fixture();state.network=deferred();vm.runInContext(functionSource('loadOlder'),context);const loading=context.loadOlder();context.channelGeneration++;context.active={channel_id:'email',provider:'email'};state.network.resolve({messages:[{text:'old VK'}]});await loading;assert.equal(state.renders,0);
});
test('history polling never enables the send button while sending',async()=>{
  const {context,elements,state}=fixture();state.network=deferred();context.sending=true;elements.send.disabled=true;
  const start=html.indexOf('  function updateComposerAvailability(');vm.runInContext(html.slice(start,html.indexOf('\n',start)),context);vm.runInContext(functionSource('refreshActive'),context);
  const polling=context.refreshActive();state.network.resolve({can_send:true,messages:[]});await polling;assert.equal(elements.send.disabled,true);
});
test('polling caches latest messages but does not replace an older page being read',async()=>{
  const {context,elements,state}=fixture();state.network=deferred();context.olderPagesLoaded=true;elements.feed.scrollHeight=2000;elements.feed.clientHeight=500;elements.feed.scrollTop=100;context.updateComposerAvailability=()=>{};
  vm.runInContext(functionSource('refreshActive'),context);const polling=context.refreshActive();state.network.resolve({can_send:true,messages:[]});await polling;assert.equal(state.renders,0);
});
