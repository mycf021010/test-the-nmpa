/** 提供页面所需的 DOM、Cookie 和浏览器属性兼容环境。Python 调用它初始化来源会话；不要在此硬编码密钥或数据库信息。 */
const fs = require('node:fs');
const vm = require('node:vm');
const v8 = require('node:v8');
const {JSDOM, VirtualConsole} = require('jsdom');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const dom = new JSDOM(input.html || `<html><head><meta id="${input.meta.id}" content="${input.meta.content}" r="m"><script r="m"></script><script r="m" src="${input.scriptUrl}"></script></head><body></body></html>`, {
  url: input.url, referrer: input.referrer || input.url,
  runScripts: 'outside-only', pretendToBeVisual: true,
  virtualConsole: new VirtualConsole(),
});
const w = dom.window;
v8.setFlagsFromString('--allow-natives-syntax');
const undetectable = vm.runInThisContext('%GetUndetectable()');
v8.setFlagsFromString('--no-allow-natives-syntax');
Object.defineProperty(w.document, 'all', {value: undetectable,configurable:true});
for(const [k,v] of Object.entries({webdriver:false,userAgent:input.userAgent,platform:'Win32',language:'zh-CN',languages:['zh-CN','zh'],hardwareConcurrency:8,deviceMemory:8,vendor:'Google Inc.'})) Object.defineProperty(w.navigator,k,{value:v,configurable:true});
w.chrome = {runtime:{}};
// DOM methods in jsdom are JS wrappers; browser fingerprint checks expect native strings.
const nativeSources=new WeakMap();
for(const root of [w,w.document,w.navigator,w.location,w.screen,w.localStorage,w.sessionStorage,w.XMLHttpRequest.prototype]) {
  let obj=root;
  for(let depth=0;obj && depth<8;depth++,obj=Object.getPrototypeOf(obj)) {
    for(const key of Reflect.ownKeys(obj)) {
      const d=Object.getOwnPropertyDescriptor(obj,key);
      for(const f of [d.value,d.get,d.set])if(typeof f==='function')nativeSources.set(f,`function ${f.name || String(key)}() { [native code] }`);
    }
  }
}
const originalToString=w.Function.prototype.toString;
w.Function.prototype.toString=function(){return nativeSources.get(this)||originalToString.call(this);};
nativeSources.set(w.Function.prototype.toString,'function toString() { [native code] }');
// Shims created in Node belong to its Function realm, not jsdom's VM realm.
const hostToString=Function.prototype.toString;
Function.prototype.toString=function(){return nativeSources.get(this)||hostToString.call(this);};
nativeSources.set(Function.prototype.toString,'function toString() { [native code] }');
require('./chromium-shims.cjs')(w,nativeSources,input);
function HTMLAllCollection(){}
nativeSources.set(HTMLAllCollection,'function HTMLAllCollection() { [native code] }');
Object.defineProperty(HTMLAllCollection.prototype,Symbol.toStringTag,{value:'HTMLAllCollection'});
w.HTMLAllCollection=HTMLAllCollection;
Object.setPrototypeOf(undetectable,HTMLAllCollection.prototype);
Object.defineProperty(undetectable,'length',{get:()=>w.document.getElementsByTagName('*').length});
undetectable.item=function(i){return w.document.getElementsByTagName('*')[i]||null;};
undetectable.namedItem=function(name){return w.document.getElementById(name);};
delete w.document.all;
const allGetter=function(){return undetectable;};nativeSources.set(allGetter,'function get all() { [native code] }');
Object.defineProperty(w.Document.prototype,'all',{get:allGetter,configurable:true,enumerable:true});
let rewritten, redirect=null;
w.XMLHttpRequest.prototype.open = function(method,url){rewritten = new URL(url,input.url).href;};
nativeSources.set(w.XMLHttpRequest.prototype.open,'function open() { [native code] }');
for(const c of (input.cookie || '').split('; ')) if(c) w.document.cookie=c;
// Navigation is captured through jsdom's internal Location implementation.
const implSymbol = Object.getOwnPropertySymbols(w.location).find(s=>s.description==='impl');
if(implSymbol) w.location[implSymbol]._locationObjectNavigate = function(urlRecord){
  const {serializeURL}=require('whatwg-url');redirect=serializeURL(urlRecord);
};
const context=dom.getInternalVMContext();
if(input.normalizeChrome){
  w.__chromeFingerprintArray=function(){
    const a=[];let flags=0,kind=1;
    Object.defineProperty(a,10,{configurable:true,enumerable:true,get(){return flags;},set(v){flags=(v&~(0x08000000|0x00040000))|0x01000000;}});
    Object.defineProperty(a,14,{configurable:true,enumerable:true,get(){return kind;},set(v){kind=1;}});
    return a;
  };
}
if(input.trace || input.normalizeChrome){
  const originalEval=w.eval;
  w.eval=function(code){
    if(input.normalizeChrome && typeof code==='string' && code.length>1000)code=require('./fingerprint-projection.cjs')(code);
    if(input.trace && typeof code==='string' && code.length>1000){
      fs.mkdirSync('artifacts/decode',{recursive:true});
      const id=require('node:crypto').createHash('sha256').update(code).digest('hex').slice(0,10);
      fs.writeFileSync('artifacts/decode/eval-'+id+'.js',code);
      const names=[...new Set([...code.matchAll(/(_\$[\w$]+)\[/g)].map(m=>m[1]))];
      const functions=[...code.matchAll(/function (_\$[\w$]+)\(\)\{([^{}]+)\}/g)].filter(m=>m[2].length<300 && !m[2].includes('apply') && /return/.test(m[2]) && /_\$[\w$]+\[\d+\]/.test(m[2]));
      const hook='window.__rsGet=function(){const t={};'+names.map(n=>`try{if(Array.isArray(${n}) && ${n}.length<2000)t[${JSON.stringify(n)}]=${n};}catch(e){}`).join('')+'return t;};'+
        'window.__rsFns=function(){const t={};'+functions.map(m=>`try{const v=${m[1]}();if(['string','number','boolean'].includes(typeof v))t[${JSON.stringify(m[1])}]=v;}catch(e){}`).join('')+'return t;};';
      code=code.replace(/^(\s*\(function\([^)]*\)\{)/,(m)=>m+hook);
      code=require('./introspection.cjs')(code);
    }
    const result=originalEval(code);
    if(w.__rsTables)fs.writeFileSync('artifacts/decode/node-tables.json',JSON.stringify(w.__rsTables(),null,2));
    return result;
  };
  nativeSources.set(w.eval,'function eval() { [native code] }');
}
try {
  vm.runInContext(input.inline,context,{timeout:10000});
  vm.runInContext(input.external,context,{timeout:10000});
  if(input.after)vm.runInContext(input.after,context,{timeout:10000});
  if(input.requestUrl){const xhr=new w.XMLHttpRequest();xhr.open('GET',input.requestUrl,true);}
  setTimeout(()=>{
    if(input.trace && w.__rsGet){const tables=Object.fromEntries(Object.entries(w.__rsGet()).map(([k,v])=>[k,v.map(x=>['string','number','boolean'].includes(typeof x)?x:null)]));fs.writeFileSync('artifacts/decode/tables.json',JSON.stringify(tables,null,2));if(w.__rsFns)fs.writeFileSync('artifacts/decode/functions.json',JSON.stringify(w.__rsFns(),null,2));}
    if(input.trace && w.__rsScalars)fs.writeFileSync('artifacts/decode/node-scalars.json',JSON.stringify(w.__rsScalars(),null,2));
    process.stdout.write(JSON.stringify({cookie:w.document.cookie,redirect,url:rewritten}));w.close();
  },1000);
} catch(e){process.stderr.write(e.stack);w.close();process.exitCode=1;}
