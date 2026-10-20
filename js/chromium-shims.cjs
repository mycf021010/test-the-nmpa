/** 统一补充运行环境属性；保留原有兼容行为，避免迁移 IDE 时改变来源执行结果。 */
const vm=require('node:vm');
module.exports=function install(w,nativeSources,input){
  const native=(fn,name)=>{nativeSources.set(fn,`function ${name||fn.name}() { [native code] }`);return fn;};
  const navProto=Object.getPrototypeOf(w.navigator);
  const navValues={webdriver:false,userAgent:input.userAgent,appVersion:input.userAgent.replace(/^Mozilla\//,''),platform:'Win32',vendor:'Google Inc.',hardwareConcurrency:8,deviceMemory:8,languages:['zh-CN','zh'],language:'zh-CN',maxTouchPoints:0,pdfViewerEnabled:true};
  for(const [k,v] of Object.entries(navValues)){
    delete w.navigator[k];
    Object.defineProperty(navProto,k,{get:native(function(){return v;},'get '+k),configurable:true,enumerable:true});
  }
  const major=/Chrome\/(\d+)/.exec(input.userAgent)?.[1]||'131';
  const brands=[{brand:'Chromium',version:major},{brand:'Google Chrome',version:major},{brand:'Not_A Brand',version:'99'}];
  const uaData={brands,mobile:false,platform:'Windows',toJSON:native(function(){return{brands,mobile:false,platform:'Windows'};},'toJSON'),getHighEntropyValues:native(async function(){return {brands,mobile:false,platform:'Windows',architecture:'x86',bitness:'64',model:'',platformVersion:'19.0.0',uaFullVersion:major+'.0.0.0'};},'getHighEntropyValues')};
  Object.defineProperty(uaData,Symbol.toStringTag,{value:'NavigatorUAData'});
  Object.defineProperty(navProto,'userAgentData',{get:native(()=>uaData,'get userAgentData'),configurable:true,enumerable:true});
  const plugins=Object.create(w.PluginArray.prototype);
  Object.defineProperty(navProto,'plugins',{get:native(()=>plugins,'get plugins'),configurable:true,enumerable:true});
  const pluginNames=['PDF Viewer','Chrome PDF Viewer','Chromium PDF Viewer','Microsoft Edge PDF Viewer','WebKit built-in PDF'];
  const pluginItems=pluginNames.map(name=>{const p=Object.create(w.Plugin.prototype);for(const [k,v] of Object.entries({name,filename:'internal-pdf-viewer',description:'Portable Document Format',length:2}))Object.defineProperty(p,k,{value:v});return p;});
  Object.defineProperty(plugins,'length',{get:native(()=>pluginItems.length,'get length')});
  pluginItems.forEach((p,i)=>Object.defineProperty(plugins,i,{value:p,enumerable:true}));
  Object.defineProperty(plugins,'item',{value:native(i=>pluginItems[i]||null,'item')});
  Object.defineProperty(plugins,'namedItem',{value:native(name=>pluginItems.find(p=>p.name===name)||null,'namedItem')});
  Object.defineProperty(plugins,'refresh',{value:native(()=>{},'refresh')});
  const blobs=new Map();let count=0;
  w.URL.createObjectURL=native(function(blob){const key='blob:'+w.location.origin+'/'+(++count);blobs.set(key,blob);return key;},'createObjectURL');
  w.URL.revokeObjectURL=native(function(key){blobs.delete(key);},'revokeObjectURL');
  class Worker {
    constructor(url){
      this.listeners=[];
      const blob=blobs.get(url);
      if(!blob)throw new Error('Unsupported worker URL '+url);
      const impl=Object.getOwnPropertySymbols(blob).find(s=>s.description==='impl');
      const code=blob[impl]._buffer.toString('utf8');
      const scope={navigator:w.navigator,console:{log(){},error(){}},Intl:w.Intl,OffscreenCanvas,
        postMessage:(data)=>setTimeout(()=>{const e={data};if(this.onmessage)this.onmessage(e);for(const f of this.listeners)f(e);},0)};
      scope.self=scope;vm.createContext(scope);
      setTimeout(()=>{try{vm.runInContext(code,scope,{timeout:5000});}catch(e){if(input.trace)process.stderr.write('Worker: '+e.stack+'\n');if(this.onerror)this.onerror(e);}},0);
    }
    addEventListener(name,fn){if(name==='message')this.listeners.push(fn);}
    removeEventListener(name,fn){this.listeners=this.listeners.filter(f=>f!==fn);}
    terminate(){}
    postMessage(){}
  }
  w.Worker=native(Worker,'Worker');
  for(const name of ['addEventListener','removeEventListener','terminate','postMessage'])native(Worker.prototype[name],name);
  function webgl(){return {getExtension:native(function(name){return name==='WEBGL_debug_renderer_info'?{UNMASKED_VENDOR_WEBGL:37445,UNMASKED_RENDERER_WEBGL:37446}:null;},'getExtension'),getParameter:native(function(p){return p===37445?'Google Inc. (Google)':p===37446?'ANGLE (Google, Vulkan 1.3.0 (SwiftShader Device (Subzero)), SwiftShader driver)':0;},'getParameter')};}
  class OffscreenCanvas {
    constructor(width,height){this.width=width;this.height=height;}
    getContext(type){return type.includes('webgl')?webgl():null;}
  }
  w.OffscreenCanvas=native(OffscreenCanvas,'OffscreenCanvas');native(OffscreenCanvas.prototype.getContext,'getContext');
  w.HTMLCanvasElement.prototype.getContext=native(function(type){
    if(type.includes('webgl'))return webgl();
    const context={canvas:this,measureText:native(()=>({width:100}),'measureText'),getImageData:native(()=>({data:new w.Uint8ClampedArray(4)}),'getImageData')};
    for(const k of ['fillRect','fillText','drawImage','beginPath','closePath','stroke','arc','moveTo','lineTo','clearRect','save','restore'])context[k]=native(function(){},k);
    return context;
  },'getContext');
  w.HTMLCanvasElement.prototype.toDataURL=native(function(){return 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=';},'toDataURL');
  for(const [key,value] of Object.entries({width:1920,height:1080,availWidth:1920,availHeight:1040,colorDepth:24,pixelDepth:24}))Object.defineProperty(w.screen,key,{get:native(()=>value,'get '+key),configurable:true});
  for(const [key,value] of Object.entries({innerWidth:1280,innerHeight:720,outerWidth:1280,outerHeight:800,devicePixelRatio:1}))Object.defineProperty(w,key,{value,configurable:true});
};
