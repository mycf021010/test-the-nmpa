/** 辅助识别与描述 JavaScript 运行环境对象，供兼容代码使用。 */
module.exports=function instrument(code){
  const names=[...new Set([...code.matchAll(/\b(_\$[\w$]+)\b/g)].map(m=>m[1]))];
  const hook='window.__rsScalars=function(){const t={};'+names.map(n=>`try{if(['string','number','boolean'].includes(typeof ${n}))t[${JSON.stringify(n)}]=${n};}catch(e){}`).join('')+'return t;};'+
  'window.__rsTables=function(){const t={};const simple=(v,d)=>["string","number","boolean"].includes(typeof v)?v:d>0&&Array.isArray(v)?v.map(x=>simple(x,d-1)):null;'+names.map(n=>`try{if(Array.isArray(${n}))t[${JSON.stringify(n)}]=simple(${n},2);}catch(e){}`).join('')+'return t;};';
  return code.replace(/^(\s*\(function\([^)]*\)\{)/,(m)=>m+hook);
};
