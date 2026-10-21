// 阅读说明：既有trace诊断辅助模块，供DOM运行环境调试分支调用。
// 生成的本地诊断资料不代表器械已入库；正式入库状态以Python任务/数据库结果为准。

/** 辅助识别与描述 JavaScript 运行环境对象，供兼容代码使用。 */
module.exports=function instrument(code){
  const names=[...new Set([...code.matchAll(/\b(_\$[\w$]+)\b/g)].map(m=>m[1]))];
  const hook='window.__rsScalars=function(){const t={};'+names.map(n=>`try{if(['string','number','boolean'].includes(typeof ${n}))t[${JSON.stringify(n)}]=${n};}catch(e){}`).join('')+'return t;};'+
  'window.__rsTables=function(){const t={};const simple=(v,d)=>["string","number","boolean"].includes(typeof v)?v:d>0&&Array.isArray(v)?v.map(x=>simple(x,d-1)):null;'+names.map(n=>`try{if(Array.isArray(${n}))t[${JSON.stringify(n)}]=simple(${n},2);}catch(e){}`).join('')+'return t;};';
  return code.replace(/^(\s*\(function\([^)]*\)\{)/,(m)=>m+hook);
};
