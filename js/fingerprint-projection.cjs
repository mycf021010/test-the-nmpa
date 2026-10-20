/** 处理运行环境投影和字段转换，为来源页面执行提供一致输入。 */
// Experimental projection of the two current VM fingerprint slots observed against Chrome.
// Cookie encryption/challenge payload remains untouched; verify on the server before use.
module.exports=function(code){
  const match=/(_\$[\w$]+)\.length=64/.exec(code);
  if(!match)return code;
  const name=match[1];
  const needle=name+'=[]';
  const replacement=name+'=window.__chromeFingerprintArray()';
  if(!code.includes(needle))throw new Error('Fingerprint array initialization missing');
  return code.replace(needle,()=>replacement);
};
