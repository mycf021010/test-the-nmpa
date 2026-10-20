/** 在 Node 中加载当前来源业务脚本，返回请求签名所需信息。网络请求及业务队列在 Python 中组织。 */
const fs=require('node:fs'),vm=require('node:vm'),crypto=require('node:crypto');
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const service=()=>{};service.interceptors={request:{use(){}},response:{use(){}}};
const sandbox={console:{log(){},warn(){},error(){},info(){},debug(){},trace(){}},
  axios:{create:()=>service},atob:s=>Buffer.from(s,'base64').toString('binary'),
  hex_md5:s=>crypto.createHash('md5').update(s).digest('hex'),
  getUrl:()=>null,getCookie:()=>'',setInterval(){},setTimeout(){},location:{search:''}};
sandbox.window=sandbox;vm.createContext(sandbox);
vm.runInContext(input.source,sandbox,{timeout:10000});
process.stdout.write(JSON.stringify({secret:sandbox.appSecret,vectors:input.vectors.map(params=>({params,sign:sandbox.jsonMD5ToStr(sandbox.getSign(params))}))}));
