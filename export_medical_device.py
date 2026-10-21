#!/usr/bin/env python3
# 阅读说明：离线字段转换和审计核心。将来源中文字段映射成22个标准业务字段。
# 只读来源文件、写CSV/JSON报告；本文件不连接网站、不向MySQL插入数据。
# 313筛选和膝/髋推导调用api/collection_rules.py，完整证号用于合并和去重。
# 本文件只增加解释注释，业务代码保持原样。

"""Offline registration conversion; no network or database writes."""
import argparse,csv,datetime,json,os,re,tempfile,uuid
from pathlib import Path
from api.collection_rules import derive_fields, eligibility, RULE_VERSION, FIELD_SCHEMA_VERSION
CATS=['境内医疗器械（注册）','境内医疗器械（注册历史数据）','进口医疗器械（注册）','进口医疗器械（注册历史数据）']
# 固定22字段的顺序：CSV表头、SQL映射与Java协议必须一致；不是MySQL表所有列（自增id等由数据库维护）。
FIELDS='registration_no registration_no_original registrant_name registrant_address production_address product_name management_category model_specification structure_description intended_use storage_validity attachment other_content remark approval_department approval_date effective_date expiry_date change_history body_part source_file collection_tag'.split()
# 与FIELDS逐项对应的来源中文别名；每个字段选择第一个非空来源值。
ALIASES=[[],['注册证编号','注册证编号/备案号'],['注册人名称','注册人/备案人名称'],['注册人住所','注册人地址','住所'],['生产地址'],['产品名称','产品名称/产品分类名称'],['管理类别'],['型号规格','型号规格/包装规格'],['结构及组成','结构及组成/主要组成成分','主要组成成分'],['适用范围','适用范围/预期用途','预期用途'],['产品储存条件及有效期','储存条件及有效期'],['附件'],['其他内容'],['备注'],['审批部门'],['批准日期'],['生效日期'],['有效期至','有效期限'],['变更情况'],[],[],[]]
# 按SQL约束设字符长度；None代表没有此处的VARCHAR字符上限，TEXT另检查UTF-8字节数。
LIMITS=dict(zip(FIELDS,[32,128,255,256,2048,255,16,None,4096,2048,256,128,1024,1024,128,20,20,20,None,16,255,32]))
REQUIRED='registration_no registration_no_original registrant_name product_name management_category approval_date expiry_date source_file'.split()
# 仅统一换行、去首尾空白并把None变空字符串；保留业务文本内部内容，不从这里识别膝髋。
def clean(v): return str(v if v is not None else '').replace('\r\n','\n').replace('\r','\n').strip()
# 同目录临时文件写完后os.replace原子替换目标，失败清理临时文件；只保证单文件，不是跨文件事务。
def atomic(p,text):
    """先写临时文件，再替换目标文件，避免留下半个导出文件。"""
    p=Path(p); p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=p.parent,prefix='.'+p.name)
    try:
        with os.fdopen(fd,'w',encoding='utf-8',newline='') as f: f.write(text)
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
# 按给定列顺序、全字段引号和UTF-8 BOM输出，便于Excel打开中文以及后续严格表头校验。
def writecsv(p,fields,rows):
    """按固定字段顺序写 UTF-8 CSV，供数据库导入与人工核对。"""
    import io
    s=io.StringIO(newline=''); w=csv.DictWriter(s,fieldnames=fields,quoting=csv.QUOTE_ALL);w.writeheader();w.writerows(rows);atomic(p,'\ufeff'+s.getvalue())
# 识别年/月/日等可用日期并转YYYY-MM-DD；不能识别的原值保留，随后validate会报告日期错误。
def date(v):
    """把来源日期规范为数据库使用的日期格式。"""
    v=clean(v)
    m=re.fullmatch(r'(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})日?',v)
    if m:
        try: return datetime.date(*map(int,m.groups())).isoformat()
        except ValueError: pass
    return v
# 返回错误列表，检查必填、字符/字节长度、日期、纯数字证号、313和派生字段一致性；空列表才通过。
def validate(r, require_313=True):
    """校验一条标准记录的必填字段、日期和长度，返回具体问题。"""
    e=[]
    for k in REQUIRED:
        if not clean(r.get(k)): e.append(k+': 必填为空')
    for k,n in LIMITS.items():
        if n and len(r[k])>n: e.append(k+': 超长')
    for k in ['model_specification','change_history']:
        if len(r[k].encode('utf-8'))>65535:e.append(k+': TEXT超出65535字节')
    if not re.fullmatch('[0-9]+',r['registration_no']):e.append('registration_no: 非纯数字')
    if r['registration_no']!=''.join(re.findall('[0-9]',r['registration_no_original'])):e.append('registration_no: 与完整编号不一致')
    if r['management_category'] not in ['第二类','第三类']:e.append('management_category: 非法类别')
    if clean(r.get('body_part')) not in ['', '膝','髋','膝，髋']:e.append('body_part: 非法部位')
    if require_313 and eligibility(r)[0] != 'eligible':e.append('registration_no: 不符合313规则')
    derived = derive_fields(r)
    if clean(r.get('body_part')) != derived['body_part']:e.append('body_part: 与三个业务字段不一致')
    if clean(r.get('collection_tag')) != derived['collection_tag']:e.append('collection_tag: 与编号规则不一致')
    for k in ['approval_date','effective_date','expiry_date']:
        if k=='effective_date' and not r[k]:continue
        try:
            if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',r[k]):raise ValueError()
            datetime.date.fromisoformat(r[k])
        except ValueError:e.append(k+': 非法日期')
    try:
        s=json.loads(r['source_file'])
        if not isinstance(s,list) or not s or any(not isinstance(x,str) or not x for x in s):raise ValueError()
    except (ValueError,TypeError):e.append('source_file: 非法来源数组')
    return e
# 规范一条已是标准字段的记录，并对来源数组排序去重，便于基线比较；不负责中文字段映射。
def normalized(r):
    """规范已转换的标准记录及来源数组，供基线比较使用。"""
    o={k:clean(r[k]) for k in FIELDS};o['source_file']=json.dumps(sorted(set(json.loads(o['source_file']))),ensure_ascii=False);return o
# 读22列表头完全匹配的CSV并按完整证号索引；不修改基线，重复证号或字段错误直接阻止对比。
def baseline(p):
    """读取已有数据基线，用于识别新增及字段变化。"""
    with open(p,encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=FIELDS:raise ValueError('基准CSV表头或顺序不匹配')
        out={}
        for i,r in enumerate(reader,2):
            if None in r or any(v is None for v in r.values()):raise ValueError(f'基准第{i}行列数错误')
            r={k:clean(v) for k,v in r.items()};e=validate(r, require_313=False);key=r['registration_no_original']
            if e or key in out:raise ValueError(f'基准第{i}行异常: {e or "重复完整证号"}')
            out[key]=r
        return out
# 实际职责是转换一条中文来源详情：别名映射、数字证号、日期、来源和派生字段；多条合并在export/merge_candidates。
def convert(raw,kw,cat):
    """将单条来源中文详情映射为22个标准字段，并计算数字证号与派生字段。"""
    # 这是单条转换，不是多条合并；完整证号保留前缀，另提取纯数字证号用于313位置判断。
    r={k:next((clean(raw[a]) for a in aliases if clean(raw.get(a))), '') for k,aliases in zip(FIELDS,ALIASES)}
    # Original identifier only trims edges; preserve its internal formatting.
    for a in ALIASES[1]:
        if raw.get(a):r['registration_no_original']=str(raw[a]).strip();break
    r['registration_no']=''.join(re.findall('[0-9]',r['registration_no_original']))
    r['management_category']={'II':'第二类','Ⅱ':'第二类','2':'第二类','二类':'第二类','III':'第三类','Ⅲ':'第三类','3':'第三类','三类':'第三类'}.get(r['management_category'],r['management_category'])
    for k in ['approval_date','effective_date','expiry_date']:r[k]=date(r[k])
    r['source_file']=json.dumps([cat],ensure_ascii=False);return derive_fields(r)
# 读取多个类别JSON→过滤非313→同完整证号合并→冲突/字段校验→可选基线对比→输出CSV与摘要；不写库。
def export(inputs,out,base=None,run_issues=None,categories=None):
    """输出标准记录及审计结果，供下一阶段判断能否入库。"""
    out=Path(out);out.mkdir(parents=True,exist_ok=False)
    issues=list(run_issues or []);errors=[];conflicts=[];groups={};count=0;partial=False;filtered=[]
    old=None;baseline_error=None
    try:old=baseline(base) if base else None
    except (ValueError,OSError) as e:baseline_error=str(e);issues.append('基准异常: '+str(e))
    for directory in inputs:
        directory=Path(directory)
        for cat in (CATS if categories is None else categories):
            p=directory/(cat+'.json')
            if not p.exists():issues.append(str(p)+': 必需类目缺失');continue
            try:
                d=json.loads(p.read_text(encoding='utf-8'));m=d['metadata'];rows=d['data'];kw=m['searchValue']
                if not isinstance(rows,list):raise ValueError('data不是数组')
            except (ValueError,KeyError,TypeError,OSError) as e:issues.append(str(p)+': '+str(e));continue
            limited=bool(m.get('limitPerCategory'));partial|=limited
            if kw != '313':issues.append(str(p)+': 不支持的搜索词 '+str(kw))
            if m.get('category')!=cat:issues.append(str(p)+': 类目元数据不一致')
            for e in m.get('collectionErrors',[]):issues.append(str(p)+': '+str(e))
            if not m.get('listComplete',False):issues.append(str(p)+': 分页完整性未确认')
            if m.get('detailsFail',0):issues.append(str(p)+': 详情失败')
            expected=min(m.get('siteTotal',0),m['limitPerCategory']) if limited else m.get('siteTotal')
            if expected!=len(rows) or m.get('total')!=len(rows):issues.append(str(p)+': 条数不符合预期')
            for idx,raw in enumerate(rows,1):
                count+=1;source=str(p)+':'+str(idx)
                if not isinstance(raw,dict):errors.append({'source':source,'errors':'记录不是对象','raw':json.dumps(raw,ensure_ascii=False)});continue
                r=convert(raw,kw,cat)
                # Validate after merge to allow historical data to fill missing fields.
                e=['详情失败: '+str(raw['_detail_error'])] if '_detail_error' in raw else []
                if kw != '313':e.append('不支持的搜索词')
                if not r['registration_no_original']:e.append('完整证号缺失')
                if e:errors.append({'source':source,'errors':'; '.join(e),'raw':json.dumps(raw,ensure_ascii=False)});continue
                status, reason = eligibility(r)
                if status == 'filtered':
                    filtered.append({'source':source,'registration_no_original':r['registration_no_original'],'reason':reason});continue
                if status == 'invalid':
                    errors.append({'source':source,'errors':reason,'raw':json.dumps(raw,ensure_ascii=False)});continue
                groups.setdefault(r['registration_no_original'],[]).append((cat,r,source,raw))
    valid=[]
    # 同完整证号跨来源合并：非空字段互相矛盾则记录冲突并排除该证号，不任取一条入库。
    for key,candidates in sorted(groups.items()):
        conflict_start = len(conflicts)
        candidates.sort(key=lambda x:('历史' in x[0],CATS.index(x[0]),json.dumps(x[1],ensure_ascii=False,sort_keys=True),x[2]))
        merged=candidates[0][1].copy()
        for k in [field for field in FIELDS if field not in ('registration_no','registration_no_original','body_part','source_file','collection_tag')]:
            choices=[(cat,r[k],src) for cat,r,src,raw in candidates if r[k]]
            merged[k]=choices[0][1] if choices else ''
            if len(set(v for _,v,_ in choices))>1:
                conflicts.append({'registration_no_original':key,'field':k,'selected':merged[k],'candidates':json.dumps(choices,ensure_ascii=False)})
        if len(conflicts) > conflict_start:
            continue
        merged=derive_fields(merged)
        merged['source_file']=json.dumps([c for c in CATS if any(cat==c for cat,_,_,_ in candidates)],ensure_ascii=False)
        e=validate(merged)
        if e:
            errors.append({'source':json.dumps([s for _,_,s,_ in candidates],ensure_ascii=False),'errors':'; '.join(e),'raw':json.dumps([raw for _,_,_,raw in candidates],ensure_ascii=False)})
        else:valid.append(merged)
    new=[];changed=[];details=[];unchanged=0
    # 仅有可靠基线才计算new/changed/unchanged；没有基线不能把所有候选叫作数据库新增。
    if old is not None:
        for r in valid:
            key=r['registration_no_original']
            if key not in old:new.append(r);continue
            a,b=normalized(old[key]),normalized(r);diff={k:{'before':old[key][k],'after':r[k],'kind':'source' if k in ['body_part','source_file'] else 'business'} for k in FIELDS if a[k]!=b[k]}
            if diff:changed.append(r);details.append({'registration_no_original':key,'fields':diff})
            else:unchanged+=1
    writecsv(out/'t_medical_device.csv',FIELDS,valid)
    # No baseline means no claims about novelty; these files are empty with headers.
    writecsv(out/'t_medical_device_new.csv',FIELDS,new);writecsv(out/'t_medical_device_changed.csv',FIELDS,changed)
    writecsv(out/'validation_errors.csv',['source','errors','raw'],errors)
    writecsv(out/'merge_conflicts.csv',['registration_no_original','field','selected','candidates'],conflicts)
    atomic(out/'change_details.json',json.dumps(details,ensure_ascii=False,indent=2))
    atomic(out/'filtered_records.json',json.dumps(filtered,ensure_ascii=False,indent=2))
    summary={'rule_version':RULE_VERSION,'field_schema_version':FIELD_SCHEMA_VERSION,'filtered_count':len(filtered),'complete':not issues and not errors and not conflicts and not partial,'limited':partial,'issues':issues,'input_count':count,'merged_count':len(groups),'valid_count':len(valid),'new_count':len(new) if old is not None else None,'changed_count':len(changed) if old is not None else None,'unchanged_count':unchanged if old is not None else None,'validation_errors':len(errors),'merge_conflicts':len(conflicts),'baseline':str(base) if base else None,'baseline_error':baseline_error,'baseline_updated':False}
    atomic(out/'export_summary.json',json.dumps(summary,ensure_ascii=False,indent=2));return summary
if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--input',action='append',required=True);a.add_argument('--output');a.add_argument('--baseline');a.add_argument('--run-report');args=a.parse_args()
    dest=args.output or str(Path.cwd()/'exports'/('batch_'+datetime.datetime.now().strftime('%Y%m%d_%H%M%S')+'_'+uuid.uuid4().hex[:8]))
    run_issues=[]
    if args.run_report:
        try:run_issues=json.loads(Path(args.run_report).read_text(encoding='utf-8')).get('issues',[])
        except Exception as e:run_issues=['运行报告读取失败: '+str(e)]
    try:
        s=export(args.input,dest,args.baseline,run_issues);print(json.dumps({'output':dest,**s},ensure_ascii=False));raise SystemExit(2 if s['issues'] or s['validation_errors'] else 0)
    except (ValueError,OSError) as e:print('导出失败: '+str(e));raise SystemExit(1)
