#!/usr/bin/env python3
"""Offline registration conversion; no network or database writes."""
import argparse,csv,datetime,json,os,re,tempfile,uuid
from pathlib import Path
CATS=['境内医疗器械（注册）','境内医疗器械（注册历史数据）','进口医疗器械（注册）','进口医疗器械（注册历史数据）']
FIELDS='registration_no registration_no_original registrant_name registrant_address production_address product_name management_category model_specification structure_description intended_use storage_validity attachment other_content remark approval_department approval_date effective_date expiry_date change_history body_part source_file'.split()
ALIASES=[[],['注册证编号','注册证编号/备案号'],['注册人名称','注册人/备案人名称'],['注册人住所','注册人地址','住所'],['生产地址'],['产品名称','产品名称/产品分类名称'],['管理类别'],['型号规格','型号规格/包装规格'],['结构及组成','结构及组成/主要组成成分','主要组成成分'],['适用范围','适用范围/预期用途','预期用途'],['产品储存条件及有效期','储存条件及有效期'],['附件'],['其他内容'],['备注'],['审批部门'],['批准日期'],['生效日期'],['有效期至','有效期限'],['变更情况'],[],[]]
LIMITS=dict(zip(FIELDS,[32,128,255,256,1024,255,16,None,4096,2048,256,128,1024,1024,128,20,20,20,None,16,255]))
REQUIRED='registration_no registration_no_original registrant_name product_name management_category approval_date expiry_date body_part source_file'.split()
def clean(v): return str(v if v is not None else '').replace('\r\n','\n').replace('\r','\n').strip()
def atomic(p,text):
    """先写临时文件，再替换目标文件，避免留下半个导出文件。"""
    p=Path(p); p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(dir=p.parent,prefix='.'+p.name)
    try:
        with os.fdopen(fd,'w',encoding='utf-8',newline='') as f: f.write(text)
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
def writecsv(p,fields,rows):
    """按固定字段顺序写 UTF-8 CSV，供数据库导入与人工核对。"""
    import io
    s=io.StringIO(newline=''); w=csv.DictWriter(s,fieldnames=fields,quoting=csv.QUOTE_ALL);w.writeheader();w.writerows(rows);atomic(p,'\ufeff'+s.getvalue())
def date(v):
    """把来源日期规范为数据库使用的日期格式。"""
    v=clean(v)
    m=re.fullmatch(r'(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})日?',v)
    if m:
        try: return datetime.date(*map(int,m.groups())).isoformat()
        except ValueError: pass
    return v
def validate(r):
    """校验一条标准记录的必填字段、日期和长度，返回具体问题。"""
    e=[]
    for k in REQUIRED:
        if not r[k].strip(): e.append(k+': 必填为空')
    for k,n in LIMITS.items():
        if n and len(r[k])>n: e.append(k+': 超长')
    for k in ['model_specification','change_history']:
        if len(r[k].encode('utf-8'))>65535:e.append(k+': TEXT超出65535字节')
    if not re.fullmatch('[0-9]+',r['registration_no']):e.append('registration_no: 非纯数字')
    if r['registration_no']!=''.join(re.findall('[0-9]',r['registration_no_original'])):e.append('registration_no: 与完整编号不一致')
    if r['management_category'] not in ['第二类','第三类']:e.append('management_category: 非法类别')
    if r['body_part'] not in ['膝','髋','膝，髋']:e.append('body_part: 非法部位')
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
def normalized(r):
    """把来源详情映射为标准字段，并保留完整原始注册证号。"""
    o={k:clean(r[k]) for k in FIELDS};o['source_file']=json.dumps(sorted(set(json.loads(o['source_file']))),ensure_ascii=False);return o
def baseline(p):
    """读取已有数据基线，用于识别新增及字段变化。"""
    with open(p,encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=FIELDS:raise ValueError('基准CSV表头或顺序不匹配')
        out={}
        for i,r in enumerate(reader,2):
            if None in r or any(v is None for v in r.values()):raise ValueError(f'基准第{i}行列数错误')
            r={k:clean(v) for k,v in r.items()};e=validate(r);key=r['registration_no_original']
            if e or key in out:raise ValueError(f'基准第{i}行异常: {e or "重复完整证号"}')
            out[key]=r
        return out
def convert(raw,kw,cat):
    """按完整证号合并来源记录，报告冲突与无效字段。"""
    r={k:next((clean(raw[a]) for a in aliases if clean(raw.get(a))), '') for k,aliases in zip(FIELDS,ALIASES)}
    # Original identifier only trims edges; preserve its internal formatting.
    for a in ALIASES[1]:
        if raw.get(a):r['registration_no_original']=str(raw[a]).strip();break
    r['registration_no']=''.join(re.findall('[0-9]',r['registration_no_original']))
    r['management_category']={'II':'第二类','Ⅱ':'第二类','2':'第二类','二类':'第二类','III':'第三类','Ⅲ':'第三类','3':'第三类','三类':'第三类'}.get(r['management_category'],r['management_category'])
    for k in ['approval_date','effective_date','expiry_date']:r[k]=date(r[k])
    r['body_part']=kw;r['source_file']=json.dumps([cat],ensure_ascii=False);return r
def export(inputs,out,base=None,run_issues=None,categories=None):
    """输出标准记录及审计结果，供下一阶段判断能否入库。"""
    out=Path(out);out.mkdir(parents=True,exist_ok=False)
    issues=list(run_issues or []);errors=[];conflicts=[];groups={};count=0;partial=False
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
            if kw not in ['膝','髋']:issues.append(str(p)+': 不支持的部位关键词 '+str(kw))
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
                if kw not in ['膝','髋']:e.append('不支持的部位关键词')
                if not r['registration_no_original']:e.append('完整证号缺失')
                if e:errors.append({'source':source,'errors':'; '.join(e),'raw':json.dumps(raw,ensure_ascii=False)});continue
                groups.setdefault(r['registration_no_original'],[]).append((cat,r,source,raw))
    valid=[]
    for key,candidates in sorted(groups.items()):
        candidates.sort(key=lambda x:('历史' in x[0],CATS.index(x[0]),json.dumps(x[1],ensure_ascii=False,sort_keys=True),x[2]))
        merged=candidates[0][1].copy()
        for k in FIELDS[2:-2]:
            choices=[(cat,r[k],src) for cat,r,src,raw in candidates if r[k]]
            merged[k]=choices[0][1] if choices else ''
            if len(set(v for _,v,_ in choices))>1:
                conflicts.append({'registration_no_original':key,'field':k,'selected':merged[k],'candidates':json.dumps(choices,ensure_ascii=False)})
        merged['body_part']='，'.join(k for k in ['膝','髋'] if any(r['body_part']==k for _,r,_,_ in candidates))
        merged['source_file']=json.dumps([c for c in CATS if any(cat==c for cat,_,_,_ in candidates)],ensure_ascii=False)
        e=validate(merged)
        if e:
            errors.append({'source':json.dumps([s for _,_,s,_ in candidates],ensure_ascii=False),'errors':'; '.join(e),'raw':json.dumps([raw for _,_,_,raw in candidates],ensure_ascii=False)})
        else:valid.append(merged)
    new=[];changed=[];details=[];unchanged=0
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
    summary={'complete':not issues and not errors and not partial,'limited':partial,'issues':issues,'input_count':count,'merged_count':len(groups),'valid_count':len(valid),'new_count':len(new) if old is not None else None,'changed_count':len(changed) if old is not None else None,'unchanged_count':unchanged if old is not None else None,'validation_errors':len(errors),'merge_conflicts':len(conflicts),'baseline':str(base) if base else None,'baseline_error':baseline_error,'baseline_updated':False}
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
