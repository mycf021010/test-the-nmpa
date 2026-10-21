import csv,importlib.util,json,tempfile,unittest
from pathlib import Path
import export_medical_device as e
class ExportTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
 def tearDown(self):self.tmp.cleanup()
 def raw(self,no='国械注准20263130001'):
  return {'注册证编号':no,'注册人名称':'公司','产品名称':'膝髋器械,"测试"\n第二行','管理类别':'III','批准日期':'2026年1月2日','有效期至':'2030-01-02'}
 def inputs(self,entries=None,limited=False,bad=False,kw='313'):
  d=self.root/('input'+str(len(list(self.root.iterdir()))));d.mkdir()
  for cat in e.CATS:
   rows=(entries or {}).get(cat,[])
   (d/(cat+'.json')).write_text(json.dumps({'metadata':{'category':cat,'searchValue':kw,'siteTotal':len(rows)+(1 if bad else 0),'total':len(rows),'listComplete':not bad,'limitPerCategory':1 if limited else None,'detailsFail':sum('_detail_error' in r for r in rows)},'data':rows},ensure_ascii=False))
  return d
 def runexport(self,inputs,base=None):return e.export(inputs,self.root/('out'+str(len(list(self.root.iterdir())))),base)
 def test_merge_mapping_collisions(self):
  a=self.raw();h=self.raw();h['产品名称']='历史名称';h['注册人住所']='历史补充'
  d=self.inputs({e.CATS[0]:[a],e.CATS[1]:[h],e.CATS[2]:[self.raw('国械注进20263130001')],e.CATS[3]:[self.raw('国械注进20263130002')]})
  d2=self.inputs({e.CATS[0]:[a]},kw='313');s=self.runexport([d,d2]);self.assertEqual(s['valid_count'],2);self.assertGreater(s['merge_conflicts'],0)
  out=next(p for p in self.root.glob('out*'))
  with open(out/'t_medical_device.csv',encoding='utf-8-sig',newline='') as f: rows=list(csv.DictReader(f))
  self.assertNotIn(a['注册证编号'],[r['registration_no_original'] for r in rows]);self.assertTrue(all(r['body_part']=='膝，髋' for r in rows));self.assertTrue(all(r['collection_tag']=='骨科' for r in rows))
  self.assertTrue((out/'t_medical_device.csv').read_bytes().startswith(b'\xef\xbb\xbf'))
 def test_incremental(self):
  a=e.convert(self.raw(),'膝',e.CATS[0]);base=self.root/'base.csv';e.writecsv(base,e.FIELDS,[a]);d=self.inputs({e.CATS[0]:[self.raw()]});s=self.runexport([d],base);self.assertEqual(s['unchanged_count'],1)
  changed=self.raw();changed['注册人名称']='更新公司';d=self.inputs({e.CATS[0]:[changed,self.raw('国械注准20263130003')]});s=self.runexport([d],base);self.assertEqual((s['new_count'],s['changed_count']),(1,1))
 def test_errors_and_completeness(self):
  a=self.raw();a['批准日期']='2026-02-30';a['产品名称']='长'*256
  b=self.raw('国械注准20263130002');b['_detail_error']='失败'
  c=self.raw('国械注准20263130003');c['注册人名称']=''
  s=self.runexport([self.inputs({e.CATS[0]:[a,b,c]},bad=True)]);self.assertEqual(s['validation_errors'],3);self.assertFalse(s['complete'])
  s=self.runexport([self.inputs()]);self.assertTrue(s['complete']);self.assertEqual(s['valid_count'],0)
  s=self.runexport([self.inputs(limited=True)]);self.assertTrue(s['limited']);self.assertFalse(s['complete'])
  s=self.runexport([self.inputs(kw='踝')]);self.assertFalse(s['complete'])
 def test_bad_baseline(self):
  base=self.root/'bad.csv';base.write_text('wrong\n');s=self.runexport([self.inputs()],base);self.assertIsNotNone(s['baseline_error']);self.assertIsNone(s['new_count'])
  r=e.convert(self.raw(),'膝',e.CATS[0]);e.writecsv(base,e.FIELDS,[r,r]);s=self.runexport([self.inputs()],base);self.assertIsNotNone(s['baseline_error'])
 def test_stable_comparison_and_text_limit(self):
  r=e.convert(self.raw(),'膝',e.CATS[0]);r['source_file']=json.dumps(e.CATS[:2]);q=r.copy();q['source_file']=json.dumps(e.CATS[1::-1]);q['product_name']=q['product_name'].replace('\n','\r\n');self.assertEqual(e.normalized(r),e.normalized(q))
  r['model_specification']='汉'*22000;self.assertTrue(any('TEXT' in x for x in e.validate(r)))
if __name__=='__main__':unittest.main()
