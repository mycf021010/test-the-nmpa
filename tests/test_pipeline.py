import csv
import json
import tempfile
import unittest
from pathlib import Path

import export_medical_device as converter
import pipeline
from nmpa.storage import CSV_COLUMNS, DOMESTIC


class FakeClient:
    calls = 0
    fail_detail = False
    omit_category = False

    def __init__(self, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def categories(self):
        result = [{'itemId': DOMESTIC if i == 0 else str(i), 'itemName': cat, 'itemType': 'table'}
                  for i, cat in enumerate(converter.CATS)]
        return result[:-1] if self.omit_category else result

    def schema(self, item):
        return {'listFeild': [{'alias': 'id', 'desc': 'NMPA_DK'}],
                'detailFeild': [{'alias': 'id', 'desc': 'NMPA_DK'}] +
                [{'alias': c, 'desc': c} for c in CSV_COLUMNS if c != '注']}

    def search(self, item, keyword, page, size):
        type(self).calls += 1
        return {'data': {'pageNum': page, 'total': 1, 'list': [{'id': item + '-1'}]}}

    def detail(self, item, record_id):
        if self.fail_detail:
            raise RuntimeError('模拟详情失败')
        row = {c: '' for c in CSV_COLUMNS if c != '注'}
        row.update({'id': record_id, '注册证编号': '国械注准20263000001', '注册人名称': '测试公司',
                    '产品名称': '关节器械', '管理类别': 'III', '批准日期': '2026年1月2日',
                    '有效期至': '2030-01-02', '注册人住所': '历史补充地址' if item == '1' else ''})
        return {'data': {'detail': row}}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.batch = Path(self.tmp.name) / 'batch'
        FakeClient.calls = 0
        FakeClient.fail_detail = False
        FakeClient.omit_category = False

    def tearDown(self):
        self.tmp.cleanup()

    def collect(self, keywords=None, **kwargs):
        return pipeline.collect_batch(self.batch, keywords or ['膝', '髋'],
                                      client_factory=FakeClient, **kwargs)

    def test_full_flow_and_offline_comparison(self):
        output, summary = self.collect()
        self.assertTrue(summary['complete'])
        self.assertEqual(summary['input_count'], 8)
        self.assertEqual(summary['valid_count'], 1)
        self.assertIsNone(summary['new_count'])
        with (output / 't_medical_device.csv').open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            self.assertEqual(reader.fieldnames, converter.FIELDS)
            row = next(reader)
        self.assertEqual(row['body_part'], '膝，髋')
        self.assertEqual(row['registrant_address'], '历史补充地址')
        self.assertEqual(row['approval_date'], '2026-01-02')
        self.assertEqual(json.loads(row['source_file']), converter.CATS)
        calls = FakeClient.calls
        _, summary = pipeline.export_batch(self.batch, output / 't_medical_device.csv')
        self.assertEqual(summary['unchanged_count'], 1)
        self.assertEqual(summary['new_count'], 0)
        self.assertEqual(FakeClient.calls, calls)

    def test_empty_database_baseline_and_existing_change(self):
        base = Path(self.tmp.name) / 'baseline.csv'
        converter.writecsv(base, converter.FIELDS, [])
        output, summary = self.collect(baseline=base)
        self.assertEqual(summary['new_count'], 1)
        row = converter.baseline(output / 't_medical_device.csv').popitem()[1]
        row['registrant_name'] = '旧公司'
        converter.writecsv(base, converter.FIELDS, [row])
        _, summary = pipeline.export_batch(self.batch, base)
        self.assertEqual(summary['changed_count'], 1)
        self.assertEqual(summary['new_count'], 0)
        self.assertEqual(converter.baseline(base)[row['registration_no_original']]['registrant_name'], '旧公司')

    def test_interrupted_detail_and_resume(self):
        FakeClient.fail_detail = True
        _, summary = self.collect(['膝'])
        self.assertFalse(summary['complete'])
        self.assertEqual(json.loads((self.batch / 'batch.json').read_text())['state'], 'failed')
        self.assertGreater(summary['validation_errors'], 0)
        FakeClient.fail_detail = False
        _, summary = self.collect(['膝'])
        self.assertTrue(summary['complete'])
        self.assertEqual(FakeClient.calls, 4)  # First page survives detail failure.

    def test_limited_run_is_never_complete(self):
        _, summary = self.collect(['膝'], max_pages=1)
        self.assertFalse(summary['complete'])
        self.assertTrue(summary['limited'])

    def test_resume_identity_and_checkpoint_reuse(self):
        self.collect(['膝'])
        calls = FakeClient.calls
        self.collect(['膝'])
        self.assertEqual(FakeClient.calls, calls)
        with self.assertRaises(ValueError):
            self.collect(['髋'])

    def test_missing_category_and_concurrent_lock(self):
        FakeClient.omit_category = True
        _, summary = self.collect(['膝'])
        self.assertFalse(summary['complete'])
        (self.batch / '.pipeline.lock').write_text('123')
        with self.assertRaises(FileExistsError):
            self.collect(['膝'])


if __name__ == '__main__':
    unittest.main()
