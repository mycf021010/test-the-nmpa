"""Real built Java jar + real FastAPI + synthetic worker + disposable MySQL only."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.request import Request, build_opener, ProxyHandler

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from sqlalchemy import create_engine, text
from api.config import Settings
from api.daily.store import DailyStore
from api.daily.engine import process_daily
from test_daily import DailyClient
from verify_daily_mysql import execute_sql


def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0))
        return s.getsockname()[1]


def main():
    if os.getenv('NMPA_313_DISPOSABLE_MYSQL')!='yes':
        raise SystemExit('Requires dedicated loopback MySQL13339 with explicit synthetic-test opt-in')
    root=Path(__file__).resolve().parents[3]
    admin=create_engine('mysql+pymysql://root@127.0.0.1:13339/',hide_parameters=True)
    name='nmpa313_smoke_'+uuid.uuid4().hex
    engine=create_engine('mysql+pymysql://root@127.0.0.1:13339/'+name,hide_parameters=True)
    children=[]
    try:
        with admin.begin() as db:db.execute(text('CREATE DATABASE `'+name+'` CHARACTER SET utf8mb4'))
        for file in ('CREATE_fresh_313_medical_device.sql','A001_java_daily.sql','A002_python_daily_ledger.sql'):
            execute_sql(engine,(root/'database'/file).read_text())
        with tempfile.TemporaryDirectory() as temporary:
            folder=Path(temporary);api_port=port();java_port=port()
            config={'api_key':'synthetic-313-smoke-only','data_dir':str(folder/'control'),'host':'127.0.0.1','port':api_port,
                    'mysql':{'host':'127.0.0.1','port':13339,'database':name,'username':'root','password':''},
                    'mysqldump':'/opt/homebrew/opt/mysql/bin/mysqldump','daily_enabled':True}
            settings=Settings(folder/'control',config['api_key'],str(engine.url),mysqldump=config['mysqldump'],daily_enabled=True)
            store=DailyStore(settings,migrate=True)
            path=folder/'settings.json';path.write_text(json.dumps(config))
            props=folder/'java.properties'
            props.write_text('\n'.join([
                'server.address=127.0.0.1',f'server.port={java_port}',
                f'spring.datasource.url=jdbc:mysql://127.0.0.1:13339/{name}?serverTimezone=UTC',
                'spring.datasource.username=root','spring.datasource.password=',
                'spring.sql.init.mode=never','nmpa.collector.enabled=true',
                f'nmpa.collector.base-url=http://127.0.0.1:{api_port}',
                'nmpa.collector.api-key='+config['api_key'],
                'nmpa.collector.daily.expected-instance-id='+store.instance_id,
                'nmpa.collector.daily.start-time=00:00','nmpa.collector.daily.zone=Asia/Shanghai']))
            env=dict(os.environ,NMPA_SETTINGS_FILE=str(path),PYTHONDONTWRITEBYTECODE='1')
            log=(folder/'runtime.log').open('w')
            children.append(subprocess.Popen([sys.executable,str(root/'code/fastapi/main.py')],cwd=folder,env=env,stdout=log,stderr=log))
            java=os.getenv('NMPA_TEST_JAVA','/Users/mycf/Library/Java/JavaVirtualMachines/ms-17.0.18/Contents/Home/bin/java')
            children.append(subprocess.Popen([java,'-jar',str(root/'releases/app.jar'),'--spring.config.location=file:'+str(props)],cwd=folder,stdout=log,stderr=log))
            opener=build_opener(ProxyHandler({}))
            def call(p,route,method='GET'):
                request=Request(f'http://127.0.0.1:{p}'+route,method=method,headers={'X-Local-Control':'yes'})
                with opener.open(request,timeout=5) as response:return json.load(response)
            deadline=time.monotonic()+30
            while True:
                try:
                    if any(child.poll() is not None for child in children):raise AssertionError('runtime exited')
                    call(api_port,'/health');health=call(java_port,'/local/health')
                    break
                except Exception:
                    if time.monotonic()>deadline:
                        log.flush();raise AssertionError('runtime startup failed; inspect temporary test log')
                    time.sleep(.25)
            assert health['rule_version']=='orthopedics-313-v1' and health['daily_streams']==4
            assert call(java_port,'/local/check','POST')['ready']
            assert not call(java_port,'/local/status')['running']
            call(java_port,'/local/tick','POST')
            task=call(java_port,'/local/status')['tasks'][0]
            detail=call(java_port,'/local/tasks/'+task['id'])
            assert len(detail['subtasks'])==4
            plan=json.loads(detail['task'][0]['spec_json'])
            assert all(item['keyword']=='313' for item in plan['items'])
            assert 320<=sum(item['targetRecords'] for item in plan['items'])<=400
            DailyClient.total=3;DailyClient.details=[];DailyClient.fail_after=None;DailyClient.reverse=False;DailyClient.invalid=False
            assert process_daily(store,DailyClient)
            call(java_port,'/local/tick','POST')
            result=call(java_port,'/local/tasks/'+task['id'])['subtasks'][0]
            assert result['state']=='succeeded'
            imported=json.loads(result['result_json']);assert imported['inserted']==3
            print(json.dumps({'real_jar_boot':True,'real_fastapi_boot':True,'java_python_protocol_handshake':True,
                'daily_four_313_subtasks':True,'daily_source_budget':sum(i['targetRecords'] for i in plan['items']),
                'synthetic_worker_inserted':3,'java_polled_true_result':True,'no_real_source_requests':True},indent=2))
            for child in reversed(children):child.terminate();child.wait(timeout=15)
            children=[];log.close()
    finally:
        for child in reversed(children):
            if child.poll() is None:
                child.terminate()
                try:child.wait(timeout=15)
                except subprocess.TimeoutExpired:child.kill();child.wait()
        engine.dispose()
        with admin.begin() as db:db.execute(text('DROP DATABASE IF EXISTS `'+name+'`'))
        admin.dispose()


if __name__=='__main__':main()
