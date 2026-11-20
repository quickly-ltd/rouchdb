#!/usr/bin/env python3
"""Yalnız bu betiğin oluşturduğu geçici redb dosyasında çalışır."""
import base64, json, os, pathlib, re, subprocess, tempfile, time, urllib.request, urllib.error
ROOT = pathlib.Path(__file__).resolve().parents[4]
BIN = ROOT / 'target/debug/rouchdb-server'
results = []
with tempfile.TemporaryDirectory(prefix='rouchdb-audit-http-') as tmp:
    logfile = pathlib.Path(tmp) / 'server.log'
    with logfile.open('w') as log:
        proc = subprocess.Popen([str(BIN), str(pathlib.Path(tmp)/'probe.redb'), '--port', '0', '--db-name', 'audit'], stdout=log, stderr=log)
        try:
            for _ in range(200):
                text = logfile.read_text()
                match = re.search(r'listening on http://127.0.0.1:(\d+)', text)
                if match: break
                if proc.poll() is not None: raise RuntimeError(text)
                time.sleep(.05)
            else: raise RuntimeError('server startup timed out')
            url = 'http://127.0.0.1:' + match[1]
            def call(method, path, body=None, extra=None):
                headers = {'Content-Type': 'application/json', **(extra or {})}
                data = json.dumps(body).encode() if body is not None else None
                req = urllib.request.Request(url+path, data=data, headers=headers, method=method)
                try:
                    with urllib.request.urlopen(req, timeout=5) as resp:
                        raw=resp.read().decode(); status=resp.status; h=dict(resp.headers)
                except urllib.error.HTTPError as e:
                    raw=e.read().decode(); status=e.code; h=dict(e.headers)
                except Exception as e:
                    return {'transport_error':str(e)}
                try: value=json.loads(raw)
                except ValueError: value=raw
                return {'status':status,'body':value,'location':h.get('location') or h.get('Location')}
            def record(name, value): results.append({'case':name, **value})
            record('seed', call('POST','/audit/_bulk_docs',{'docs':[{'_id':'a','x':1,'y':99},{'_id':'b','x':2,'y':42}]}))
            record('index_x',call('POST','/audit/_index',{'index':{'fields':['x']},'name':'same','ddoc':'first'}))
            record('index_y_same_name',call('POST','/audit/_index',{'index':{'fields':['y']},'name':'same','ddoc':'second'}))
            record('indexes_before_restart',call('GET','/audit/_index'))
            record('explain_y_before_restart',call('POST','/audit/_explain',{'selector':{'y':42}}))
            record('replace_x_with_y',call('POST','/audit/_index',{'index':{'fields':['y']},'name':'same','ddoc':'first'}))
            record('indexes_after_replace',call('GET','/audit/_index'))
            record('explain_x_after_replace',call('POST','/audit/_explain',{'selector':{'x':1}}))
            empty=call('POST','/audit/_find',{'selector':{'x':999}})
            record('empty_page',empty)
            record('reuse_empty_bookmark',call('POST','/audit/_find',{'selector':{'x':999},'bookmark':empty['body']['bookmark']}))
            bookmark=base64.urlsafe_b64encode(json.dumps({'skip':2**64-1}).encode()).decode().rstrip('=')
            record('bookmark_skip_overflow',call('POST','/audit/_find',{'selector':{},'bookmark':bookmark,'skip':1}))
            record('health_after_overflow',call('GET','/audit'))
            record('forged_forwarded_location',call('PUT','/audit/c',{'x':3},{'X-Forwarded-Host':'untrusted.invalid','X-Forwarded-Proto':'https'}))
            tomb=call('PUT','/audit/tomb',{'kind':'order','tenant':'A'})
            deleted=call('PUT','/audit/tomb',{'_rev':tomb['body']['rev'],'_deleted':True,'kind':'order','tenant':'A','deleted_at':'2026-11-14'})
            record('tombstone_delete',deleted)
            record('tombstone_stored_body',call('GET','/audit/tomb?rev='+deleted['body']['rev']))
            record('local_revision_overflow',call('PUT','/audit/_local/overflow',{'_rev':'0-18446744073709551615','x':1}))
            record('max_revision_seed',call('POST','/audit/_bulk_docs',{'new_edits':False,'docs':[{'_id':'maxrev','_rev':'18446744073709551615-abc','x':7}]}))
            record('max_revision_edit',call('PUT','/audit/maxrev',{'_rev':'18446744073709551615-abc','x':8}))
            record('changes_max_since',call('GET','/audit/_changes?since=18446744073709551615'))
            purge_doc=call('PUT','/audit/purgecase',{'x':11})
            record('purge_result',call('POST','/audit/_purge',{'purgecase':[purge_doc['body']['rev']]}))
            record('info_after_purge',call('GET','/audit'))
            record('null_index_field',call('POST','/audit/_index',{'index':{'fields':[]},'name':'empty'}))
        finally:
            proc.terminate()
            try: proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
    output={'results':results,'server_log':logfile.read_text()}
    print(json.dumps(output,ensure_ascii=False,indent=2))
