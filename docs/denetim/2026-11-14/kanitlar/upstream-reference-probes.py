#!/usr/bin/env python3
"""Online kaynak okumasından sonra CouchDB referans davranışını sınar."""
import os,json,uuid,urllib.request,urllib.error,urllib.parse,base64,time
parts=urllib.parse.urlsplit(os.environ['COUCHDB_URL'])
base=urllib.parse.urlunsplit((parts.scheme,parts.netloc.rsplit('@',1)[-1],'','',''))
auth=base64.b64encode((urllib.parse.unquote(parts.username or '')+':'+urllib.parse.unquote(parts.password or '')).encode()).decode()
db='rouchdb_online_'+uuid.uuid4().hex
records=[]
def call(method,path,body=None,extra=None):
    req=urllib.request.Request(base+path,method=method,headers={'Authorization':'Basic '+auth,'Content-Type':'application/json',**(extra or {})},data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req,timeout=10) as r:return {'status':r.status,'body':json.load(r),'location':r.headers.get('Location')}
    except urllib.error.HTTPError as e:return {'status':e.code,'body':json.load(e)}
def rec(name,value):records.append({'case':name,**value});return value
try:
    rec('version',call('GET','/'))
    assert call('PUT','/'+db+'?q=1&n=1')['status']==201
    call('POST','/'+db+'/_bulk_docs',{'docs':[{'_id':'a','x':1,'y':99},{'_id':'b','x':2,'y':42}]})
    rec('index_x',call('POST','/'+db+'/_index',{'index':{'fields':['x']},'name':'same','ddoc':'first'}))
    rec('index_y_second_ddoc',call('POST','/'+db+'/_index',{'index':{'fields':['y']},'name':'same','ddoc':'second'}))
    rec('replace_x_by_y',call('POST','/'+db+'/_index',{'index':{'fields':['y']},'name':'same','ddoc':'first'}))
    rec('indexes_after_redefinition',call('GET','/'+db+'/_index'))
    rec('empty_index_fields',call('POST','/'+db+'/_index',{'index':{'fields':[]},'name':'empty','ddoc':'empty'}))
    rec('forwarded_location',call('PUT','/'+db+'/forwarded',{'x':1},{'X-Forwarded-Host':'untrusted.invalid','X-Forwarded-Proto':'https'}))
    rec('local_max_rev',call('PUT','/'+db+'/_local/overflow',{'_rev':'0-18446744073709551615','x':1}))
    rec('revision_max_seed',call('POST','/'+db+'/_bulk_docs',{'new_edits':False,'docs':[{'_id':'maxrev','_rev':'18446744073709551615-abc','x':7}]}))
    rec('revision_max_edit',call('PUT','/'+db+'/maxrev',{'_rev':'18446744073709551615-abc','x':8}))
    rec('find_large_skip',call('POST','/'+db+'/_find',{'selector':{'x':999},'skip':18446744073709551616}))
    created=call('PUT','/'+db+'/purgecase',{'x':11})
    rec('info_before_purge',call('GET','/'+db))
    rec('purge_result',call('POST','/'+db+'/_purge',{'purgecase':[created['body']['rev']]}))
    rec('info_after_purge',call('GET','/'+db))
    print(json.dumps(records,indent=2,ensure_ascii=False))
finally:call('DELETE','/'+db)
