#!/usr/bin/env python3
"""COUCHDB_URL ile verilen test sunucusunda kendine özel DB oluşturur/siler."""
import urllib.request, urllib.error, urllib.parse, json, base64, uuid, os
parts=urllib.parse.urlsplit(os.environ['COUCHDB_URL'])
base=urllib.parse.urlunsplit((parts.scheme,parts.netloc.rsplit('@',1)[-1],'','',''))
auth=base64.b64encode((urllib.parse.unquote(parts.username or '')+':'+urllib.parse.unquote(parts.password or '')).encode()).decode()
db='rouchdb_audit_manual_'+uuid.uuid4().hex
records=[]
def call(method,path,body=None):
    req=urllib.request.Request(base+path,method=method,headers={'Authorization':'Basic '+auth,'Content-Type':'application/json'},data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req,timeout=5) as r: return {'status':r.status,'body':json.load(r)}
    except urllib.error.HTTPError as e: return {'status':e.code,'body':json.load(e)}
try:
    records.append({'case':'server_version',**call('GET','/')})
    created=call('PUT','/'+db)
    assert created['status']==201,created
    a=call('PUT','/'+db+'/tomb',{'kind':'order','tenant':'A'})
    b=call('PUT','/'+db+'/tomb',{'_rev':a['body']['rev'],'_deleted':True,'kind':'order','tenant':'A','deleted_at':'2026-11-14'})
    records.append({'case':'tombstone_stored_body',**call('GET','/'+db+'/tomb?rev='+b['body']['rev'])})
    empty=call('POST','/'+db+'/_find',{'selector':{'kind':'missing'}})
    records.append({'case':'empty_page',**empty})
    records.append({'case':'reuse_empty_bookmark',**call('POST','/'+db+'/_find',{'selector':{'kind':'missing'},'bookmark':empty['body']['bookmark']})})
    call('POST','/'+db+'/_bulk_docs',{'docs':[{'_id':'one','n':18446744073709551616},{'_id':'two','n':18446744073709551617}]})
    records.append({'case':'big_integer_eq',**call('POST','/'+db+'/_find',{'selector':{'n':{'$eq':18446744073709551616}}})})
    print(json.dumps(records,indent=2))
finally: call('DELETE','/'+db)
