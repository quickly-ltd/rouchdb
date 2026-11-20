// PouchDB 9.0.0 ve resmî 9.0.0 find dağıtımı kullanılır.
const path = require('path');
const PouchDB = require(path.join(process.cwd(), 'node_modules/pouchdb'));
PouchDB.plugin(require(path.join(process.cwd(), 'node_modules/pouchdb-adapter-memory')));
if (!process.env.POUCHDB_FIND_DIST) throw new Error('POUCHDB_FIND_DIST must point to official 9.0.0 dist/pouchdb.find.js');
global.self = globalThis; // Resmî browser dağıtımının global alias beklentisi.
global.PouchDB = PouchDB;
require(path.resolve(process.env.POUCHDB_FIND_DIST));
delete global.PouchDB;
const output = [];
(async () => {
  const prefix = `rouchdb-online-${Date.now()}-`;
  const db = new PouchDB(prefix+'index', {adapter:'memory'});
  const src = new PouchDB(prefix+'source', {adapter:'memory'});
  const dst = new PouchDB(prefix+'target', {adapter:'memory'});
  try {
    await db.put({_id:'a',x:1,y:2});
    output.push({case:'empty_index_fields', result:await db.createIndex({index:{fields:[]},name:'empty',ddoc:'empty'})});
    output.push({case:'index_x',result:await db.createIndex({index:{fields:['x']},name:'same',ddoc:'first'})});
    output.push({case:'index_y_second_ddoc',result:await db.createIndex({index:{fields:['y']},name:'same',ddoc:'second'})});
    output.push({case:'replace_x_by_y',result:await db.createIndex({index:{fields:['y']},name:'same',ddoc:'first'})});
    output.push({case:'indexes_after_redefinition',result:await db.getIndexes()});
    await src.put({_id:'one',x:1});
    const result=await src.replicate.to(dst,{batch_size:0});
    output.push({case:'zero_batch',ok:result.ok,docs_written:result.docs_written,target_count:(await dst.info()).doc_count});
    const a=new PouchDB(prefix+'shared',{adapter:'memory'});
    const b=new PouchDB(prefix+'shared',{adapter:'memory'});
    await a.put({_id:'old',x:1});
    await a.createIndex({index:{fields:['x']}});
    await b.destroy();
    const recreated=new PouchDB(prefix+'shared',{adapter:'memory'});
    await recreated.put({_id:'new',x:1});
    try {output.push({case:'old_handle_after_destroy',result:await a.find({selector:{x:1}})});}
    catch(e) {output.push({case:'old_handle_after_destroy',error:e.message});}
    output.push({case:'new_handle_after_destroy',result:await recreated.find({selector:{x:1}})});
    await recreated.destroy();
    const rev=await db.put({_id:'tomb',tenant:'A'});
    const del=await db.put({_id:'tomb',_rev:rev.rev,_deleted:true,tenant:'A',deleted_at:'2026-11-14'});
    output.push({case:'tombstone_stored_body',result:await db.get('tomb',{rev:del.rev})});
    output.push({case:'javascript_large_integer',one:JSON.parse('18446744073709551616'),two:JSON.parse('18446744073709551617'),equal:JSON.parse('18446744073709551616')===JSON.parse('18446744073709551617')});
    console.log(JSON.stringify({pouchdb_version:PouchDB.version,results:output},null,2));
  } finally {await Promise.all([db.destroy(),src.destroy(),dst.destroy()]);}
})().catch(e=>{console.error(e);process.exitCode=1;});
