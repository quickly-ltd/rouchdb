use std::sync::Arc;
use rouchdb::*;
use serde_json::json;

fn index(name: &str, field: &str) -> IndexDefinition {
    IndexDefinition {name: name.into(),fields:vec![SortField::Simple(field.into())],ddoc:None}
}
async fn query(db: &Database, selector: serde_json::Value) -> Vec<serde_json::Value> {
    db.find(FindOptions {selector,..Default::default()}).await.unwrap().docs
}
#[tokio::main]
async fn main() {
    // Aynı adapter'ı kullanan iki yasal Database handle'ı.
    let adapter=Arc::new(MemoryAdapter::new("shared"));
    let a=Database::from_adapter(adapter.clone());
    let b=Database::from_adapter(adapter);
    a.put("old",json!({"x":1})).await.unwrap();
    a.create_index(index("x","x")).await.unwrap();
    b.destroy().await.unwrap();
    b.put("new",json!({"x":1})).await.unwrap();
    let indexed=query(&a,json!({"x":1})).await;
    let scanned=query(&b,json!({"x":1})).await;
    println!("{}",json!({"case":"shared_handle_reset","indexed":indexed,"full_scan":scanned}));
    assert!(indexed.is_empty() && scanned.len()==1);

    let src=Database::memory("source");
    let dst=Database::memory("target");
    src.put("one",json!({"x":1})).await.unwrap();
    let result=replicate(src.adapter(),dst.adapter(),ReplicationOptions {batch_size:0,..Default::default()}).await.unwrap();
    println!("{}",json!({"case":"zero_batch","ok":result.ok,"docs_read":result.docs_read,"docs_written":result.docs_written,"source_count":src.info().await.unwrap().doc_count,"target_count":dst.info().await.unwrap().doc_count,"last_seq":result.last_seq}));
    assert!(result.ok && dst.info().await.unwrap().doc_count==0);

    #[cfg(feature="arbitrary-precision")]
    {
        let db=Database::memory("numbers");
        let one:serde_json::Value=serde_json::from_str(r#"{"n":18446744073709551616}"#).unwrap();
        let two:serde_json::Value=serde_json::from_str(r#"{"n":18446744073709551617}"#).unwrap();
        db.put("one",one).await.unwrap();
        db.put("two",two).await.unwrap();
        let selector=serde_json::from_str(r#"{"n":{"$eq":18446744073709551616}}"#).unwrap();
        let docs=query(&db,selector).await;
        println!("{}",json!({"case":"big_integer_eq","docs":docs}));
        assert_eq!(docs.len(),2,"current defect: unequal numbers compare equal");
    }
    let db=Database::memory("index-fields");
    let empty=db.create_index(IndexDefinition{name:"empty".into(),fields:vec![],ddoc:None}).await;
    println!("{}",json!({"case":"empty_index","result":format!("{:?}",empty)}));
}
