//! In-memory projection of the changes feed for `_changes?filter=_selector`.
//!
//! A selector feed must look at every change after `since`; reading every
//! body makes a client that filters out a long history (e.g. a replica that
//! skips old, closed records) pay for the whole history on every request.
//! This index keeps, per current change, the document id, winning rev,
//! deleted flag and the configured top-level fields
//! ([`ServerConfig::selector_index_fields`](crate::ServerConfig::selector_index_fields)).
//! A selector is first evaluated against that projection with three-valued
//! logic: changes that cannot match are skipped without reading their body;
//! the remaining candidates are loaded and matched in full, so the results
//! are exactly those of a full scan.
//!
//! The index is built lazily (first filtered request, or
//! [`SelectorIndex::warm`]) and then refreshed incrementally from the last
//! indexed sequence. It starts over when the database's identity
//! ([`Adapter::id`](rouchdb_core::adapter::Adapter::id)) changes, i.e. after
//! the database was destroyed, whose sequences restart from zero.

use std::collections::{BTreeMap, HashMap};

use rouchdb::{Database, matches_selector};
use rouchdb_core::document::ChangesOptions;
use rouchdb_core::error::Result;
use serde_json::{Map, Value};
use tokio::sync::{Mutex, MutexGuard};

const REFRESH_CHUNK: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct IndexedChange {
    pub id: String,
    pub rev: String,
    pub deleted: bool,
    fields: Map<String, Value>,
}

#[derive(Default)]
pub struct Inner {
    /// Identity of the database the entries belong to.
    db_id: Option<String>,
    upto: u64,
    by_seq: BTreeMap<u64, IndexedChange>,
    seq_of: HashMap<String, u64>,
}

impl Inner {
    /// Last indexed sequence.
    pub fn upto(&self) -> u64 {
        self.upto
    }
}

pub struct SelectorIndex {
    /// Projected top-level fields; `_deleted` is always projected.
    fields: Vec<String>,
    inner: Mutex<Inner>,
}

impl SelectorIndex {
    /// An index projecting `fields` (top-level document fields). Selectors
    /// constraining other fields are answered by reading the body.
    pub fn new(fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut fields: Vec<String> = fields.into_iter().map(Into::into).collect();
        if !fields.iter().any(|f| f == "_deleted") {
            fields.push("_deleted".into());
        }
        Self {
            fields,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Changes after `since` whose projection may match `selector`, in seq
    /// order.
    pub fn candidates(
        &self,
        inner: &Inner,
        since: u64,
        selector: &Value,
    ) -> Vec<(u64, IndexedChange)> {
        inner
            .by_seq
            .range(since.saturating_add(1)..)
            .filter(|(_, change)| self.eval(change, selector) != Tri::False)
            .map(|(seq, change)| (*seq, change.clone()))
            .collect()
    }

    /// Brings the index up to the database's current changes and returns it
    /// locked.
    pub async fn refreshed(&self, db: &Database) -> Result<MutexGuard<'_, Inner>> {
        let mut inner = self.inner.lock().await;
        let db_id = db.adapter().id().await?;
        if inner.db_id.as_deref() != Some(db_id.as_str()) {
            *inner = Inner {
                db_id: Some(db_id),
                ..Default::default()
            };
        }
        loop {
            let page = db
                .adapter()
                .changes(ChangesOptions {
                    since: inner.upto.into(),
                    limit: Some(REFRESH_CHUNK),
                    include_docs: true,
                    ..Default::default()
                })
                .await?;
            let scanned = page.results.len() as u64;
            for event in page.results {
                let seq = event.seq.as_num();
                if let Some(old) = inner.seq_of.insert(event.id.clone(), seq) {
                    inner.by_seq.remove(&old);
                }
                let mut fields = Map::new();
                if let Some(Value::Object(doc)) = &event.doc {
                    for key in &self.fields {
                        if let Some(value) = doc.get(key) {
                            fields.insert(key.clone(), value.clone());
                        }
                    }
                }
                if event.deleted {
                    fields.insert("_deleted".into(), Value::Bool(true));
                }
                let rev = event
                    .changes
                    .first()
                    .map(|c| c.rev.clone())
                    .unwrap_or_default();
                inner.by_seq.insert(
                    seq,
                    IndexedChange {
                        id: event.id,
                        rev,
                        deleted: event.deleted,
                        fields,
                    },
                );
                inner.upto = inner.upto.max(seq);
            }
            if scanned < REFRESH_CHUNK {
                return Ok(inner);
            }
        }
    }

    /// Forgets every indexed change (e.g. right after the database was
    /// destroyed; a changed identity is also detected on the next refresh).
    pub async fn reset(&self) {
        *self.inner.lock().await = Inner::default();
    }

    /// Builds the index ahead of the first filtered request.
    pub async fn warm(&self, db: &Database) {
        let _ = self.refreshed(db).await;
    }

    /// Three-valued Mango evaluation over the projection: `False` only when
    /// the full document cannot match.
    fn eval(&self, change: &IndexedChange, selector: &Value) -> Tri {
        let Value::Object(map) = selector else {
            return Tri::Unknown;
        };
        let mut result = Tri::True;
        for (key, condition) in map {
            let part = match key.as_str() {
                "$and" => match condition {
                    Value::Array(items) => items
                        .iter()
                        .fold(Tri::True, |acc, item| and(acc, self.eval(change, item))),
                    _ => Tri::Unknown,
                },
                "$or" => match condition {
                    Value::Array(items) => items
                        .iter()
                        .fold(Tri::False, |acc, item| or(acc, self.eval(change, item))),
                    _ => Tri::Unknown,
                },
                "_id" => definite(Some(&Value::String(change.id.clone())), key, condition),
                field if self.fields.iter().any(|f| f == field) => {
                    definite(change.fields.get(field), key, condition)
                }
                _ => Tri::Unknown,
            };
            result = and(result, part);
            if result == Tri::False {
                return Tri::False;
            }
        }
        result
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    False,
    True,
    Unknown,
}

fn and(a: Tri, b: Tri) -> Tri {
    match (a, b) {
        (Tri::False, _) | (_, Tri::False) => Tri::False,
        (Tri::True, Tri::True) => Tri::True,
        _ => Tri::Unknown,
    }
}

fn or(a: Tri, b: Tri) -> Tri {
    match (a, b) {
        (Tri::True, _) | (_, Tri::True) => Tri::True,
        (Tri::False, Tri::False) => Tri::False,
        _ => Tri::Unknown,
    }
}

fn definite(value: Option<&Value>, key: &str, condition: &Value) -> Tri {
    if let Some(answer) = quick_match(value, condition) {
        return if answer { Tri::True } else { Tri::False };
    }
    let mut doc = Map::new();
    if let Some(value) = value {
        doc.insert(key.to_string(), value.clone());
    }
    let mut single = Map::new();
    single.insert(key.to_string(), condition.clone());
    if matches_selector(&Value::Object(doc), &Value::Object(single)) {
        Tri::True
    } else {
        Tri::False
    }
}

/// Allocation-free answer for the common operators on strings; `None` defers
/// to `matches_selector`. Ordering is decided only for ASCII strings, where
/// byte order equals the collation order.
fn quick_match(value: Option<&Value>, condition: &Value) -> Option<bool> {
    let ops = match condition {
        Value::Object(ops) => ops,
        Value::String(expected) => {
            return Some(matches!(value, Some(Value::String(v)) if v == expected));
        }
        _ => return None,
    };
    let mut result = true;
    for (op, operand) in ops {
        let answer = match (op.as_str(), operand) {
            ("$exists", Value::Bool(expected)) => value.is_some() == *expected,
            ("$eq", Value::String(expected)) => {
                matches!(value, Some(Value::String(v)) if v == expected)
            }
            ("$in", Value::Array(options)) if options.iter().all(Value::is_string) => {
                matches!(value, Some(v @ Value::String(_)) if options.contains(v))
            }
            ("$gt" | "$gte" | "$lt" | "$lte", Value::String(bound)) => {
                let Some(Value::String(v)) = value else {
                    return None;
                };
                if !v.is_ascii() || !bound.is_ascii() {
                    return None;
                }
                let order = v.as_str().cmp(bound.as_str());
                match op.as_str() {
                    "$gt" => order.is_gt(),
                    "$gte" => order.is_ge(),
                    "$lt" => order.is_lt(),
                    _ => order.is_le(),
                }
            }
            _ => return None,
        };
        result &= answer;
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn change(id: &str, fields: Value) -> IndexedChange {
        IndexedChange {
            id: id.into(),
            rev: "1-a".into(),
            deleted: false,
            fields: fields.as_object().unwrap().clone(),
        }
    }

    #[test]
    fn projection_decides_projected_fields_and_defers_the_rest() {
        let index = SelectorIndex::new(["kind", "day", "createdAt"]);
        let eval = |c: &IndexedChange, s: &Value| index.eval(c, s);
        let event = change("event:1", json!({"kind": "events", "day": "day-1"}));
        assert_eq!(eval(&event, &json!({"kind": "products"})), Tri::False);
        assert_eq!(eval(&event, &json!({"kind": "events"})), Tri::True);
        assert_eq!(
            eval(&event, &json!({"kind": "events", "payload.x": 1})),
            Tri::Unknown
        );
        assert_eq!(
            eval(
                &event,
                &json!({"$or": [{"kind": "day_events", "payload.x": 1}, {"day": "day-2"}]})
            ),
            Tri::False
        );
        assert_eq!(
            eval(&event, &json!({"kind": {"$exists": false}})),
            Tri::False
        );
        assert_eq!(
            eval(
                &change("x", json!({})),
                &json!({"kind": {"$exists": false}})
            ),
            Tri::True
        );
        assert_eq!(eval(&event, &json!({"$nor": []})), Tri::Unknown);
        let dated = change(
            "k",
            json!({"kind": "tickets", "createdAt": "2026-11-10T08:00:00.000Z"}),
        );
        assert_eq!(
            eval(
                &dated,
                &json!({"createdAt": {"$gte": "2026-11-10T07:55:00.000Z"}})
            ),
            Tri::True
        );
        assert_eq!(
            eval(
                &dated,
                &json!({"createdAt": {"$gte": "2026-11-10T08:00:00.001Z"}})
            ),
            Tri::False
        );
        assert_eq!(
            eval(&dated, &json!({"kind": {"$in": ["orders", "tickets"]}})),
            Tri::True
        );
        // A field that is not projected is never decided from the projection.
        assert_eq!(eval(&dated, &json!({"name": "x"})), Tri::Unknown);
    }

    /// Only changes the projection cannot rule out are candidates (their
    /// bodies are read); a selector on an unprojected field keeps them all.
    #[tokio::test]
    async fn candidates_skip_changes_the_projection_rules_out() {
        let db = Database::memory("db");
        for i in 0..5 {
            db.put(&format!("e{i}"), json!({"kind": "events"}))
                .await
                .unwrap();
        }
        db.put("p", json!({"kind": "products"})).await.unwrap();
        let index = SelectorIndex::new(["kind"]);
        let inner = index.refreshed(&db).await.unwrap();
        assert_eq!(inner.upto(), 6);
        let found = index.candidates(&inner, 0, &json!({"kind": "products"}));
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1.id, "p");
        assert!(
            index
                .candidates(&inner, 6, &json!({"kind": "products"}))
                .is_empty()
        );
        assert_eq!(index.candidates(&inner, 0, &json!({"name": "x"})).len(), 6);
    }
}
