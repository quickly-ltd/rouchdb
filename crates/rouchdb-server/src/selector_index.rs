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
//!
//! The changes feed is read outside the index lock, which is held only to
//! apply a page or to collect candidates. One refresh runs at a time; while
//! the first build of a database runs, a request does not wait for it and is
//! answered by a full scan instead.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use rouchdb::{Database, matches_selector};
use rouchdb_core::document::{ChangeEvent, ChangesOptions};
use rouchdb_core::error::Result;
use serde_json::{Map, Value};
use tokio::sync::Mutex;

const REFRESH_CHUNK: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct IndexedChange {
    pub id: String,
    pub rev: String,
    pub deleted: bool,
    fields: Map<String, Value>,
}

#[derive(Default)]
struct Inner {
    /// Identity of the database the entries belong to.
    db_id: Option<String>,
    /// Whether a refresh of this database completed at least once.
    built: bool,
    upto: u64,
    by_seq: BTreeMap<u64, IndexedChange>,
    seq_of: HashMap<String, u64>,
}

pub struct SelectorIndex {
    /// Projected top-level fields; `_deleted` is always projected.
    fields: Vec<String>,
    inner: RwLock<Inner>,
    /// Held by the one refresh reading the changes feed.
    refresh: Mutex<()>,
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
            inner: RwLock::new(Inner::default()),
            refresh: Mutex::new(()),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    /// The last indexed sequence and the changes after `since` whose
    /// projection may match `selector`, in seq order.
    pub fn candidates(&self, since: u64, selector: &Value) -> (u64, Vec<(u64, IndexedChange)>) {
        let inner = self.read();
        let found = inner
            .by_seq
            .range(since.saturating_add(1)..)
            .filter(|(_, change)| self.eval(change, selector) != Tri::False)
            .map(|(seq, change)| (*seq, change.clone()))
            .collect();
        (inner.upto, found)
    }

    /// Brings the index up to the database's current changes and returns
    /// the identity of the database it now reflects. `Ok(None)` when it
    /// cannot be used right now: the first build runs in another task
    /// (waiting for it would cost as much as a full scan), or the database
    /// was reset during the refresh.
    pub async fn refresh(&self, db: &Database) -> Result<Option<String>> {
        let db_id = db.adapter().id().await?;
        let built = {
            let inner = self.read();
            inner.built && inner.db_id.as_deref() == Some(db_id.as_str())
        };
        let _refreshing = if built {
            self.refresh.lock().await
        } else {
            match self.refresh.try_lock() {
                Ok(guard) => guard,
                Err(_) => return Ok(None),
            }
        };
        let mut upto = {
            let mut inner = self.write();
            if inner.db_id.as_deref() != Some(db_id.as_str()) {
                *inner = Inner {
                    db_id: Some(db_id.clone()),
                    ..Default::default()
                };
            }
            inner.upto
        };
        loop {
            let page = db
                .adapter()
                .changes(ChangesOptions {
                    since: upto.into(),
                    limit: Some(REFRESH_CHUNK),
                    include_docs: true,
                    ..Default::default()
                })
                .await?;
            let scanned = page.results.len() as u64;
            let entries: Vec<(u64, IndexedChange)> =
                page.results.into_iter().map(|e| self.project(e)).collect();
            let mut inner = self.write();
            // Reset (or destroyed and reused) while the page was read.
            if inner.db_id.as_deref() != Some(db_id.as_str()) || inner.upto != upto {
                return Ok(None);
            }
            for (seq, change) in entries {
                if let Some(old) = inner.seq_of.insert(change.id.clone(), seq) {
                    inner.by_seq.remove(&old);
                }
                inner.by_seq.insert(seq, change);
                inner.upto = inner.upto.max(seq);
            }
            upto = inner.upto;
            if scanned < REFRESH_CHUNK {
                inner.built = true;
                return Ok(Some(db_id));
            }
        }
    }

    fn project(&self, event: ChangeEvent) -> (u64, IndexedChange) {
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
        (
            event.seq.as_num(),
            IndexedChange {
                id: event.id,
                rev,
                deleted: event.deleted,
                fields,
            },
        )
    }

    /// Forgets every indexed change (e.g. right after the database was
    /// destroyed; a changed identity is also detected on the next refresh).
    /// A refresh running meanwhile discards what it read.
    pub async fn reset(&self) {
        *self.write() = Inner::default();
    }

    /// Builds the index ahead of the first filtered request.
    pub async fn warm(&self, db: &Database) {
        let _ = self.refresh(db).await;
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
        assert!(index.refresh(&db).await.unwrap().is_some());
        let (upto, found) = index.candidates(0, &json!({"kind": "products"}));
        assert_eq!(upto, 6);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].1.id, "p");
        assert!(
            index
                .candidates(6, &json!({"kind": "products"}))
                .1
                .is_empty()
        );
        assert_eq!(index.candidates(0, &json!({"name": "x"})).1.len(), 6);
    }

    /// A request arriving while the first build runs elsewhere is not held
    /// up by it (it falls back to a full scan); once built, a refresh only
    /// waits for another incremental refresh.
    #[tokio::test]
    async fn request_does_not_wait_for_a_running_first_build() {
        let db = Database::memory("db");
        db.put("p", json!({"kind": "products"})).await.unwrap();
        let index = SelectorIndex::new(["kind"]);
        let building = index.refresh.lock().await; // a build reading the feed
        let answer =
            tokio::time::timeout(std::time::Duration::from_millis(200), index.refresh(&db)).await;
        assert!(answer.expect("not held up by the build").unwrap().is_none());
        drop(building);
        assert!(index.refresh(&db).await.unwrap().is_some());
        assert_eq!(index.candidates(0, &json!({"kind": "products"})).1.len(), 1);
    }

    /// A reset forgets the database's entries; the next refresh rebuilds.
    #[tokio::test]
    async fn reset_forgets_and_the_next_refresh_rebuilds() {
        let db = Database::memory("db");
        db.put("p", json!({"kind": "products"})).await.unwrap();
        let index = SelectorIndex::new(["kind"]);
        assert!(index.refresh(&db).await.unwrap().is_some());
        index.reset().await;
        let (upto, found) = index.candidates(0, &json!({"name": "x"}));
        assert_eq!(upto, 0);
        assert!(found.is_empty());
        assert!(index.refresh(&db).await.unwrap().is_some());
        assert_eq!(index.candidates(0, &json!({"name": "x"})).1.len(), 1);
    }
}
