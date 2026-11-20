# Differences from CouchDB

RouchDB follows CouchDB 3.5 (and PouchDB where the two disagree and RouchDB
behaves like a local PouchDB database). The differences below are known and
accepted; each is pinned by a test named `accepted_divergence_*`, so a change
to one of them has to be deliberate.

## Object key order

CouchDB keeps the members of a JSON object in the order they were written,
and that order is significant: objects collate key by key in document order,
and Mango compares them the same way. RouchDB uses `serde_json` without its
`preserve_order` feature, so every object keeps its keys **sorted**:

| | CouchDB 3.5.1 | RouchDB |
|---|---|---|
| View keys `{"b":2,"a":1}` and `{"b":1}` | `{"b":1}` first | `{"a":1,"b":2}` first |
| `{"m": {"$eq": {"a": 2, "b": 1}}}` on `{"m": {"b": 1, "a": 2}}` | no match | match |
| A document read back | members as written | members sorted |
| Mango index `map.fields` of a design document | written order | sorted (the `options.def.fields` array keeps the order) |
| Revision hash | over the document as written | over the sorted document: key order never changes a revision id |

Enabling `preserve_order` would change every `serde_json` map of the whole
dependency graph (and so every revision hash), which is why it is not done.
Revision ids are opaque in any case: the same edit gets different ids on
CouchDB and on RouchDB.

Pinned by `accepted_divergence_objects_collate_in_sorted_key_order`
(rouchdb-core collation), `accepted_divergence_object_key_order_is_not_significant`
(rouchdb-query Mango) and `accepted_divergence_rev_hash_ignores_key_order`
(rouchdb-core document).

## `all_docs` offset of the local adapters

`AllDocsResponse::offset` is, for the memory and redb adapters, the `skip`
that was applied, as in PouchDB's local adapters. CouchDB (and so the HTTP
adapter) reports the global position of the first returned row, including
the rows before `start_key`: with `a` .. `e` stored, `startkey="c"&skip=1`
returns `d`, `e` with `offset: 3` on CouchDB and `1` locally. Knowing the
global position needs a counted index that the local stores do not keep.

For a `keys` query CouchDB sends `"offset": null`: the HTTP adapter reads it
as `0`, the local adapters still report `skip`, and `rouchdb-server` sends
`null` like CouchDB.

Pinned by `accepted_divergence_all_docs_offset_is_the_skip` (memory and
redb, `adapter_conformance.rs`) and `offset_is_the_global_position_on_couchdb`
(CouchDB, `all_docs.rs`). Views (`query_view`) report CouchDB's offset.

## Numbers

By default, numbers are `serde_json` numbers: integers that fit `i64` or
`u64` are exact, other numbers are `f64` (`18446744073709551616` becomes
`1.8446744073709552e19`, `1.50` becomes `1.5`). CouchDB keeps integers of
any size exactly and rounds decimals to doubles.

With the opt-in `arbitrary-precision` feature every number keeps the text it
was written with, through storage, replication and the HTTP adapter: big
integers round-trip with CouchDB, and decimals are kept more precisely than
by CouchDB, which rewrites them as doubles (`1.50` as `1.5`, `-0.0` as
`0.0`). It turns on `serde_json`'s
`arbitrary_precision` for the whole build: `serde_json::Number` values
then compare by their text (`1.0` is not `1.00`), and revision ids of
documents holding such numbers differ from those computed without the
feature. Collation (Mango selectors, sorting, indexes, views) follows
CouchDB: an integer written without `.` or an exponent is compared exactly
(`18446744073709551617` sorts after `18446744073709551616`), every other
number as the nearest double (`1.8446744073709552e19` equals
`18446744073709551616`, `1.00` equals `1`, and two long decimals that round
to the same double are equal). A literal beyond the double range (which
CouchDB rejects) collates as an infinity. Comparing a number that is not an
`i64`/`u64` reads its text, which costs an allocation per comparison in
number-heavy sorts. Crates that depend on `rouchdb-core` directly turn the
mode on with its own `arbitrary-precision` feature. See
[Installation](../getting-started/installation.md#exact-numbers); pinned by
`big_numbers.rs`.

## Counter limits

CouchDB's revision generations, `_local` revision counters and `_find`
`skip` are Erlang integers, without an upper limit: an edit of
`18446744073709551615-…` becomes `18446744073709551616-…`. RouchDB stores
them as `u64`. A write whose new revision generation (or `_local` counter)
would pass `u64::MAX` is refused with `400 bad_request` and changes
nothing; such a revision only exists if it was replicated or written with
`new_edits: false`, and an edit of generation `u64::MAX - 1` still works. A
`_find` whose bookmark offset plus `skip` passes `u64::MAX` is a `400`, and a
`skip` or `limit` above `u64::MAX` is rejected as an invalid query (CouchDB
accepts them). Bookmarks are opaque: RouchDB's are not CouchDB's.

Pinned by `accepted_divergence_generation_stops_at_u64_max` (rouchdb-core
write) and the `accepted_divergence_*_stops_at_u64_max*` tests of
rouchdb-server (`arithmetic_bounds.rs`).

## Attachment storage

CouchDB gzip-compresses attachments of compressible types (`text/*`, JSON,
JavaScript, XML) and their `digest` is the MD5 of the compressed bytes;
RouchDB stores the bytes as they are and its `digest` is their MD5. The same
text attachment therefore has different digests on the two sides (binary
types are never compressed and match). The `encoding` and `encoded_length`
members CouchDB reports (with `att_encoding_info=true`) are kept on stubs
that carry them, but RouchDB always serves decoded bytes.
