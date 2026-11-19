/// CouchDB collation order.
///
/// CouchDB (and PouchDB) use an Erlang-derived ordering for keys:
///
/// ```text
/// null < boolean < number < string < array < object
/// ```
///
/// This module provides comparison and encoding functions that match this
/// ordering, ensuring consistent behavior across local storage and remote
/// CouchDB instances.
use serde_json::Value;
use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// Type ranking
// ---------------------------------------------------------------------------

/// Numeric type rank matching CouchDB collation.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Number(_) => 3,
        Value::String(_) => 4,
        Value::Array(_) => 5,
        Value::Object(_) => 6,
    }
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

/// Compare two JSON values using CouchDB collation order.
///
/// Strings compare by UTF-16 code units, like PouchDB (JavaScript). CouchDB
/// itself uses ICU collation for strings (e.g. `"apple" < "Banana"`), which
/// is not implemented here.
///
/// Objects compare key by key in *sorted* key order: `serde_json` (without
/// its `preserve_order` feature) does not keep the document's key order,
/// which CouchDB uses (`{"b":1} < {"b":2,"a":1}` in CouchDB, the other way
/// round here). This is an accepted difference, see the book's
/// "Differences from CouchDB" page.
pub fn collate(a: &Value, b: &Value) -> Ordering {
    let rank_a = type_rank(a);
    let rank_b = type_rank(b);

    if rank_a != rank_b {
        return rank_a.cmp(&rank_b);
    }

    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => compare_numbers(a, b),
        (Value::String(a), Value::String(b)) => compare_strings(a, b),
        (Value::Array(a), Value::Array(b)) => {
            // Element-by-element, shorter arrays sort first
            for (ea, eb) in a.iter().zip(b.iter()) {
                match collate(ea, eb) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            a.len().cmp(&b.len())
        }
        (Value::Object(a), Value::Object(b)) => {
            // Key-by-key comparison in map order; fewer keys sort first.
            // CouchDB uses the document's key order, but serde_json (without
            // `preserve_order`) keeps keys sorted, so that is the order here.
            for ((ka, va), (kb, vb)) in a.iter().zip(b.iter()) {
                match compare_strings(ka, kb) {
                    Ordering::Equal => {}
                    other => return other,
                }
                match collate(va, vb) {
                    Ordering::Equal => continue,
                    other => return other,
                }
            }
            a.len().cmp(&b.len())
        }
        _ => Ordering::Equal, // Should be unreachable due to rank check
    }
}

/// Compare two strings by UTF-16 code units (JavaScript string order).
///
/// UTF-8 byte order is code point order, which only differs from UTF-16
/// order when a supplementary character (a surrogate pair in UTF-16) meets
/// a character in U+E000..=U+FFFF, so only the first differing characters
/// need to be looked at.
fn compare_strings(a: &str, b: &str) -> Ordering {
    let common = a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count();
    // The shared prefix is identical in both, so a char boundary in `a` is
    // one in `b` too.
    let mut start = common;
    while !a.is_char_boundary(start) {
        start -= 1;
    }
    match (a[start..].chars().next(), b[start..].chars().next()) {
        (Some(x), Some(y)) => utf16_units(x).cmp(&utf16_units(y)),
        (x, y) => x.is_some().cmp(&y.is_some()),
    }
}

/// The UTF-16 code units of a character, as a comparable pair.
fn utf16_units(c: char) -> (u16, u16) {
    let mut buf = [0u16; 2];
    match c.encode_utf16(&mut buf) {
        [unit] => (*unit, 0),
        [high, low] => (*high, *low),
        _ => unreachable!("a char is one or two UTF-16 units"),
    }
}

/// Compare two JSON numbers like CouchDB.
///
/// `serde_json::Number` can hold `u64`, `i64`, or `f64`. Converting straight
/// to `f64` (as a naive implementation does) collapses integers larger than
/// 2^53 onto the same float, making distinct values compare `Equal` and
/// breaking Mango `$eq`/range queries. Integers are compared via `i128`, an
/// integer and a float are compared exactly, and `-0.0` equals `0`.
///
/// With serde_json's `arbitrary_precision` (the `rouchdb/arbitrary-precision`
/// feature) a number keeps the text it was written with. CouchDB keeps an
/// integer written without `.` or an exponent exactly and reads every other
/// number as the nearest double; so does this comparison:
/// `18446744073709551616 < 18446744073709551617`, while `1.8446744073709552e19`
/// equals `18446744073709551616` and `0.1` equals
/// `0.1000000000000000055511151231257827`.
fn compare_numbers(a: &serde_json::Number, b: &serde_json::Number) -> Ordering {
    match (Num::of(a), Num::of(b)) {
        (Num::Int(ia), Num::Int(ib)) => ia.cmp(&ib),
        (Num::Int(i), Num::Big(d)) => Decimal::integer(&i.to_string()).cmp(&d),
        (Num::Big(d), Num::Int(i)) => d.cmp(&Decimal::integer(&i.to_string())),
        (Num::Big(da), Num::Big(db)) => da.cmp(&db),
        (Num::Int(i), Num::Float(f)) => compare_int_float(i, f),
        (Num::Float(f), Num::Int(i)) => compare_int_float(i, f).reverse(),
        (Num::Big(d), Num::Float(f)) => compare_big_float(&d, f),
        (Num::Float(f), Num::Big(d)) => compare_big_float(&d, f).reverse(),
        // JSON numbers are finite (only an `arbitrary_precision` literal
        // beyond the `f64` range reads as an infinity), so only -0.0 vs 0.0
        // needs care, and partial_cmp treats them as equal.
        (Num::Float(fa), Num::Float(fb)) => fa.partial_cmp(&fb).unwrap_or(Ordering::Equal),
    }
}

/// A JSON number as CouchDB compares it.
enum Num {
    Int(i128),
    /// An integer beyond `i64`/`u64` kept as written (`arbitrary_precision`).
    Big(Decimal),
    Float(f64),
}

impl Num {
    fn of(n: &serde_json::Number) -> Num {
        if let Some(i) = as_i128(n) {
            return Num::Int(i);
        }
        if arbitrary_precision() {
            let text = n.to_string();
            if !text.contains(['.', 'e', 'E']) {
                return Num::Big(Decimal::integer(&text));
            }
            // `as_f64` is `None` for a literal beyond the `f64` range.
            let overflow = if text.starts_with('-') {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
            return Num::Float(n.as_f64().unwrap_or(overflow));
        }
        Num::Float(n.as_f64().unwrap_or(0.0))
    }
}

/// Whether serde_json keeps numbers as written (`arbitrary_precision`, on
/// for the whole build through Cargo feature unification).
fn arbitrary_precision() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        serde_json::from_str::<serde_json::Number>("18446744073709551616")
            .is_ok_and(|n| n.to_string() == "18446744073709551616")
    })
}

/// The exact value of an integer: `0.d1d2d3... × 10^exponent`, with
/// `digits` free of leading and trailing zeros (empty for zero, whose sign
/// is dropped so that `-0 == 0`).
#[derive(Debug, PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: String,
    exponent: i128,
}

impl Decimal {
    /// Read an integer's text (`-?[0-9]+`).
    fn integer(text: &str) -> Decimal {
        let (negative, magnitude) = match text.strip_prefix('-') {
            Some(m) => (true, m),
            None => (false, text),
        };
        let magnitude = magnitude.trim_start_matches('0');
        let digits = magnitude.trim_end_matches('0').to_string();
        Decimal {
            negative: negative && !digits.is_empty(),
            exponent: if digits.is_empty() {
                0
            } else {
                magnitude.len() as i128
            },
            digits,
        }
    }

    fn magnitude_cmp(&self, other: &Decimal) -> Ordering {
        self.exponent
            .cmp(&other.exponent)
            .then_with(|| self.digits.cmp(&other.digits))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Decimal) -> Ordering {
        let sign = |d: &Decimal| match (d.digits.is_empty(), d.negative) {
            (true, _) => 1,
            (false, true) => 0,
            (false, false) => 2,
        };
        match sign(self).cmp(&sign(other)) {
            Ordering::Equal if self.negative => other.magnitude_cmp(self),
            Ordering::Equal => self.magnitude_cmp(other),
            unequal => unequal,
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Decimal) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Compare an integer beyond `i64`/`u64` with a float exactly (like
/// [`compare_int_float`]): an integral `f64` prints its exact value with
/// `{:.0}`.
fn compare_big_float(d: &Decimal, f: f64) -> Ordering {
    if f.is_infinite() {
        return if f > 0.0 {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    let whole = f.trunc();
    match d.cmp(&Decimal::integer(&format!("{whole:.0}"))) {
        Ordering::Equal => whole.partial_cmp(&f).unwrap_or(Ordering::Equal),
        other => other,
    }
}

fn as_i128(n: &serde_json::Number) -> Option<i128> {
    if let Some(i) = n.as_i64() {
        Some(i as i128)
    } else {
        n.as_u64().map(|u| u as i128)
    }
}

fn compare_int_float(i: i128, f: f64) -> Ordering {
    let whole = f.trunc();
    // Saturates beyond the i128 range, where the integer part alone decides.
    match i.cmp(&(whole as i128)) {
        Ordering::Equal => whole.partial_cmp(&f).unwrap_or(Ordering::Equal),
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Indexable string encoding
// ---------------------------------------------------------------------------

/// Encode a JSON value into a string whose byte order is the CouchDB
/// collation order of the values (`a.cmp(&b)` on the encodings equals
/// `collate(a, b)`), usable as a key in a sorted store.
///
/// Every encoded value starts with its type rank and ends with a `\0`
/// terminator, so an array or object that is a prefix of another sorts
/// first:
/// - Null:    `"1"`
/// - Bool:    `"2F"` / `"2T"`
/// - Number:  `"3"` + encoded number (exact, see below)
/// - String:  `"4"` + UTF-16 code units remapped so that byte order is
///   UTF-16 order, with `\0`, `\1`, `\2` escaped as in pouchdb-collate
/// - Array:   `"5"` + encoded elements
/// - Object:  `"6"` + encoded keys and values
pub fn to_indexable_string(v: &Value) -> String {
    let mut s = String::new();
    encode_value(v, &mut s);
    s
}

fn encode_value(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push('1'),
        Value::Bool(b) => {
            out.push('2');
            out.push(if *b { 'T' } else { 'F' });
        }
        Value::Number(n) => {
            out.push('3');
            encode_number(n, out);
        }
        Value::String(s) => {
            out.push('4');
            encode_string(s, out);
        }
        Value::Array(arr) => {
            out.push('5');
            for elem in arr {
                encode_value(elem, out);
            }
        }
        Value::Object(obj) => {
            out.push('6');
            for (key, value) in obj {
                out.push('4');
                encode_string(key, out);
                out.push('\0');
                encode_value(value, out);
            }
        }
    }
    out.push('\0');
}

/// Encode a string so that byte order is UTF-16 code unit order.
///
/// Units below the surrogates map to themselves; surrogates and
/// U+E000..=U+FFFF are moved above U+FFFF (in that order), so they keep
/// their relative UTF-16 order. `\0`, `\1` and `\2` are escaped to keep
/// `\0` free for the terminator.
fn encode_string(s: &str, out: &mut String) {
    for unit in s.encode_utf16() {
        let code = match unit {
            0 => {
                out.push_str("\u{1}\u{1}");
                continue;
            }
            1 => {
                out.push_str("\u{1}\u{2}");
                continue;
            }
            2 => {
                out.push_str("\u{2}\u{2}");
                continue;
            }
            0xD800..=0xDFFF => 0x1_0000 + u32::from(unit - 0xD800),
            0xE000..=0xFFFF => 0x1_0800 + u32::from(unit - 0xE000),
            _ => u32::from(unit),
        };
        out.push(char::from_u32(code).expect("remapped unit is a valid char"));
    }
}

/// Encode a number exactly, so that lexicographic order is numeric order:
/// - Zero (including `-0.0`): `1`
/// - Positive: `2` + (exponent + 500, 3 digits) + significant digits
/// - Negative: `0` + (500 - exponent, 3 digits) + nines' complement of the
///   significant digits + `:` (which sorts after every digit)
///
/// Integers (and integral floats) use their exact decimal digits; other
/// floats use the shortest representation that round-trips, which orders
/// like the float itself.
fn encode_number(n: &serde_json::Number, out: &mut String) {
    let (negative, significant, exp) = match Num::of(n) {
        Num::Int(i) => {
            let digits = i.unsigned_abs().to_string();
            let exp = digits.len() as i128 - 1;
            (i < 0, digits.trim_end_matches('0').to_string(), exp)
        }
        Num::Big(d) => (d.negative, d.digits, d.exponent - 1),
        // An `arbitrary_precision` literal beyond the `f64` range: above
        // (below) every finite number, as in `collate`.
        Num::Float(f) if f.is_infinite() => {
            out.push_str(if f > 0.0 {
                "2:9999999999999999999999999999999"
            } else {
                "0/0000000000000000000000000000000:"
            });
            return;
        }
        Num::Float(f) => {
            let digits = if f.fract() == 0.0 {
                format!("{:.0}", f.abs())
            } else {
                format!("{:e}", f.abs())
            };
            // Split into significant digits and a decimal exponent (d.ddd × 10^exp).
            let (mantissa, exp) = match digits.split_once('e') {
                Some((m, e)) => (m.replace('.', ""), e.parse::<i128>().unwrap_or(0)),
                None => (digits.clone(), digits.len() as i128 - 1),
            };
            (f < 0.0, mantissa.trim_end_matches('0').to_string(), exp)
        }
    };
    if significant.is_empty() {
        out.push('1');
        return;
    }

    if negative {
        out.push('0');
        push_exponent(500 - exp, out);
        out.extend(significant.bytes().map(|d| char::from(b'9' - (d - b'0'))));
        out.push(':');
    } else {
        out.push('2');
        push_exponent(500 + exp, out);
        out.push_str(&significant);
    }
}

/// The biased exponent of `encode_number`: three digits for every `f64`
/// (`000`..=`999`). Only `arbitrary_precision` integers with more than about
/// 500 digits go beyond; they get a fixed-width form that sorts below (`/`)
/// or above (`:`) all three-digit ones, so the encoding still sorts like
/// `collate`.
fn push_exponent(biased: i128, out: &mut String) {
    const WIDE: i128 = 1 << 100;
    match biased {
        0..=999 => out.push_str(&format!("{biased:03}")),
        b if b < 0 => out.push_str(&format!("/{:031}", WIDE + b)),
        b => out.push_str(&format!(":{b:031}")),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn type_ordering() {
        let values = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(-1),
            json!(0),
            json!(1),
            json!(1.5),
            json!(""),
            json!("a"),
            json!("b"),
            json!([]),
            json!([1]),
            json!({}),
            json!({"a": 1}),
        ];

        for i in 0..values.len() {
            for j in (i + 1)..values.len() {
                assert_eq!(
                    collate(&values[i], &values[j]),
                    Ordering::Less,
                    "{:?} should be less than {:?}",
                    values[i],
                    values[j]
                );
            }
        }
    }

    #[test]
    fn null_equality() {
        assert_eq!(collate(&json!(null), &json!(null)), Ordering::Equal);
    }

    #[test]
    fn bool_ordering() {
        assert_eq!(collate(&json!(false), &json!(true)), Ordering::Less);
        assert_eq!(collate(&json!(true), &json!(true)), Ordering::Equal);
    }

    #[test]
    fn number_ordering() {
        assert_eq!(collate(&json!(-100), &json!(-1)), Ordering::Less);
        assert_eq!(collate(&json!(0), &json!(1)), Ordering::Less);
        assert_eq!(collate(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(collate(&json!(1.5), &json!(2)), Ordering::Less);
    }

    #[test]
    fn large_integer_precision() {
        // Distinct integers beyond f64's 2^53 exact range must not collapse
        // to Equal (regression: naive as_f64 comparison loses precision).
        let a = json!(9_007_199_254_740_992_i64); // 2^53
        let b = json!(9_007_199_254_740_993_i64); // 2^53 + 1
        assert_eq!(collate(&a, &b), Ordering::Less);
        assert_eq!(collate(&b, &a), Ordering::Greater);
        assert_eq!(collate(&a, &a), Ordering::Equal);

        // u64 above i64::MAX vs a smaller value.
        let big = json!(u64::MAX);
        let small = json!(1_i64);
        assert_eq!(collate(&small, &big), Ordering::Less);

        // Mixed integer / float still orders correctly.
        assert_eq!(collate(&json!(2_i64), &json!(1.5)), Ordering::Greater);
    }

    #[test]
    fn string_ordering() {
        assert_eq!(collate(&json!("a"), &json!("b")), Ordering::Less);
        assert_eq!(collate(&json!("aa"), &json!("b")), Ordering::Less);
    }

    #[test]
    fn array_ordering() {
        assert_eq!(collate(&json!([]), &json!([1])), Ordering::Less);
        assert_eq!(collate(&json!([1]), &json!([2])), Ordering::Less);
        assert_eq!(collate(&json!([1]), &json!([1, 2])), Ordering::Less);
    }

    #[test]
    fn accepted_divergence_objects_collate_in_sorted_key_order() {
        // CouchDB 3.5.1 sorts the view keys {"b":2,"a":1} and {"b":1} as
        // [{"b":1}, {"b":2,"a":1}] (document key order). serde_json keeps
        // keys sorted, so here {"a":1,"b":2} < {"b":1}. Accepted difference
        // (book: "Differences from CouchDB"); a change must be deliberate.
        let a: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"b":1}"#).unwrap();
        assert_eq!(collate(&a, &b), Ordering::Less);
        assert_eq!(a.as_object().unwrap().keys().next().unwrap(), "a");
    }

    #[test]
    fn object_ordering() {
        assert_eq!(collate(&json!({}), &json!({"a": 1})), Ordering::Less);
        assert_eq!(collate(&json!({"a": 1}), &json!({"a": 2})), Ordering::Less);
        assert_eq!(collate(&json!({"a": 1}), &json!({"b": 1})), Ordering::Less);
    }

    #[test]
    fn indexable_string_preserves_order() {
        let values = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(0),
            json!(1),
            json!(100),
            json!("a"),
            json!("b"),
            json!([]),
            json!({}),
        ];

        let encoded: Vec<String> = values.iter().map(to_indexable_string).collect();

        for i in 0..encoded.len() {
            for j in (i + 1)..encoded.len() {
                assert!(
                    encoded[i] < encoded[j],
                    "encoded({:?}) = {:?} should be < encoded({:?}) = {:?}",
                    values[i],
                    encoded[i],
                    values[j],
                    encoded[j]
                );
            }
        }
    }

    #[test]
    fn indexable_string_negative_numbers() {
        let small = to_indexable_string(&json!(-100));
        let big = to_indexable_string(&json!(-1));
        let zero = to_indexable_string(&json!(0));
        assert!(small < big, "-100 should sort before -1");
        assert!(big < zero, "-1 should sort before 0");
    }

    #[test]
    fn negative_zero_equals_zero() {
        // F79: -0.0 and 0 are the same number.
        assert_eq!(collate(&json!(-0.0), &json!(0)), Ordering::Equal);
        assert_eq!(collate(&json!(-0.0), &json!(0.0)), Ordering::Equal);
        assert_eq!(collate(&json!(-0.0), &json!(-1)), Ordering::Greater);
    }

    #[test]
    fn integer_float_comparison_is_exact() {
        // F79: 2^53 + 1 is greater than the float 2^53.
        let int = json!(9_007_199_254_740_993_i64);
        let float = json!(9_007_199_254_740_992.0);
        assert_eq!(collate(&int, &float), Ordering::Greater);
        assert_eq!(collate(&float, &int), Ordering::Less);
        assert_eq!(collate(&json!(3), &json!(3.0)), Ordering::Equal);
        assert_eq!(collate(&json!(3), &json!(2.5)), Ordering::Greater);
        assert_eq!(collate(&json!(-3), &json!(-2.5)), Ordering::Less);
        assert_eq!(collate(&json!(u64::MAX), &json!(1e300)), Ordering::Less);
        assert_eq!(collate(&json!(i64::MIN), &json!(-1e300)), Ordering::Greater);
    }

    #[test]
    fn strings_compare_by_utf16_code_units() {
        // F78: like PouchDB (JavaScript), a supplementary character (a
        // surrogate pair) sorts before U+E000..U+FFFF.
        assert_eq!(
            collate(&json!("\u{1F600}"), &json!("\u{FF21}")),
            Ordering::Less
        );
        assert_eq!(
            collate(&json!("a\u{1F600}"), &json!("a\u{E000}")),
            Ordering::Less
        );
        assert_eq!(
            collate(&json!("\u{1F600}"), &json!("\u{1F601}")),
            Ordering::Less
        );
        assert_eq!(collate(&json!("z"), &json!("\u{E9}")), Ordering::Less);
        assert_eq!(collate(&json!("ab"), &json!("a")), Ordering::Greater);
    }

    #[test]
    fn indexable_string_matches_collate() {
        // F81: the encoding must order (and equate) values exactly like
        // `collate`.
        let values = vec![
            json!(null),
            json!(false),
            json!(true),
            json!(-1e300),
            json!(i64::MIN),
            json!(-1.9),
            json!(-1.5),
            json!(-1),
            json!(-1e-300),
            json!(-0.0),
            json!(0),
            json!(0.0),
            json!(1e-300),
            json!(1),
            json!(1.0),
            json!(1.00000000001),
            json!(9.5),
            json!(9.99999999999),
            json!(10),
            json!(100),
            json!(1_152_921_504_606_846_976_u64),
            json!(1_152_921_504_606_846_976.0),
            json!(9_007_199_254_740_992_u64),
            json!(9_007_199_254_740_992.0),
            json!(9_007_199_254_740_993_u64),
            json!(u64::MAX),
            json!(1e300),
            json!(""),
            json!("\u{0}"),
            json!("\u{1}"),
            json!("\u{2}"),
            json!("B"),
            json!("a"),
            json!("a\u{0}b"),
            json!("ab"),
            json!("b"),
            json!("\u{E9}"),
            json!("\u{D7FF}"),
            json!("\u{10000}"),
            json!("\u{1F600}"),
            json!("\u{1F601}"),
            json!("\u{10FFFF}"),
            json!("\u{E000}"),
            json!("\u{FF21}"),
            json!("\u{FFFF}"),
            json!([]),
            json!([null]),
            json!([""]),
            json!(["", 1]),
            json!(["\u{0}"]),
            json!(["\u{1}"]),
            json!(["\u{2}"]),
            json!(["\u{2}", 1]),
            json!([1]),
            json!([[1], 2]),
            json!([[1, 2]]),
            json!([1, 2]),
            json!(["a"]),
            json!({}),
            json!({"": null}),
            json!({"a": null}),
            json!({"a": 1}),
            json!({"a": 1, "b": 2}),
            json!({"a": 2}),
            json!({"a ": 1}),
            json!({"a!": 1}),
            json!({"a\u{0}": 1}),
            json!({"ab": 1}),
            json!({"b": 1}),
            json!({"\u{E000}": 1}),
            json!({"\u{1F600}": 1}),
        ];
        for a in &values {
            for b in &values {
                assert_eq!(
                    to_indexable_string(a).cmp(&to_indexable_string(b)),
                    collate(a, b),
                    "{a} vs {b}"
                );
            }
        }
    }

    #[test]
    fn indexable_string_format() {
        // The format documented on `to_indexable_string`: type rank, value,
        // `\0` terminator; \0, \1 and \2 escaped as in pouchdb-collate.
        for (value, encoded) in [
            (json!(null), "1\0"),
            (json!(false), "2F\0"),
            (json!(true), "2T\0"),
            (json!(0), "31\0"),
            (json!(-0.0), "31\0"),
            (json!(1), "325001\0"),
            (json!(1.5), "3250015\0"),
            (json!(-1), "305008:\0"),
            (json!(120), "3250212\0"),
            (
                json!("a\u{0}\u{1}\u{2}\u{3}"),
                "4a\u{1}\u{1}\u{1}\u{2}\u{2}\u{2}\u{3}\0",
            ),
            (json!([]), "5\0"),
            (json!([null, "b"]), "51\u{0}4b\0\0"),
            (json!({"a": 1}), "64a\u{0}325001\0\0"),
        ] {
            assert_eq!(to_indexable_string(&value), encoded, "{value}");
        }
    }

    #[test]
    fn decimal_reads_the_exact_integer() {
        let d = Decimal::integer;
        let parts = |t: &str| (d(t).negative, d(t).digits, d(t).exponent);
        assert_eq!(parts("120"), (false, "12".into(), 3));
        assert_eq!(parts("-0042"), (true, "42".into(), 2));
        assert_eq!(d("-0"), d("0"));
        assert_eq!(d("-2").cmp(&d("-1")), Ordering::Less);
        assert_eq!(d("1").cmp(&d("-5")), Ordering::Greater);
        assert_eq!(d("99").cmp(&d("100")), Ordering::Less);
        assert_eq!(compare_big_float(&d("4"), 3.5), Ordering::Greater);
        assert_eq!(compare_big_float(&d("-4"), -3.5), Ordering::Less);
        assert_eq!(compare_big_float(&d("3"), 3.0), Ordering::Equal);
        assert_eq!(compare_big_float(&d("3"), f64::INFINITY), Ordering::Less);
    }

    /// With `arbitrary_precision` (CI runs the suite with the
    /// `rouchdb/arbitrary-precision` feature), integers written without `.`
    /// or an exponent stay exact and every other number is the nearest
    /// `f64`, as in CouchDB, in `collate` and in the encoding. Each group
    /// holds equal values, in ascending order.
    #[test]
    fn arbitrary_precision_numbers_collate_like_couchdb() {
        if !arbitrary_precision() {
            return;
        }
        let big = |sign: &str, zeros: usize| format!("{sign}1{}", "0".repeat(zeros));
        let groups: Vec<Vec<String>> = vec![
            vec!["-1e400".into(), "-1e401".into()],
            vec![big("-", 600)],
            vec![big("-", 41)],
            vec!["-18446744073709551617".into()],
            vec![
                "-18446744073709551616".into(),
                "-1.8446744073709552e19".into(),
            ],
            vec!["0".into(), "-0".into(), "-1e-400".into(), "0.0".into()],
            vec!["0.1".into(), "0.1000000000000000055511151231257827".into()],
            vec!["1".into(), "1.00".into(), "1e0".into()],
            vec![
                "3.141592653589793238462643383279".into(),
                "3.141592653589793238462643383280".into(),
            ],
            vec!["18446744073709551615".into()],
            vec![
                "18446744073709551616".into(),
                "1.8446744073709552e19".into(),
            ],
            vec!["18446744073709551617".into()],
            vec![big("", 600)],
            vec!["1e400".into(), "1e401".into()],
        ];
        let values: Vec<(usize, Value)> = groups
            .iter()
            .enumerate()
            .flat_map(|(i, g)| g.iter().map(move |t| (i, serde_json::from_str(t).unwrap())))
            .collect();
        for (i, a) in &values {
            for (j, b) in &values {
                assert_eq!(collate(a, b), i.cmp(j), "{a} vs {b}");
                assert_eq!(
                    to_indexable_string(a).cmp(&to_indexable_string(b)),
                    i.cmp(j),
                    "encoding of {a} vs {b}"
                );
            }
        }
    }

    mod properties {
        use super::*;
        use proptest::prelude::*;

        /// Characters around every boundary of the string encoding: the
        /// escaped \0, \1 and \2, ASCII below and above the type ranks,
        /// Latin-1, both sides of the surrogate range, and supplementary
        /// characters (surrogate pairs in UTF-16).
        const CHARS: &[char] = &[
            '\u{0}',
            '\u{1}',
            '\u{2}',
            '\u{3}',
            ' ',
            '!',
            '1',
            'B',
            'a',
            'b',
            'z',
            '\u{E9}',
            '\u{F1}',
            '\u{7FF}',
            '\u{D7FF}',
            '\u{E000}',
            '\u{FF21}',
            '\u{FFFF}',
            '\u{10000}',
            '\u{1F600}',
            '\u{1F601}',
            '\u{10FFFF}',
        ];

        fn string() -> impl Strategy<Value = String> {
            prop::collection::vec(prop::sample::select(CHARS), 0..4)
                .prop_map(|chars| chars.into_iter().collect())
        }

        /// Object keys: few enough to be shared between objects, with keys
        /// that are prefixes of others.
        fn key() -> impl Strategy<Value = String> {
            prop_oneof![
                prop::sample::select(vec![
                    "",
                    "a",
                    "a ",
                    "a!",
                    "a\u{0}",
                    "ab",
                    "b",
                    "\u{E000}",
                    "\u{1F600}"
                ])
                .prop_map(String::from),
                string(),
            ]
        }

        fn number() -> impl Strategy<Value = Value> {
            prop_oneof![
                (-3i64..=3).prop_map(Value::from),
                any::<i64>().prop_map(Value::from),
                any::<u64>().prop_map(Value::from),
                // Fractions, integral floats and -0.0.
                (
                    -40i32..=40,
                    prop::sample::select(vec![1.0, 2.0, 4.0, 10.0, 3.0])
                )
                    .prop_map(|(n, d)| json!(f64::from(n) / d)),
                prop::sample::select(vec![
                    -0.0,
                    1e-300,
                    -1e-300,
                    1e300,
                    -1e300,
                    9_007_199_254_740_992.0
                ])
                .prop_map(|f| json!(f)),
                any::<f64>()
                    .prop_filter("JSON numbers are finite", |f| f.is_finite())
                    .prop_map(|f| json!(f)),
            ]
        }

        /// Nested JSON values of every type.
        fn value() -> impl Strategy<Value = Value> {
            let leaf = prop_oneof![
                Just(Value::Null),
                any::<bool>().prop_map(Value::Bool),
                number(),
                string().prop_map(Value::String),
            ];
            leaf.prop_recursive(3, 24, 4, |inner| {
                prop_oneof![
                    prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
                    prop::collection::vec((key(), inner), 0..4)
                        .prop_map(|entries| Value::Object(entries.into_iter().collect())),
                ]
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 300,
                failure_persistence: None,
                // Bound shrinking so a failure is reported in seconds (the
                // default is unbounded, which can take minutes on nested
                // values and makes mutation runs time out).
                max_shrink_iters: 1024,
                ..ProptestConfig::default()
            })]

            /// `collate` is a total order, and ordering by the indexable
            /// encoding is the same order.
            #[test]
            fn collate_is_a_total_order_matched_by_the_encoding(
                values in prop::collection::vec(value(), 1..10)
            ) {
                let encoded: Vec<String> = values.iter().map(to_indexable_string).collect();
                for (i, a) in values.iter().enumerate() {
                    prop_assert_eq!(collate(a, a), Ordering::Equal, "reflexive: {}", a);
                    for (j, b) in values.iter().enumerate() {
                        let ab = collate(a, b);
                        prop_assert_eq!(ab, collate(b, a).reverse(), "antisymmetric: {} {}", a, b);
                        prop_assert_eq!(
                            encoded[i].cmp(&encoded[j]),
                            ab,
                            "encoding of {} ({:?}) vs {} ({:?})",
                            a,
                            encoded[i],
                            b,
                            encoded[j]
                        );
                        for c in &values {
                            if ab != Ordering::Greater && collate(b, c) != Ordering::Greater {
                                prop_assert_ne!(
                                    collate(a, c),
                                    Ordering::Greater,
                                    "transitive: {} <= {} <= {}",
                                    a,
                                    b,
                                    c
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
