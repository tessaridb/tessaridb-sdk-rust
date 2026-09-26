//! The value codec, against the protocol specification.
//!
//! # These are the weaker tier of evidence
//!
//! Every test here checks this client against **itself** or against the
//! specification as this author read it. That catches an inconsistent
//! implementation; it cannot catch a consistent misreading. The stronger tier is
//! exercising a running node, and it is owed.
//!
//! Which is exactly why the byte-level tests below exist. A round trip passes
//! happily when encode and decode are wrong in the same way, so the bytes
//! themselves are pinned wherever the specification pins them.

// Test assertions are exactly where a panic is the correct outcome; the lints
// these turn off target production paths, where a panic is a defect.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]
use std::collections::BTreeMap;
use std::ops::Bound;

use tessaridb_client::codec::{decode, encode};
use tessaridb_client::{
    EncodingFault, Geometry, Number, Polygon, Position, RecordId, RecordRef, Ring, Value,
    ValueRange,
};

fn round_trip(value: &Value) {
    let bytes = encode(value);
    let back = decode(&bytes).expect("a value this crate wrote should decode");
    assert_eq!(&back, value, "round trip changed the value");
}

#[test]
fn every_one_of_the_seventeen_types_round_trips() {
    let cases = vec![
        Value::None,
        Value::Null,
        Value::Bool(true),
        Value::Bool(false),
        Value::Number(Number::Integer(0)),
        Value::Number(Number::Integer(i64::MIN)),
        Value::Number(Number::Integer(i64::MAX)),
        Value::Number(Number::Float(1.5)),
        Value::Number(Number::Decimal {
            mantissa: 1234,
            scale: 2,
        }),
        Value::String("hello".to_owned()),
        Value::Bytes(vec![0, 1, 255]),
        Value::Duration {
            seconds: -5,
            nanos: 999_999_999,
        },
        Value::Datetime {
            seconds: 1_700_000_000,
            nanos: 0,
        },
        Value::Uuid([7; 16]),
        Value::Table(42),
        Value::Record(RecordRef::new(3, RecordId::Int(-1))),
        Value::Array(vec![Value::Null, Value::Bool(true)]),
        Value::Object(BTreeMap::from([
            ("b".to_owned(), Value::Null),
            ("a".to_owned(), Value::Bool(false)),
        ])),
        Value::Range(Box::new(ValueRange::new(
            Bound::Included(Value::Number(Number::Integer(1))),
            Bound::Excluded(Value::Number(Number::Integer(9))),
        ))),
        Value::Set(vec![Value::Bool(true)]),
        Value::Geometry(Geometry::Point(Position::new(2.3522, 48.8566))),
        // A polygon with a hole: the interior-ring count is the field a codec
        // written from the point case alone would forget.
        Value::Geometry(Geometry::Polygon(Polygon {
            exterior: Ring(vec![
                Position::new(0.0, 0.0),
                Position::new(1.0, 0.0),
                Position::new(1.0, 1.0),
                Position::new(0.0, 0.0),
            ]),
            interiors: vec![Ring(vec![Position::new(0.25, 0.25)])],
        })),
        // A collection inside a collection: each member carries its own kind
        // byte, which is what makes the nesting readable at all.
        Value::Geometry(Geometry::Collection(vec![Box::new(Geometry::Collection(
            vec![Box::new(Geometry::Line(vec![
                Position::new(0.0, 0.0),
                Position::new(1.0, 1.0),
            ]))],
        ))])),
        Value::Regex("^a.*z$".to_owned()),
    ];
    // A control on the case list itself: a test that silently iterated an empty
    // vector would pass and prove nothing.
    assert_eq!(cases.len(), 24, "the case list lost entries");
    for case in &cases {
        round_trip(case);
    }
}

/// The single most likely defect in any client of this protocol.
///
/// The specification's `i64` is **not** two's-complement big-endian: the top bit
/// of the first byte is inverted. A round trip cannot see this — encode and
/// decode agree with each other whichever convention they share — so the bytes
/// are asserted directly.
#[test]
fn an_integer_is_written_with_its_sign_bit_inverted() {
    // tag 0x04 = number, then 0x01 = integer, then the eight bytes.
    let one = encode(&Value::Number(Number::Integer(1)));
    assert_eq!(
        one,
        vec![0x04, 0x01, 0x80, 0, 0, 0, 0, 0, 0, 0x01],
        "an integer 1 must encode as 80 00 00 00 00 00 00 01, not plain big-endian"
    );

    let minimum = encode(&Value::Number(Number::Integer(i64::MIN)));
    assert_eq!(
        minimum,
        vec![0x04, 0x01, 0, 0, 0, 0, 0, 0, 0, 0],
        "the inversion maps i64::MIN to all-zero"
    );

    let maximum = encode(&Value::Number(Number::Integer(i64::MAX)));
    assert_eq!(
        maximum,
        vec![0x04, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        "the inversion maps i64::MAX to all-ones"
    );

    let negative = encode(&Value::Number(Number::Integer(-1)));
    assert_eq!(
        negative,
        vec![0x04, 0x01, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
        "minus one sits just below zero under the inversion"
    );
}

/// Three numeric shapes, two conventions — and this is the asymmetry.
#[test]
fn a_float_and_a_decimal_mantissa_are_written_plain() {
    // 1.0f64 is 0x3FF0_0000_0000_0000. Written as its bits, not inverted.
    let float = encode(&Value::Number(Number::Float(1.0)));
    assert_eq!(
        float,
        vec![0x04, 0x02, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0],
        "float bits are plain big-endian"
    );

    let decimal = encode(&Value::Number(Number::Decimal {
        mantissa: 1,
        scale: 0,
    }));
    let mut expected = vec![0x04, 0x03];
    expected.extend_from_slice(&1_i128.to_be_bytes());
    expected.extend_from_slice(&0_u32.to_be_bytes());
    assert_eq!(
        decimal, expected,
        "a decimal mantissa is plain big-endian, unlike an integer"
    );
}

#[test]
fn a_record_id_of_each_kind_round_trips_including_the_awkward_bytes() {
    for id in [
        RecordId::Int(-9),
        RecordId::Text("a/b".to_owned()),
        RecordId::Uuid([1; 16]),
        // A zero inside the body is what the escape exists for; a trailing one
        // is where a careless escaper terminates early.
        RecordId::Bytes(vec![0, 1, 0, 0xFF, 0]),
    ] {
        round_trip(&Value::Record(RecordRef::new(1, id)));
    }
}

#[test]
fn a_nested_value_round_trips_through_every_container() {
    let deep = Value::Array(vec![Value::Object(BTreeMap::from([(
        "inner".to_owned(),
        Value::Set(vec![Value::Range(Box::new(ValueRange::new(
            Bound::Unbounded,
            Bound::Included(Value::String("end".to_owned())),
        )))]),
    )]))]);
    round_trip(&deep);
}

#[test]
fn an_unknown_type_tag_is_refused_rather_than_guessed_at() {
    let fault = decode(&[0xEE]).expect_err("an unknown tag must not decode");
    assert_eq!(fault, EncodingFault::UnknownTag { tag: 0xEE });
}

#[test]
fn trailing_bytes_after_a_complete_value_are_an_error() {
    let mut bytes = encode(&Value::Null);
    bytes.push(0x99);
    let fault = decode(&bytes).expect_err("trailing bytes must not be ignored");
    assert_eq!(fault, EncodingFault::TrailingBytes { count: 1 });
}

#[test]
fn a_truncated_value_is_an_error_and_not_a_panic() {
    // A string that declares four bytes and supplies one.
    let bytes = vec![0x05, 0, 0, 0, 4, b'a'];
    let fault = decode(&bytes).expect_err("a short read must not decode");
    assert_eq!(fault, EncodingFault::Truncated);
}

#[test]
fn a_lying_count_costs_an_error_and_not_the_process() {
    // An array claiming four billion items, supplying none. If the decoder
    // pre-allocated on that number this test would not return.
    let bytes = vec![0x0c, 0xFF, 0xFF, 0xFF, 0xFF];
    let fault = decode(&bytes).expect_err("a lying count must not decode");
    assert_eq!(fault, EncodingFault::Truncated);
}

#[test]
fn a_deeply_nested_value_is_refused_rather_than_ending_the_stack() {
    // Two hundred nested arrays, each declaring one item. Without a depth limit
    // this recurses until the stack ends, which is a crash no caller can catch.
    let mut bytes = Vec::new();
    for _ in 0_u32..200 {
        bytes.push(0x0c);
        bytes.extend_from_slice(&1_u32.to_be_bytes());
    }
    bytes.push(0x02);
    let fault = decode(&bytes).expect_err("excessive nesting must be refused");
    assert_eq!(fault, EncodingFault::Truncated);
}

#[test]
fn an_invalid_escape_inside_a_record_id_is_named() {
    // tag record, table 1, kind text, then `00 7F` — a zero followed by neither
    // a terminator nor an escaped zero.
    let bytes = vec![0x0b, 0, 0, 0, 1, 0x02, 0x00, 0x7F];
    let fault = decode(&bytes).expect_err("an invalid escape must not decode");
    assert_eq!(fault, EncodingFault::InvalidEscape { found: 0x7F });
}

#[test]
fn a_sub_second_value_a_second_or_longer_is_refused() {
    let mut bytes = vec![0x08];
    bytes.extend_from_slice(&[0x80, 0, 0, 0, 0, 0, 0, 0]);
    bytes.extend_from_slice(&1_000_000_000_u32.to_be_bytes());
    let fault = decode(&bytes).expect_err("a whole second of nanoseconds is not a sub-second");
    assert_eq!(
        fault,
        EncodingFault::InvalidSubSecond {
            nanos: 1_000_000_000
        }
    );
}

#[test]
fn text_that_is_not_utf8_is_refused() {
    let bytes = vec![0x05, 0, 0, 0, 1, 0xFF];
    let fault = decode(&bytes).expect_err("invalid UTF-8 must not decode");
    assert_eq!(fault, EncodingFault::InvalidUtf8);
}

#[test]
fn none_and_null_are_different_bytes_and_stay_different() {
    assert_eq!(encode(&Value::None), vec![0x01]);
    assert_eq!(encode(&Value::Null), vec![0x02]);
    assert_ne!(Value::None, Value::Null);
}

/// A removed-record change body, as §3.8 lays it out, with an optional cursor.
fn change_body(cursor: Option<&str>) -> Vec<u8> {
    let mut body = 7_u64.to_be_bytes().to_vec();
    for text in ["orders", "orders:1"] {
        body.extend_from_slice(&u32::try_from(text.len()).expect("short").to_be_bytes());
        body.extend_from_slice(text.as_bytes());
    }
    body.push(1);
    if let Some(cursor) = cursor {
        body.extend_from_slice(&u32::try_from(cursor.len()).expect("short").to_be_bytes());
        body.extend_from_slice(cursor.as_bytes());
    }
    body
}

#[test]
fn a_change_from_a_split_table_carries_its_cursor_and_one_without_carries_none() {
    let plain = tessaridb_client::Change::decode(&change_body(None)).expect("a plain change");
    assert_eq!((plain.sequence, plain.cursor), (7, None));
    let split = tessaridb_client::Change::decode(&change_body(Some("0:7,2:3"))).expect("split");
    assert_eq!(split.became, tessaridb_client::Became::Removed);
    assert_eq!(split.cursor.as_deref(), Some("0:7,2:3"));
}

#[test]
fn a_follow_sends_its_cursor_last_and_only_when_it_has_one() {
    let plain = tessaridb_client::Follow::everything()
        .to_table("orders")
        .encode();
    let resumed = tessaridb_client::Follow::resuming_at("0:7,2:3")
        .to_table("orders")
        .encode();
    assert_eq!(resumed[..plain.len()], plain[..]);
    assert_eq!(&resumed[plain.len()..plain.len() + 4], &7_u32.to_be_bytes());
    assert_eq!(&resumed[plain.len() + 4..], b"0:7,2:3");
}
