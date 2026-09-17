//! Reading the shared conformance corpora.
//!
//! Extracted from `conformance.rs` when a second corpus arrived. The alternative
//! — a narrower value reader written for the query corpus — would have put two
//! JSON-to-[`Value`] readers in one repository, which is exactly the drift a
//! shared corpus exists to prevent.
//!
//! A subdirectory rather than a sibling file: `tests/*.rs` is compiled as one
//! test binary each, and a module of helpers with no tests in it would be an
//! empty binary reporting zero tests.

// Test support is exactly where a panic is the correct outcome; the lints these
// turn off target production paths, where a panic is a defect.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]
// Each `tests/*.rs` compiles this module in full, so a helper only one of them
// needs is dead code in the other. That is the cost of sharing, and it is
// cheaper than two readers.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::PathBuf;

use serde_json::Value as Json;
use tessaridb_client::query::{
    Answerer, BuildError, Create, Delete, Filter, Order, Query, Select, Update, field,
};
use tessaridb_client::{
    Geometry, Number, Parameters, Polygon, Position, RecordId, RecordRef, Ring, Value, ValueRange,
};

/// Where a named corpus lives.
///
/// The protocol is its own repository, so the directory is configurable and the
/// default assumes it sits beside this one. A missing corpus **fails** rather
/// than skipping: a conformance suite that quietly passes when it found nothing
/// to check is worse than no suite at all, because it reports coverage it does
/// not have.
pub fn corpus_path(name: &str) -> PathBuf {
    std::env::var("TESSARI_PROTOCOL_CONFORMANCE").map_or_else(
        |_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../tessaridb-protocol/conformance")
                .join(name)
        },
        |directory| PathBuf::from(directory).join(name),
    )
}

/// Read a corpus, failing loudly and with the path when it is not there.
pub fn read_corpus(name: &str) -> Json {
    let path = corpus_path(name);
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|why| {
        panic!(
            "the conformance corpus is required, not optional — a suite that \
             passes having found nothing reports coverage it does not have.\n\
             tried: {}\n{why}\n\
             set TESSARI_PROTOCOL_CONFORMANCE to the conformance directory",
            path.display()
        )
    });
    serde_json::from_str(&raw).expect("the corpus is JSON")
}

pub fn hex_to_bytes(text: &str) -> Vec<u8> {
    assert!(
        text.len().is_multiple_of(2),
        "hex must have an even length: {text}"
    );
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("hex is ascii");
            u8::from_str_radix(pair, 16).expect("valid hex")
        })
        .collect()
}

/// Build a value from the corpus's tagged-key notation.
pub fn value_of(json: &Json) -> Value {
    let object = json.as_object().expect("a value is an object");
    assert_eq!(object.len(), 1, "a value is exactly one tagged key");
    let (kind, held) = object.iter().next().expect("one entry");
    match kind.as_str() {
        "none" => Value::None,
        "null" => Value::Null,
        "bool" => Value::Bool(held.as_bool().expect("a bool")),
        "integer" => Value::Number(Number::Integer(
            held.as_str()
                .expect("an integer as text")
                .parse()
                .expect("an i64"),
        )),
        "float_bits" => {
            let bits = u64::from_str_radix(held.as_str().expect("hex bits"), 16).expect("u64 hex");
            Value::Number(Number::Float(f64::from_bits(bits)))
        }
        "decimal" => Value::Number(Number::Decimal {
            mantissa: held["mantissa"]
                .as_str()
                .expect("text")
                .parse()
                .expect("i128"),
            scale: u32::try_from(held["scale"].as_u64().expect("a scale")).expect("u32"),
        }),
        "string" => Value::String(held.as_str().expect("text").to_owned()),
        "bytes" => Value::Bytes(hex_to_bytes(held.as_str().expect("hex"))),
        "duration" => Value::Duration {
            seconds: held["seconds"]
                .as_str()
                .expect("text")
                .parse()
                .expect("i64"),
            nanos: u32::try_from(held["nanos"].as_u64().expect("nanos")).expect("u32"),
        },
        "datetime" => Value::Datetime {
            seconds: held["seconds"]
                .as_str()
                .expect("text")
                .parse()
                .expect("i64"),
            nanos: u32::try_from(held["nanos"].as_u64().expect("nanos")).expect("u32"),
        },
        "uuid" => Value::Uuid(
            <[u8; 16]>::try_from(hex_to_bytes(held.as_str().expect("hex")).as_slice())
                .expect("sixteen bytes"),
        ),
        "table" => Value::Table(u32::try_from(held.as_u64().expect("an id")).expect("u32")),
        "record" => Value::Record(RecordRef::new(
            u32::try_from(held["table"].as_u64().expect("a table")).expect("u32"),
            record_id_of(&held["id"]),
        )),
        "array" => Value::Array(
            held.as_array()
                .expect("an array")
                .iter()
                .map(value_of)
                .collect(),
        ),
        "object" => {
            let mut fields = BTreeMap::new();
            for (name, field) in held.as_object().expect("an object") {
                fields.insert(name.clone(), value_of(field));
            }
            Value::Object(fields)
        }
        "range" => Value::Range(Box::new(ValueRange::new(
            bound_of(&held["start"]),
            bound_of(&held["end"]),
        ))),
        "set" => Value::Set(
            held.as_array()
                .expect("an array")
                .iter()
                .map(value_of)
                .collect(),
        ),
        "geometry" => Value::Geometry(geometry_of(held)),
        "regex" => Value::Regex(held.as_str().expect("a pattern").to_owned()),
        other => panic!("the corpus names a value kind this client has no type for: {other}"),
    }
}

/// A coordinate, from the corpus's **bits**.
///
/// The corpus writes each coordinate as eight bytes of hex rather than as a
/// decimal literal, so nothing in this path depends on two languages agreeing
/// about how to parse `2.3522` — which they do, but which would be an
/// unnecessary thing for a byte-level corpus to rest on.
pub fn position_of(json: &Json) -> Position {
    let longitude = u64::from_str_radix(json["lon"].as_str().expect("hex bits"), 16).expect("u64");
    let latitude = u64::from_str_radix(json["lat"].as_str().expect("hex bits"), 16).expect("u64");
    Position::new(f64::from_bits(longitude), f64::from_bits(latitude))
}

pub fn positions_of(json: &Json) -> Vec<Position> {
    json.as_array()
        .expect("positions are an array")
        .iter()
        .map(position_of)
        .collect()
}

pub fn polygon_of(json: &Json) -> Polygon {
    Polygon {
        exterior: Ring(positions_of(&json["exterior"])),
        interiors: json["interiors"]
            .as_array()
            .expect("interiors are an array")
            .iter()
            .map(|ring| Ring(positions_of(ring)))
            .collect(),
    }
}

pub fn geometry_of(json: &Json) -> Geometry {
    let object = json.as_object().expect("a geometry is an object");
    assert_eq!(object.len(), 1, "a geometry is exactly one tagged key");
    let (kind, held) = object.iter().next().expect("one entry");
    match kind.as_str() {
        "point" => Geometry::Point(position_of(held)),
        "line" => Geometry::Line(positions_of(held)),
        "polygon" => Geometry::Polygon(polygon_of(held)),
        "multipoint" => Geometry::MultiPoint(positions_of(held)),
        "multiline" => Geometry::MultiLine(
            held.as_array()
                .expect("lines are an array")
                .iter()
                .map(positions_of)
                .collect(),
        ),
        "multipolygon" => Geometry::MultiPolygon(
            held.as_array()
                .expect("polygons are an array")
                .iter()
                .map(polygon_of)
                .collect(),
        ),
        "collection" => Geometry::Collection(
            held.as_array()
                .expect("a collection is an array")
                .iter()
                .map(|one| Box::new(geometry_of(one)))
                .collect(),
        ),
        other => panic!("the corpus names a shape this client has no type for: {other}"),
    }
}

pub fn record_id_of(json: &Json) -> RecordId {
    let object = json.as_object().expect("a record id is an object");
    let (kind, held) = object.iter().next().expect("one entry");
    match kind.as_str() {
        "int" => RecordId::Int(held.as_str().expect("text").parse().expect("i64")),
        "text" => RecordId::Text(held.as_str().expect("text").to_owned()),
        "uuid" => RecordId::Uuid(
            <[u8; 16]>::try_from(hex_to_bytes(held.as_str().expect("hex")).as_slice())
                .expect("sixteen bytes"),
        ),
        "bytes" => RecordId::Bytes(hex_to_bytes(held.as_str().expect("hex"))),
        other => panic!("no such record id kind: {other}"),
    }
}

pub fn bound_of(json: &Json) -> Bound<Value> {
    if json.as_str() == Some("unbounded") {
        return Bound::Unbounded;
    }
    let object = json
        .as_object()
        .expect("a bound is an object or \"unbounded\"");
    let (kind, held) = object.iter().next().expect("one entry");
    match kind.as_str() {
        "included" => Bound::Included(value_of(held)),
        "excluded" => Bound::Excluded(value_of(held)),
        other => panic!("no such bound: {other}"),
    }
}

/// Compare, treating a NaN's *bits* as the thing that must survive.
///
/// `f64::NAN != f64::NAN`, so a corpus case carrying a NaN would fail an
/// ordinary equality check even when the bytes are exactly right. What the
/// protocol promises is that the bits cross unchanged, and that is what is
/// asserted.
pub fn same(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(Number::Float(a)), Value::Number(Number::Float(b))) => {
            a.to_bits() == b.to_bits()
        }
        _ => left == right,
    }
}

// --- replaying a query corpus case through this client's builder ----------

/// Replay one corpus filter through this client's condition builder.
fn filter_of(json: &Json) -> Filter {
    let object = json.as_object().expect("a filter is an object");
    let (kind, held) = object.iter().next().expect("one entry");
    match kind.as_str() {
        "compare" => {
            let named = field(held["field"].as_str().expect("a field name"));
            let value = value_of(&held["value"]);
            match held["op"].as_str().expect("an operator") {
                "eq" => named.eq(value),
                "ne" => named.ne(value),
                "lt" => named.lt(value),
                "le" => named.le(value),
                "gt" => named.gt(value),
                "ge" => named.ge(value),
                other => panic!("the corpus names an operator this builder has not: {other}"),
            }
        }
        "and" => {
            let pair = held.as_array().expect("and takes two");
            filter_of(&pair[0]).and(filter_of(&pair[1]))
        }
        "or" => {
            let pair = held.as_array().expect("or takes two");
            filter_of(&pair[0]).or(filter_of(&pair[1]))
        }
        other => panic!("the corpus names a filter kind this builder has not: {other}"),
    }
}

fn fields_of(json: &Json) -> Vec<(String, Value)> {
    json.as_object()
        .expect("a set is an object")
        .iter()
        .map(|(name, value)| (name.clone(), value_of(value)))
        .collect()
}

/// Replay one corpus case through this client's builder.
pub fn build(json: &Json) -> Result<Query, BuildError> {
    let object = json.as_object().expect("a build is an object");
    let (kind, held) = object.iter().next().expect("one entry");
    match kind.as_str() {
        "select" => {
            let mut select = Select::from(held["from"].as_str().expect("a table"));
            if let Some(named) = held.get("fields").and_then(Json::as_array) {
                for item in named {
                    // §4.2: a projection item is a bare field name or a line
                    // window. The window carries its own counts, so it cannot
                    // be a string.
                    select = if let Some(window) = item.get("lines") {
                        select.field_lines(
                            window["field"].as_str().expect("a field name"),
                            window["start"].as_u64().expect("a start line"),
                            window["count"].as_u64().expect("a line count"),
                        )
                    } else {
                        select.field(item.as_str().expect("a field name"))
                    };
                }
            }
            if let Some(condition) = held.get("where") {
                select = select.filter(filter_of(condition));
            }
            if let Some(ordering) = held.get("order").and_then(Json::as_array) {
                for one in ordering {
                    let pair = one.as_array().expect("an ordering is a pair");
                    let direction = match pair[1].as_str().expect("a direction") {
                        "asc" => Order::Ascending,
                        "desc" => Order::Descending,
                        other => panic!("no such direction: {other}"),
                    };
                    select = select.order_by(pair[0].as_str().expect("a field name"), direction);
                }
            }
            if let Some(at) = held.get("start").and_then(Json::as_u64) {
                select = select.start(at);
            }
            if let Some(count) = held.get("limit").and_then(Json::as_u64) {
                select = select.limit(count);
            }
            if let Some(bound) = held.get("staleness").and_then(Json::as_str) {
                select = select.staleness(bound);
            }
            if let Some(word) = held.get("answered_by").and_then(Json::as_str) {
                // Through `TryFrom` rather than around it: the builder takes an
                // enumeration, so this is the one door a corpus string comes in
                // by, and it is where the corpus's refusal case is reachable.
                select = select.answered_by(Answerer::try_from(word)?);
            }
            select.build()
        }
        "create_record" => {
            let mut create = Create::record(
                held["table"].as_str().expect("a table"),
                value_of(&held["id"]),
            );
            for (name, value) in fields_of(&held["set"]) {
                create = create.set(name, value);
            }
            create.build()
        }
        "create_in_table" => {
            let mut create = Create::in_table(held["table"].as_str().expect("a table"));
            for (name, value) in fields_of(&held["set"]) {
                create = create.set(name, value);
            }
            create.build()
        }
        "update_record" => {
            let mut update = Update::record(
                held["table"].as_str().expect("a table"),
                value_of(&held["id"]),
            );
            for (name, value) in fields_of(&held["set"]) {
                update = update.set(name, value);
            }
            update.build()
        }
        "delete_record" => Delete::record(
            held["table"].as_str().expect("a table"),
            value_of(&held["id"]),
        )
        .build(),
        other => panic!("the corpus names a statement this builder has not: {other}"),
    }
}

/// The parameter map a case states, in this client's own types.
pub fn expected_parameters(json: &Json) -> Parameters {
    json.as_object()
        .expect("parameters are an object")
        .iter()
        .map(|(name, value)| (name.clone(), value_of(value)))
        .collect()
}
