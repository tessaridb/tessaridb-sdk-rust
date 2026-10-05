//! The shared value corpus, run against this client.
//!
//! # Why this is worth more than the other two test files
//!
//! `codec.rs` and `wire.rs` check this client against this author's reading of
//! the specification. They cannot catch a *consistent* misreading — one where the
//! encoder and the decoder are wrong in the same way, and every round trip
//! therefore passes.
//!
//! The corpus is produced by a **second implementation**, in another language,
//! written from the specification alone. When the two agree byte for byte, the
//! agreement is evidence that the document says enough for an independent reader
//! to arrive at the same bytes — which is the property the specification exists
//! to have, and which no amount of self-testing can demonstrate.
//!
//! It is still not the strongest tier. Two readers of one document can both be
//! wrong about what the node does; only the node settles that.
//!
//! The reader for the corpus notation lives in [`support`], shared with
//! `query_corpus.rs`.

// Test assertions are exactly where a panic is the correct outcome; the lints
// these turn off target production paths, where a panic is a defect.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]

mod support;

use serde_json::Value as Json;
use tessaridb_client::codec::{decode, encode};
use tessaridb_client::{Error, Redirect, Settlement};

use crate::support::{hex_to_bytes, read_corpus, same, value_of};

#[test]
fn every_corpus_vector_encodes_and_decodes_exactly() {
    let document: Json = read_corpus("values-v1.json");

    assert_eq!(
        document["protocol_major"].as_u64(),
        Some(u64::from(tessaridb_client::protocol::MAJOR)),
        "this client implements a different major from the corpus"
    );

    let cases = document["cases"].as_array().expect("the corpus has cases");
    assert!(
        cases.len() >= 50,
        "the corpus shrank to {} cases — a suite cannot get stronger by losing vectors",
        cases.len()
    );

    let mut checked = 0_usize;
    for case in cases {
        let name = case["name"].as_str().expect("every case is named");
        let expected = hex_to_bytes(case["bytes"].as_str().expect("every case has bytes"));
        let value = value_of(&case["value"]);

        // Forwards: this client must produce exactly the corpus's bytes.
        let produced = encode(&value);
        assert_eq!(
            produced,
            expected,
            "case {name}: this client encoded {} but the corpus says {}",
            hex(&produced),
            hex(&expected)
        );

        // Backwards: the corpus's bytes must decode to exactly the value. Not
        // implied by the forward check — an encoder and a decoder can agree with
        // each other and both disagree with the document.
        let decoded = decode(&expected)
            .unwrap_or_else(|why| panic!("case {name}: the corpus bytes did not decode: {why}"));
        assert!(
            same(&decoded, &value),
            "case {name}: decoded {decoded:?}, corpus says {value:?}"
        );

        checked = checked.saturating_add(1);
    }

    assert_eq!(checked, cases.len(), "every case must have been checked");
    println!("conformance: {checked} vectors, both directions");
}

/// `frames-v1.json`: every `Elsewhere` body (protocol §3.12) decodes to exactly
/// its `decoded`, and every `malformed` one is refused as malformed.
#[test]
fn every_elsewhere_vector_decodes_exactly_or_is_refused() {
    let corpus = read_corpus("frames-v1.json");
    let cases = corpus["elsewhere"].as_array().expect("an elsewhere list");
    assert!(cases.len() >= 8, "the corpus holds every case it should");
    for case in cases {
        let name = case["name"].as_str().unwrap_or_default();
        let decoded =
            Redirect::decode(&hex_to_bytes(case["body_hex"].as_str().unwrap_or_default()));
        if case.get("malformed").is_some() {
            assert!(
                matches!(decoded, Err(Error::Malformed)),
                "{name}: {decoded:?}"
            );
            continue;
        }
        let want = &case["decoded"];
        let redirect = decoded.unwrap_or_else(|why| panic!("{name}: {why}"));
        assert_eq!(
            hex(&redirect.node),
            want["node"].as_str().unwrap_or_default(),
            "{name}"
        );
        assert_eq!(
            redirect.epoch.to_string(),
            want["epoch"].as_str().unwrap_or_default(),
            "{name}"
        );
        let settlement = match redirect.settlement {
            Settlement::Settled => "settled",
            Settlement::Transient => "transient",
        };
        assert_eq!(
            settlement,
            want["settlement"].as_str().unwrap_or_default(),
            "{name}"
        );
        assert_eq!(
            redirect.endpoint,
            want["endpoint"].as_str().unwrap_or_default(),
            "{name}"
        );
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// `frames-v1.json`, key `refusal`: every Refusal body (protocol §3.6) reads as
/// its class — a word, `unknown` for byte 0, or none from an older node — and its
/// message, through the same reader every refusal this client receives goes
/// through.
#[test]
fn every_refusal_vector_reads_as_its_class_and_message() {
    let corpus = read_corpus("frames-v1.json");
    let cases = corpus["refusal"].as_array().expect("a refusal list");
    assert!(cases.len() >= 12, "the corpus holds every case it should");
    for case in cases {
        let name = case["name"].as_str().unwrap_or_default();
        let body = hex_to_bytes(case["body_hex"].as_str().unwrap_or_default());
        let want = &case["decoded"];
        let (class, message) = tessaridb_client::protocol::read_refusal(&body);
        assert_eq!(
            class.map(tessaridb_client::RefusalClass::word),
            want["class"].as_str(),
            "{name}"
        );
        assert_eq!(
            message,
            want["message"].as_str().unwrap_or_default(),
            "{name}"
        );
    }
}
