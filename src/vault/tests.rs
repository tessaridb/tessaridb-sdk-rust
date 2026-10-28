//! The vault half of the shared corpus (`vault-v1.json`), and the passphrase's
//! absence from every rendering of the types that carry it.

#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::Value as Json;

use super::call::{Act, Place, VaultStatus, body};
use super::statements::{Statements, audit, limit_refused};
use super::{Custody, SealState};
use crate::value::{Number, Value};

fn corpus() -> Json {
    let path = std::env::var("TESSARI_PROTOCOL_CONFORMANCE").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tessaridb-protocol/conformance"),
        PathBuf::from,
    );
    let path = path.join("vault-v1.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|why| panic!("the vault corpus is required at {}: {why}", path.display()));
    serde_json::from_str(&raw).unwrap()
}

fn text(json: &Json, key: &str) -> String {
    json[key].as_str().unwrap().to_owned()
}

fn parameter(json: &Json) -> Value {
    let (kind, held) = json.as_object().unwrap().iter().next().unwrap();
    let held = held.as_str().unwrap();
    match kind.as_str() {
        "string" => Value::String(held.to_owned()),
        "integer" => Value::Number(Number::Integer(held.parse().unwrap())),
        "bytes" => Value::Bytes(
            held.as_bytes()
                .chunks(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect(),
        ),
        other => panic!("a parameter kind the corpus does not define: {other}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").unwrap();
        out
    })
}

#[test]
fn every_frame_this_client_sends_is_the_corpus_bytes() {
    let mut checked = 0;
    for case in corpus()["frames"].as_array().unwrap() {
        let build = &case["build"];
        // This client never puts credentials in the frame: a vault call runs as
        // the connection's session. The cases that carry them are other clients'.
        if !build["credentials"].is_null() {
            continue;
        }
        let act = match build["act"].as_str().unwrap() {
            "status" => Act::Status,
            "unseal" => Act::Unseal(build["passphrase"].as_str().unwrap()),
            "seal" => Act::Seal,
            "change" => Act::Change {
                current: build["current"].as_str().unwrap(),
                new: build["new"].as_str().unwrap(),
            },
            "declare" => Act::Declare(build["passphrase"].as_str().unwrap()),
            other => panic!("an act the corpus does not define: {other}"),
        };
        let place = build["vault"].as_object().map(|vault| Place {
            namespace: vault["namespace"].as_str().unwrap(),
            database: vault["database"].as_str().unwrap(),
            vault: vault["vault"].as_str().unwrap(),
        });
        assert_eq!(
            hex(&body(&act, place)),
            text(case, "body_hex"),
            "{}",
            text(case, "name")
        );
        checked += 1;
    }
    assert_eq!(
        checked, 11,
        "the corpus holds eleven frames without credentials"
    );
}

/// Render one statement case, or say it was refused before sending.
fn rendered(build: &Json) -> Option<(String, Vec<(String, Value)>)> {
    let (kind, fields) = build.as_object().unwrap().iter().next().unwrap();
    let within = (
        fields["namespace"].as_str().unwrap(),
        fields["database"].as_str().unwrap(),
    );
    if kind == "audit" {
        return audit(within, fields["actor"].as_str())
            .ok()
            .map(|script| (script, Vec::new()));
    }
    let statements = Statements::new(within, fields["vault"].as_str().unwrap()).ok()?;
    let id = || parameter(&fields["id"]);
    match kind.as_str() {
        "list" => {
            let limit = fields["limit"]
                .as_u64()
                .map(|limit| u32::try_from(limit).unwrap());
            if limit_refused(limit) {
                return None;
            }
            let after = (!fields["after"].is_null()).then(|| parameter(&fields["after"]));
            Some(statements.list(after, limit))
        }
        "reveal" => {
            let named: Vec<&str> = fields["fields"]
                .as_array()
                .map(|names| names.iter().map(|name| name.as_str().unwrap()).collect())
                .unwrap_or_default();
            statements.reveal(id(), &named).ok()
        }
        "write" => {
            let written: BTreeMap<String, Value> = fields["fields"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| (name.clone(), parameter(value)))
                .collect();
            if written.is_empty() {
                return None;
            }
            statements.write(id(), &written).ok()
        }
        "recipients" => Some(statements.recipients(id())),
        "add_recipient" => {
            let key = parameter(&serde_json::json!({ "bytes": fields["key"] }));
            let Value::Bytes(key) = key else {
                unreachable!()
            };
            Some(statements.add_recipient(id(), fields["name"].as_str().unwrap(), &key))
        }
        "remove_recipient" => {
            Some(statements.remove_recipient(id(), fields["name"].as_str().unwrap()))
        }
        other => panic!("a statement the corpus does not define: {other}"),
    }
}

#[test]
fn every_statement_renders_or_is_refused_as_the_corpus_says() {
    let cases = corpus()["statements"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 19);
    for case in &cases {
        let name = text(case, "name");
        match (rendered(&case["build"]), case.get("refused")) {
            (None, Some(_)) => {}
            (Some(_), Some(refused)) => {
                panic!("{name}: rendered, but the corpus refuses it: {refused}")
            }
            (None, None) => panic!("{name}: refused, but the corpus renders it"),
            (Some((script, given)), None) => {
                assert_eq!(script, text(case, "script"), "{name}");
                let expected: BTreeMap<String, Value> = case["parameters"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(key, value)| (key.clone(), parameter(value)))
                    .collect();
                let given: BTreeMap<String, Value> = given.into_iter().collect();
                assert_eq!(given, expected, "{name}");
            }
        }
    }
}

#[test]
fn a_passphrase_is_in_no_rendering_of_the_act_that_carries_it() {
    let unseal = Act::Unseal("correct horse battery");
    let change = Act::Change {
        current: "correct horse battery",
        new: "staple 9f2b",
    };
    for rendered in [format!("{unseal:?}"), format!("{change:?}")] {
        assert!(
            !rendered.contains("horse") && !rendered.contains("staple"),
            "{rendered}"
        );
    }
}

#[test]
fn a_status_reads_its_three_states_and_the_instant_only_when_unsealed() {
    let status = |state: &str, seals_at: Option<Value>| {
        let mut fields = BTreeMap::from([
            ("state".to_owned(), Value::String(state.to_owned())),
            (
                "unseal_for".to_owned(),
                Value::Duration {
                    seconds: 600,
                    nanos: 0,
                },
            ),
        ]);
        if let Some(at) = seals_at {
            fields.insert("seals_at".to_owned(), at);
        }
        VaultStatus::from_value(Value::Object(fields)).unwrap()
    };
    assert_eq!(status("sealed", None).state, SealState::Sealed);
    assert_eq!(status("uninitialised", None).seals_at, None);
    let open = status(
        "unsealed",
        Some(Value::Datetime {
            seconds: 1_790_000_000,
            nanos: 0,
        }),
    );
    assert_eq!(open.state, SealState::Unsealed);
    assert_eq!(
        open.seals_at,
        Some(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_790_000_000))
    );
    assert_eq!(open.unseal_for, std::time::Duration::from_secs(600));
    assert!(VaultStatus::from_value(Value::String("sealed".to_owned())).is_err());
}

#[test]
fn a_vaults_status_names_its_custody_from_a_closed_set() {
    let status = |custody: Option<&str>| {
        let mut fields = BTreeMap::from([
            ("state".to_owned(), Value::String("sealed".to_owned())),
            (
                "unseal_for".to_owned(),
                Value::Duration {
                    seconds: 600,
                    nanos: 0,
                },
            ),
        ]);
        if let Some(custody) = custody {
            fields.insert("custody".to_owned(), Value::String(custody.to_owned()));
        }
        VaultStatus::from_value(Value::Object(fields))
    };
    assert_eq!(status(Some("own")).unwrap().custody, Some(Custody::Own));
    assert_eq!(status(Some("store")).unwrap().custody, Some(Custody::Store));
    assert_eq!(
        status(None).unwrap().custody,
        None,
        "the store's own status names none"
    );
    assert!(
        status(Some("shared")).is_err(),
        "a custody outside the set was read"
    );
}

fn frames() -> Json {
    let path = std::env::var("TESSARI_PROTOCOL_CONFORMANCE").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tessaridb-protocol/conformance"),
        PathBuf::from,
    );
    let path = path.join("frames-v1.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|why| panic!("the frames corpus is required at {}: {why}", path.display()));
    serde_json::from_str(&raw).unwrap()
}

fn key_from(json: &Json, field: &str) -> crate::wire::message::VaultKey {
    let bytes: Vec<u8> = json[field]
        .as_str()
        .unwrap()
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    crate::wire::message::VaultKey::new(bytes.try_into().unwrap())
}

#[test]
fn every_keyed_act_is_the_body_the_frames_corpus_names() {
    let corpus = frames();
    let mut checked = 0_usize;
    for case in corpus["keyed"].as_array().unwrap() {
        let want = &case["decoded"];
        let Some(act) = want.get("act").and_then(Json::as_str) else {
            continue;
        };
        let place = Place {
            namespace: want["namespace"].as_str().unwrap(),
            database: want["database"].as_str().unwrap(),
            vault: want["vault"].as_str().unwrap(),
        };
        let written = match act {
            "declare-keyed" => body(&Act::DeclareKeyed(&key_from(want, "key_hex")), Some(place)),
            "change-key" => body(
                &Act::ChangeKey {
                    current: &key_from(want, "current_hex"),
                    new: &key_from(want, "new_hex"),
                },
                Some(place),
            ),
            other => panic!("an act the corpus does not define: {other}"),
        };
        assert_eq!(hex(&written), text(case, "body_hex"), "{}", case["name"]);
        checked += 1;
    }
    assert_eq!(checked, 2, "both keyed acts are in the corpus");
}

#[test]
fn a_keyed_vault_reports_key_custody() {
    let mut fields = BTreeMap::new();
    fields.insert("state".to_owned(), Value::String("sealed".to_owned()));
    fields.insert(
        "unseal_for".to_owned(),
        Value::Duration {
            seconds: 600,
            nanos: 0,
        },
    );
    fields.insert("custody".to_owned(), Value::String("key".to_owned()));
    let status = VaultStatus::from_value(Value::Object(fields)).unwrap();
    assert_eq!(status.custody, Some(Custody::Key));
    assert_eq!(status.state, SealState::Sealed);
}

#[test]
fn a_key_is_never_printed() {
    let key = crate::wire::message::VaultKey::new([0x5a; 32]);
    let request = crate::wire::message::Request::new("RETURN 1;")
        .presenting(("prod", "work", "team"), key.clone());
    for rendered in [
        format!("{key:?}"),
        format!("{request:?}"),
        format!("{:?}", Act::DeclareKeyed(&key)),
    ] {
        assert!(
            !rendered.contains("90") && !rendered.contains("5a"),
            "{rendered}"
        );
    }
}
