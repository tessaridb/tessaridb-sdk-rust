//! The shared query corpus, run against this client's builder.
//!
//! # What this replaces, and what it does not
//!
//! G010's original criterion for the builder was `parse(render(built)) == built`
//! through the node's own parser. That proof lived in the server's workspace, and
//! LR-SDK-007 puts that workspace out of reach of every client in every language.
//!
//! This file is the offline half of the substitute: each case in
//! `queries-v1.json` names a query in a language-neutral notation, and this
//! client must render it to exactly the stated text with exactly the stated
//! parameters. The corpus is produced by a second implementation of
//! `spec/query-builder-v1.md`, so a disagreement is a disagreement between two
//! readers of a contract rather than between this file and itself.
//!
//! **It does not reach the parser.** Nothing here would notice a rendering the
//! node stopped accepting. That is `tests/node.rs`, which executes every one of
//! these cases against a running node — the two halves are separate on purpose,
//! because only one of them can run in a repository with no node to reach.

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
use tessaridb_client::Value;
use tessaridb_client::query::BuildError;

use crate::support::{build, expected_parameters, read_corpus, value_of};

/// The reason a refusal names, in the corpus's vocabulary (contract §5).
fn refusal_reason(error: &BuildError) -> &'static str {
    match error {
        BuildError::NotAName { .. } => "not-a-name",
        BuildError::Incomplete { .. } => "incomplete",
        BuildError::NotASpan { .. } => "not-a-span",
        BuildError::NotAnAnswerer { .. } => "not-an-answerer",
        // `BuildError` is non-exhaustive, so this arm is required. It panics
        // rather than guessing: contract §5 says a builder reports these two
        // reasons and must not add one the document does not describe, so a
        // third variant reaching here is that rule being broken, and a silent
        // fallback would let the corpus keep passing while it was.
        other => panic!(
            "this builder refused with a reason the contract does not name: {other}\n\
             add it to spec/query-builder-v1.md §5 first, then here"
        ),
    }
}

#[test]
fn every_corpus_query_renders_exactly() {
    let document: Json = read_corpus("queries-v1.json");

    assert_eq!(
        document["contract_major"].as_u64(),
        Some(1),
        "this builder implements a different contract major from the corpus"
    );

    let cases = document["cases"].as_array().expect("the corpus has cases");
    assert!(
        cases.len() >= 20,
        "the corpus shrank to {} cases — a suite cannot get stronger by losing vectors",
        cases.len()
    );

    let mut rendered = 0_usize;
    let mut refused = 0_usize;

    for case in cases {
        let name = case["name"].as_str().expect("every case is named");
        let built = build(&case["build"]);

        if let Some(expected) = case.get("refused") {
            let error = built.as_ref().err().unwrap_or_else(|| {
                panic!(
                    "case {name}: the corpus says this is refused, and this builder \
                     rendered {:?}",
                    built.as_ref().map(|query| &query.script)
                )
            });
            assert_eq!(
                refusal_reason(error),
                expected["reason"].as_str().expect("a reason"),
                "case {name}: refused for the wrong reason — {error}"
            );
            refused = refused.saturating_add(1);
            continue;
        }

        let query = built
            .unwrap_or_else(|why| panic!("case {name}: this builder refused a stated case: {why}"));

        assert_eq!(
            query.script,
            case["script"].as_str().expect("every case has a script"),
            "case {name}: rendered text differs from the corpus"
        );
        assert_eq!(
            query.parameters,
            expected_parameters(&case["parameters"]),
            "case {name}: parameters differ from the corpus"
        );

        rendered = rendered.saturating_add(1);
    }

    assert_eq!(
        rendered.saturating_add(refused),
        cases.len(),
        "every case must have been checked"
    );
    assert!(
        refused > 0,
        "a corpus with no refusals tests only the happy half"
    );
    println!("query corpus: {rendered} rendered, {refused} refused");
}

#[test]
fn a_corpus_value_never_reaches_the_script() {
    // Protocol §7 clause 7, checked over the whole corpus rather than on one
    // case: whatever a case's parameters hold, its text must not spell it.
    let document: Json = read_corpus("queries-v1.json");
    let cases = document["cases"].as_array().expect("the corpus has cases");

    let mut checked = 0_usize;
    for case in cases {
        let Some(script) = case.get("script").and_then(Json::as_str) else {
            continue;
        };
        for (name, value) in case["parameters"].as_object().expect("parameters") {
            if let Value::String(text) = value_of(value) {
                assert!(
                    !script.contains(text.as_str()),
                    "case {}: the value bound to ${name} is spelled in the script",
                    case["name"]
                );
                checked = checked.saturating_add(1);
            }
        }
    }

    assert!(
        checked > 0,
        "no case bound a string, so this assertion proved nothing"
    );
    println!("hostile-value check: {checked} bound strings, none in any script");
}
