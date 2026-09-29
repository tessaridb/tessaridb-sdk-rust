//! The statements, byte for byte, as cache contract §2 writes them.

// A test is the one place a panic is the correct outcome; these lints exist to
// keep panics out of the paths a caller runs.
#![allow(clippy::expect_used)]

use std::time::Duration;

use super::{Statements, duration, unquoted};
use crate::query::BuildError;
use crate::value::Value;

const USE: &str = "USE NAMESPACE app; USE DATABASE main; ";

fn space() -> Statements {
    Statements::new(("app", "main"), "cache").expect("three names")
}

fn names(given: &[(&'static str, Value)]) -> Vec<&'static str> {
    given.iter().map(|(name, _)| *name).collect()
}

#[test]
fn every_statement_is_the_one_the_contract_writes() {
    let s = space();
    let ttl = || duration(Duration::from_secs(30));
    let cases = [
        (s.get("k"), "GET cache:$k;", vec!["k"]),
        (
            s.set("k", Value::Null, "", None, None),
            "SET cache:$k = $v;",
            vec!["k", "v"],
        ),
        (
            s.set("k", Value::Null, "", None, ttl()),
            "SET cache:$k = $v EXPIRE $t;",
            vec!["k", "v", "t"],
        ),
        (
            s.set("k", Value::Null, " IF ABSENT", None, ttl()),
            "SET cache:$k = $v IF ABSENT EXPIRE $t;",
            vec!["k", "v", "t"],
        ),
        (
            s.set("k", Value::Null, " IF PRESENT", None, None),
            "SET cache:$k = $v IF PRESENT;",
            vec!["k", "v"],
        ),
        (
            s.set("k", Value::Null, " IF = $e", Some(Value::Null), None),
            "SET cache:$k = $v IF = $e;",
            vec!["k", "v", "e"],
        ),
        (s.delete("k"), "DELETE cache:$k RETURN BEFORE;", vec!["k"]),
        (s.incr("k", 5), "INCR cache:$k BY $n;", vec!["k", "n"]),
        (s.ttl("k"), "RETURN TTL cache:$k;", vec!["k"]),
        (
            s.expire("k", Value::Null),
            "EXPIRE cache:$k $t;",
            vec!["k", "t"],
        ),
        (s.persist("k"), "PERSIST cache:$k;", vec!["k"]),
        (
            s.keys(None, None, 100),
            "KEYS FROM cache LIMIT 100;",
            vec![],
        ),
        (
            s.keys(Some(""), None, 100),
            "KEYS FROM cache LIMIT 100;",
            vec![],
        ),
        (
            s.keys(Some("user:"), Some("user:1"), 10),
            "KEYS FROM cache PREFIX $p AFTER $a LIMIT 10;",
            vec!["p", "a"],
        ),
        (
            s.lock("k", "w1", Value::Null),
            "SET cache:$k = $h IF ABSENT EXPIRE $t;",
            vec!["k", "h", "t"],
        ),
        (
            s.extend("k", "w1", Value::Null),
            "SET cache:$k = $h IF = $h EXPIRE $t;",
            vec!["k", "h", "t"],
        ),
        (
            s.release("k", "w1"),
            "SET cache:$k = 'free' IF = $h EXPIRE 1ms;",
            vec!["k", "h"],
        ),
    ];
    for ((script, given), statement, bound) in cases {
        assert_eq!(script, format!("{USE}{statement}"));
        assert_eq!(names(&given), bound, "{statement}");
    }
}

#[test]
fn a_name_that_is_not_one_is_refused_before_anything_is_rendered() {
    assert!(matches!(
        Statements::new(("app", "main"), "ca-che"),
        Err(BuildError::NotAName { what: "space", .. })
    ));
    assert!(Statements::new(("app; DROP", "main"), "cache").is_err());
}

#[test]
fn a_ttl_that_is_not_positive_is_no_duration() {
    assert_eq!(duration(Duration::ZERO), None);
    assert_eq!(
        duration(Duration::from_millis(1500)),
        Some(Value::Duration {
            seconds: 1,
            nanos: 500_000_000
        })
    );
}

#[test]
fn a_quoted_key_is_the_string_again_and_any_other_kind_is_left_alone() {
    assert_eq!(unquoted("'user:1'"), "user:1");
    assert_eq!(unquoted(r"'it\'s'"), "it's");
    assert_eq!(unquoted(r"'a\\b'"), r"a\b");
    assert_eq!(unquoted("42"), "42");
}
