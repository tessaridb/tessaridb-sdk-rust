//! The container statements, byte for byte, as cache contract §2a writes them.

#![allow(clippy::expect_used)]

use super::super::Statements;
use super::{Bound, End};
use crate::value::Value;

const USE: &str = "USE NAMESPACE app; USE DATABASE main; ";

fn space() -> Statements {
    Statements::new(("app", "main"), "cache").expect("three names")
}

fn names(given: &[(String, Value)]) -> Vec<&str> {
    given.iter().map(|(name, _)| name.as_str()).collect()
}

fn text(values: &[&str]) -> Vec<Value> {
    values.iter().map(|value| Value::from(*value)).collect()
}

fn check(cases: Vec<(Bound, &str, Vec<&str>)>) {
    for ((script, given), statement, expected) in cases {
        assert_eq!(script, format!("{USE}{statement}"));
        assert_eq!(names(&given), expected, "{statement}");
    }
}

#[test]
fn every_list_statement_is_the_one_the_contract_writes() {
    let s = space();
    check(vec![
        (
            s.list_push("k", End::Back, text(&["a", "b"])),
            "LIST PUSH cache:$k BACK $v0, $v1;",
            vec!["k", "v0", "v1"],
        ),
        (
            s.list_push("k", End::Front, text(&["a"])),
            "LIST PUSH cache:$k FRONT $v0;",
            vec!["k", "v0"],
        ),
        (
            s.list_pop("k", End::Front, None),
            "LIST POP cache:$k FRONT;",
            vec!["k"],
        ),
        (
            s.list_pop("k", End::Back, Some(3)),
            "LIST POP cache:$k BACK LIMIT $n;",
            vec!["k", "n"],
        ),
        (
            s.list_range("k", 0, 10),
            "RETURN array::slice(GET cache:$k, $i, $n);",
            vec!["k", "i", "n"],
        ),
        (
            s.list_at("k", -1),
            "RETURN array::at(GET cache:$k, $i);",
            vec!["k", "i"],
        ),
        (
            s.list_len("k"),
            "RETURN array::len(GET cache:$k);",
            vec!["k"],
        ),
        (
            s.list_remove("k", Value::from("a"), None),
            "LIST REMOVE cache:$k $v;",
            vec!["k", "v"],
        ),
        (
            s.list_remove("k", Value::from("a"), Some(2)),
            "LIST REMOVE cache:$k $v LIMIT $n;",
            vec!["k", "v", "n"],
        ),
        (
            s.list_trim("k", 0, 99),
            "LIST TRIM cache:$k FROM $i TO $j;",
            vec!["k", "i", "j"],
        ),
    ]);
}

#[test]
fn every_hash_statement_is_the_one_the_contract_writes() {
    let s = space();
    check(vec![
        (
            s.hash_set(
                "k",
                vec![
                    ("x".to_owned(), Value::from(1)),
                    ("y".to_owned(), Value::Null),
                ],
            ),
            "HASH SET cache:$k $f0 = $v0, $f1 = $v1;",
            vec!["k", "f0", "v0", "f1", "v1"],
        ),
        (
            s.hash_get("k", "x"),
            "RETURN object::get(GET cache:$k, $f);",
            vec!["k", "f"],
        ),
        (
            s.hash_remove("k", vec!["x".to_owned(), "y".to_owned()]),
            "HASH REMOVE cache:$k $f0, $f1;",
            vec!["k", "f0", "f1"],
        ),
        (
            s.hash_incr("k", "x", 2),
            "HASH INCR cache:$k $f BY $n;",
            vec!["k", "f", "n"],
        ),
        (
            s.hash_len("k"),
            "RETURN object::len(GET cache:$k);",
            vec!["k"],
        ),
    ]);
}

#[test]
fn every_set_statement_is_the_one_the_contract_writes() {
    let s = space();
    check(vec![
        (
            s.members("ADD", "k", text(&["a", "b"])),
            "MEMBERS ADD cache:$k $v0, $v1;",
            vec!["k", "v0", "v1"],
        ),
        (
            s.members("REMOVE", "k", text(&["a"])),
            "MEMBERS REMOVE cache:$k $v0;",
            vec!["k", "v0"],
        ),
        (
            s.members_has("k", Value::from("a")),
            "RETURN $v IN GET cache:$k;",
            vec!["k", "v"],
        ),
        (
            s.members_len("k"),
            "RETURN set::len(GET cache:$k);",
            vec!["k"],
        ),
    ]);
}

#[test]
fn every_ranking_statement_is_the_one_the_contract_writes() {
    let s = space();
    check(vec![
        (
            s.scores_add("k", vec![("ann".to_owned(), 1.0), ("bob".to_owned(), 2.5)]),
            "SCORES ADD cache:$k $m0 = $c0, $m1 = $c1;",
            vec!["k", "m0", "c0", "m1", "c1"],
        ),
        (
            s.scores_incr("k", "ann", 1.5),
            "SCORES INCR cache:$k $m BY $c;",
            vec!["k", "m", "c"],
        ),
        (
            s.scores_remove("k", vec!["ann".to_owned()]),
            "SCORES REMOVE cache:$k $m0;",
            vec!["k", "m0"],
        ),
        (
            s.scores_of("score", "k", "ann"),
            "RETURN scores::score(GET cache:$k, $m);",
            vec!["k", "m"],
        ),
        (
            s.scores_of("rank", "k", "ann"),
            "RETURN scores::rank(GET cache:$k, $m);",
            vec!["k", "m"],
        ),
        (
            s.scores_range("k", 0, -1),
            "RETURN scores::range(GET cache:$k, $i, $j);",
            vec!["k", "i", "j"],
        ),
        (
            s.scores_between("k", Some(1.0), Some(2.0)),
            "RETURN scores::between(GET cache:$k, $lo, $hi);",
            vec!["k", "lo", "hi"],
        ),
        (
            s.scores_between("k", None, Some(2.0)),
            "RETURN scores::between(GET cache:$k, NONE, $hi);",
            vec!["k", "hi"],
        ),
        (
            s.scores_between("k", Some(1.0), None),
            "RETURN scores::between(GET cache:$k, $lo, NONE);",
            vec!["k", "lo"],
        ),
    ]);
}

#[test]
fn numbered_values_are_bound_in_the_order_given() {
    let (_, given) = space().list_push("k", End::Back, text(&["first", "second"]));
    assert_eq!(
        given.get(1).map(|(_, value)| value),
        Some(&Value::from("first"))
    );
    assert_eq!(
        given.get(2).map(|(_, value)| value),
        Some(&Value::from("second"))
    );
}
