//! The container statements a cache handle sends (cache contract 1.1, §2a).
//!
//! Writes are the node's container statements; reads are functions over `GET`,
//! so every call is one statement. Each value, field, member, score and position
//! is bound — several of them under numbered names, `$v0`, `$v1`, … in the order
//! the caller gave them.

use crate::value::Value;

use super::Statements;

/// A statement and its numbered parameters.
pub(in crate::cache) type Bound = (String, Vec<(String, Value)>);

/// Which end of a list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    /// The first element.
    Front,
    /// The last element.
    Back,
}

impl End {
    const fn word(self) -> &'static str {
        match self {
            Self::Front => "FRONT",
            Self::Back => "BACK",
        }
    }
}

impl Statements {
    /// `{statement}` with `{s}` already written, keyed by `$k`.
    fn bound(&self, statement: &str, key: &str) -> Bound {
        (
            format!("{}{statement}", self.tenancy),
            vec![("k".to_owned(), Value::String(key.to_owned()))],
        )
    }

    /// `$p0, $p1, …` for `values`, bound in order.
    fn numbered(prefix: &str, values: Vec<Value>, given: &mut Vec<(String, Value)>) -> String {
        let mut names = Vec::with_capacity(values.len());
        for (at, value) in values.into_iter().enumerate() {
            let name = format!("{prefix}{at}");
            names.push(format!("${name}"));
            given.push((name, value));
        }
        names.join(", ")
    }

    /// `$a0 = $b0, $a1 = $b1, …` for `pairs`, bound in order.
    fn paired(
        (left, right): (&str, &str),
        pairs: Vec<(Value, Value)>,
        given: &mut Vec<(String, Value)>,
    ) -> String {
        let mut written = Vec::with_capacity(pairs.len());
        for (at, (name, value)) in pairs.into_iter().enumerate() {
            written.push(format!("${left}{at} = ${right}{at}"));
            given.push((format!("{left}{at}"), name));
            given.push((format!("{right}{at}"), value));
        }
        written.join(", ")
    }

    fn with(mut bound: Bound, extra: &[(&str, Value)]) -> Bound {
        for (name, value) in extra {
            bound.1.push(((*name).to_owned(), value.clone()));
        }
        bound
    }

    pub(in crate::cache) fn list_push(&self, key: &str, end: End, values: Vec<Value>) -> Bound {
        let mut given = vec![("k".to_owned(), Value::String(key.to_owned()))];
        let list = Self::numbered("v", values, &mut given);
        (
            format!(
                "{}LIST PUSH {}:$k {} {list};",
                self.tenancy,
                self.space,
                end.word()
            ),
            given,
        )
    }

    pub(in crate::cache) fn list_pop(&self, key: &str, end: End, count: Option<i64>) -> Bound {
        match count {
            None => self.bound(&format!("LIST POP {}:$k {};", self.space, end.word()), key),
            Some(count) => Self::with(
                self.bound(
                    &format!("LIST POP {}:$k {} LIMIT $n;", self.space, end.word()),
                    key,
                ),
                &[("n", Value::from(count))],
            ),
        }
    }

    pub(in crate::cache) fn list_range(&self, key: &str, start: i64, count: i64) -> Bound {
        Self::with(
            self.bound(
                &format!("RETURN array::slice(GET {}:$k, $i, $n);", self.space),
                key,
            ),
            &[("i", Value::from(start)), ("n", Value::from(count))],
        )
    }

    pub(in crate::cache) fn list_at(&self, key: &str, index: i64) -> Bound {
        Self::with(
            self.bound(
                &format!("RETURN array::at(GET {}:$k, $i);", self.space),
                key,
            ),
            &[("i", Value::from(index))],
        )
    }

    pub(in crate::cache) fn list_len(&self, key: &str) -> Bound {
        self.bound(&format!("RETURN array::len(GET {}:$k);", self.space), key)
    }

    pub(in crate::cache) fn list_remove(
        &self,
        key: &str,
        value: Value,
        count: Option<i64>,
    ) -> Bound {
        match count {
            None => Self::with(
                self.bound(&format!("LIST REMOVE {}:$k $v;", self.space), key),
                &[("v", value)],
            ),
            Some(count) => Self::with(
                self.bound(&format!("LIST REMOVE {}:$k $v LIMIT $n;", self.space), key),
                &[("v", value), ("n", Value::from(count))],
            ),
        }
    }

    pub(in crate::cache) fn list_trim(&self, key: &str, start: i64, stop: i64) -> Bound {
        Self::with(
            self.bound(&format!("LIST TRIM {}:$k FROM $i TO $j;", self.space), key),
            &[("i", Value::from(start)), ("j", Value::from(stop))],
        )
    }

    pub(in crate::cache) fn hash_set(&self, key: &str, fields: Vec<(String, Value)>) -> Bound {
        let mut given = vec![("k".to_owned(), Value::String(key.to_owned()))];
        let pairs = fields
            .into_iter()
            .map(|(field, value)| (Value::String(field), value))
            .collect();
        let written = Self::paired(("f", "v"), pairs, &mut given);
        (
            format!("{}HASH SET {}:$k {written};", self.tenancy, self.space),
            given,
        )
    }

    pub(in crate::cache) fn hash_get(&self, key: &str, field: &str) -> Bound {
        Self::with(
            self.bound(
                &format!("RETURN object::get(GET {}:$k, $f);", self.space),
                key,
            ),
            &[("f", Value::String(field.to_owned()))],
        )
    }

    pub(in crate::cache) fn hash_remove(&self, key: &str, fields: Vec<String>) -> Bound {
        let mut given = vec![("k".to_owned(), Value::String(key.to_owned()))];
        let list = Self::numbered(
            "f",
            fields.into_iter().map(Value::String).collect(),
            &mut given,
        );
        (
            format!("{}HASH REMOVE {}:$k {list};", self.tenancy, self.space),
            given,
        )
    }

    pub(in crate::cache) fn hash_incr(&self, key: &str, field: &str, by: i64) -> Bound {
        Self::with(
            self.bound(&format!("HASH INCR {}:$k $f BY $n;", self.space), key),
            &[
                ("f", Value::String(field.to_owned())),
                ("n", Value::from(by)),
            ],
        )
    }

    pub(in crate::cache) fn hash_len(&self, key: &str) -> Bound {
        self.bound(&format!("RETURN object::len(GET {}:$k);", self.space), key)
    }

    pub(in crate::cache) fn members(&self, verb: &str, key: &str, members: Vec<Value>) -> Bound {
        let mut given = vec![("k".to_owned(), Value::String(key.to_owned()))];
        let list = Self::numbered("v", members, &mut given);
        (
            format!("{}MEMBERS {verb} {}:$k {list};", self.tenancy, self.space),
            given,
        )
    }

    pub(in crate::cache) fn members_has(&self, key: &str, member: Value) -> Bound {
        Self::with(
            self.bound(&format!("RETURN $v IN GET {}:$k;", self.space), key),
            &[("v", member)],
        )
    }

    pub(in crate::cache) fn members_len(&self, key: &str) -> Bound {
        self.bound(&format!("RETURN set::len(GET {}:$k);", self.space), key)
    }

    pub(in crate::cache) fn scores_add(&self, key: &str, scores: Vec<(String, f64)>) -> Bound {
        let mut given = vec![("k".to_owned(), Value::String(key.to_owned()))];
        let pairs = scores
            .into_iter()
            .map(|(member, score)| (Value::String(member), Value::from(score)))
            .collect();
        let written = Self::paired(("m", "c"), pairs, &mut given);
        (
            format!("{}SCORES ADD {}:$k {written};", self.tenancy, self.space),
            given,
        )
    }

    pub(in crate::cache) fn scores_incr(&self, key: &str, member: &str, by: f64) -> Bound {
        Self::with(
            self.bound(&format!("SCORES INCR {}:$k $m BY $c;", self.space), key),
            &[
                ("m", Value::String(member.to_owned())),
                ("c", Value::from(by)),
            ],
        )
    }

    pub(in crate::cache) fn scores_remove(&self, key: &str, members: Vec<String>) -> Bound {
        let mut given = vec![("k".to_owned(), Value::String(key.to_owned()))];
        let list = Self::numbered(
            "m",
            members.into_iter().map(Value::String).collect(),
            &mut given,
        );
        (
            format!("{}SCORES REMOVE {}:$k {list};", self.tenancy, self.space),
            given,
        )
    }

    /// `scores::score` or `scores::rank` of one member.
    pub(in crate::cache) fn scores_of(&self, function: &str, key: &str, member: &str) -> Bound {
        Self::with(
            self.bound(
                &format!("RETURN scores::{function}(GET {}:$k, $m);", self.space),
                key,
            ),
            &[("m", Value::String(member.to_owned()))],
        )
    }

    pub(in crate::cache) fn scores_range(&self, key: &str, start: i64, stop: i64) -> Bound {
        Self::with(
            self.bound(
                &format!("RETURN scores::range(GET {}:$k, $i, $j);", self.space),
                key,
            ),
            &[("i", Value::from(start)), ("j", Value::from(stop))],
        )
    }

    /// An absent bound is written `NONE` in its place (§2a).
    pub(in crate::cache) fn scores_between(
        &self,
        key: &str,
        low: Option<f64>,
        high: Option<f64>,
    ) -> Bound {
        let low_text = if low.is_some() { "$lo" } else { "NONE" };
        let high_text = if high.is_some() { "$hi" } else { "NONE" };
        let mut bound = self.bound(
            &format!(
                "RETURN scores::between(GET {}:$k, {low_text}, {high_text});",
                self.space
            ),
            key,
        );
        if let Some(low) = low {
            bound.1.push(("lo".to_owned(), Value::from(low)));
        }
        if let Some(high) = high {
            bound.1.push(("hi".to_owned(), Value::from(high)));
        }
        bound
    }
}

#[cfg(test)]
mod tests;
