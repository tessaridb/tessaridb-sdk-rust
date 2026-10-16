//! The statements a queue worker sends (queue contract §2), rendered in one place.
//!
//! The worker calls nothing else to build its text, so the shared corpus
//! (`queue-v1.json`) checking this module checks what actually goes out.

use std::time::Duration;

use crate::query::Filter;
use crate::query::{Binder, BuildError, check_name};
use crate::value::Value;

/// The most records one claim may take; the node refuses more.
const CEILING: u64 = 500;

/// Names checked once, ready to be written into every statement.
#[derive(Debug)]
pub(super) struct Statements {
    /// `USE NAMESPACE …; USE DATABASE …; ` — sent with every statement, because
    /// a connection that reconnected has forgotten any earlier `USE` (§5).
    tenancy: String,
    queue: String,
    worker: String,
}

/// Whether a worker name may be written into a statement as a quoted literal
/// (§3): the statement cannot take it as a parameter, so it is checked, never
/// escaped.
fn is_worker_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.:-".contains(character))
}

impl Statements {
    pub(super) fn new(
        (namespace, database): (&str, &str),
        queue: &str,
        worker: &str,
    ) -> Result<Self, BuildError> {
        check_name("namespace", namespace)?;
        check_name("database", database)?;
        check_name("queue", queue)?;
        if !is_worker_name(worker) {
            return Err(BuildError::NotAWorkerName {
                name: worker.to_owned(),
            });
        }
        Ok(Self {
            tenancy: format!("USE NAMESPACE {namespace}; USE DATABASE {database}; "),
            queue: queue.to_owned(),
            worker: worker.to_owned(),
        })
    }

    /// Take up to `count` records, narrowed by `filter`; its values bound.
    pub(super) fn claim(
        &self,
        count: u64,
        filter: Option<&Filter>,
    ) -> Result<(String, Vec<(String, Value)>), BuildError> {
        if !(1..=CEILING).contains(&count) {
            return Err(BuildError::NotACount { count });
        }
        let mut binder = Binder::new();
        let narrowed = match filter {
            Some(filter) => format!(" WHERE {}", filter.render(&mut binder)?),
            None => String::new(),
        };
        let script = format!(
            "{}USE CONSUMER '{}'; CLAIM {count} FROM {}{narrowed};",
            self.tenancy, self.worker, self.queue
        );
        Ok((script, binder.finish().into_iter().collect()))
    }

    /// Delete a record a claim took, named by the claim's own spelling (§2).
    pub(super) fn done(&self, id: &str) -> String {
        format!("{}DELETE {}:{id};", self.tenancy, self.queue)
    }

    /// Hand a record back, now or not before `after`.
    pub(super) fn release(&self, id: &str, after: Option<Duration>) -> String {
        // A delay is a duration literal written from a number this function
        // formats, never from a caller's text. Under a millisecond there is
        // nothing to wait for.
        let tail = after
            .map(|after| after.as_millis())
            .filter(|millis| *millis > 0)
            .map_or_else(String::new, |millis| format!(" NOT BEFORE {millis}ms"));
        format!(
            "{}RELEASE {}:{id} FOR CONSUMER '{}'{tail};",
            self.tenancy, self.queue, self.worker
        )
    }
}

#[cfg(test)]
mod tests {
    // A test is the one place a panic is the correct outcome; these lints exist
    // to keep panics out of the paths a caller runs.
    #![allow(
        clippy::expect_used,
        clippy::unwrap_used,
        clippy::panic,
        clippy::indexing_slicing
    )]

    use std::path::PathBuf;
    use std::time::Duration;

    use serde_json::Value as Json;

    use super::Statements;
    use crate::query::BuildError;
    use crate::query::{Filter, field};
    use crate::value::{Number, Value};

    /// The protocol repository sits beside this one unless the variable says
    /// otherwise; a missing corpus fails rather than passing having found nothing.
    fn corpus() -> Json {
        let path = std::env::var("TESSARI_PROTOCOL_CONFORMANCE").map_or_else(
            |_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../tessaridb-protocol/conformance/queue-v1.json")
            },
            |directory| PathBuf::from(directory).join("queue-v1.json"),
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|why| {
            panic!(
                "the queue corpus is required: {}: {why}\n\
                 set TESSARI_PROTOCOL_CONFORMANCE to the conformance directory",
                path.display()
            )
        });
        serde_json::from_str(&raw).expect("the corpus is JSON")
    }

    fn text<'a>(fields: &'a Json, key: &str) -> &'a str {
        fields[key].as_str().expect("a string field")
    }

    fn value(json: &Json) -> Value {
        match json.as_object().and_then(|kind| kind.iter().next()) {
            Some((kind, held)) if kind == "string" => {
                Value::String(held.as_str().expect("text").to_owned())
            }
            Some((kind, held)) if kind == "integer" => Value::Number(Number::Integer(
                held.as_str().expect("digits").parse().expect("an integer"),
            )),
            other => panic!("a value this test does not know: {other:?}"),
        }
    }

    fn encoded(value: &Value) -> Json {
        match value {
            Value::String(held) => serde_json::json!({"string": held}),
            Value::Number(Number::Integer(held)) => {
                serde_json::json!({"integer": held.to_string()})
            }
            other => panic!("a value this test does not encode: {other:?}"),
        }
    }

    /// The corpus's filter as this client's builder.
    fn filter(node: &Json) -> Filter {
        if let Some(compare) = node.get("compare") {
            let named = field(text(compare, "field"));
            let held = value(&compare["value"]);
            return match text(compare, "op") {
                "eq" => named.eq(held),
                "ne" => named.ne(held),
                "lt" => named.lt(held),
                "le" => named.le(held),
                "gt" => named.gt(held),
                "ge" => named.ge(held),
                other => panic!("an operator this test does not know: {other}"),
            };
        }
        let (joiner, sides) = node
            .as_object()
            .expect("a filter")
            .iter()
            .next()
            .expect("one");
        let left = filter(&sides[0]);
        let right = filter(&sides[1]);
        match joiner.as_str() {
            "and" => left.and(right),
            "or" => left.or(right),
            other => panic!("a joiner this test does not know: {other}"),
        }
    }

    /// What the corpus says a refusal is, from this client's error.
    fn refusal(error: &BuildError) -> Json {
        match error {
            BuildError::NotAName { what, name } => {
                // The builder's own checks already say "a field"; this module's say "queue".
                let what = if what.starts_with("a ") {
                    (*what).to_owned()
                } else {
                    format!("a {what}")
                };
                serde_json::json!({"reason": "not-a-name", "what": what, "name": name})
            }
            BuildError::NotAWorkerName { name } => {
                serde_json::json!({"reason": "not-a-name", "what": "a worker", "name": name})
            }
            BuildError::NotACount { count } => {
                serde_json::json!({"reason": "not-a-count", "what": "a count", "name": count.to_string()})
            }
            other => panic!("a refusal the corpus does not name: {other:?}"),
        }
    }

    fn rendered(kind: &str, fields: &Json) -> Result<(String, Json), BuildError> {
        let statements = Statements::new(
            (text(fields, "namespace"), text(fields, "database")),
            text(fields, "queue"),
            fields["worker"].as_str().unwrap_or("worker"),
        )?;
        let (script, parameters) = match kind {
            "claim" => {
                let narrowed = fields.get("where").map(filter);
                statements.claim(
                    fields["count"].as_u64().expect("a count"),
                    narrowed.as_ref(),
                )?
            }
            "done" => (statements.done(text(fields, "id")), Vec::new()),
            "release" => {
                let after = fields["after_ms"].as_u64().map(Duration::from_millis);
                (statements.release(text(fields, "id"), after), Vec::new())
            }
            other => panic!("a build kind this test does not know: {other}"),
        };
        let mut bound = serde_json::Map::new();
        for (name, value) in parameters {
            bound.insert(name, encoded(&value));
        }
        Ok((script, Json::Object(bound)))
    }

    #[test]
    fn every_queue_statement_renders_as_the_corpus_says() {
        let corpus = corpus();
        let cases = corpus["cases"].as_array().expect("cases");
        assert!(!cases.is_empty(), "a corpus with no cases checks nothing");
        for case in cases {
            let name = text(case, "name");
            let (kind, fields) = case["build"]
                .as_object()
                .expect("a build")
                .iter()
                .next()
                .expect("one kind");
            match (rendered(kind, fields), case.get("refused")) {
                (Ok((script, parameters)), None) => {
                    assert_eq!(script, text(case, "script"), "{name}");
                    assert_eq!(parameters, case["parameters"], "{name}");
                }
                (Err(error), Some(expected)) => assert_eq!(&refusal(&error), expected, "{name}"),
                (Ok(_), Some(_)) => panic!("{name}: rendered a case the corpus refuses"),
                (Err(error), None) => {
                    panic!("{name}: refused a case the corpus renders: {error:?}")
                }
            }
        }
    }
}
