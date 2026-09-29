//! The statements a consumer sends (consumer contract §2), rendered in one place.
//!
//! The consumer calls nothing else to build its text, so the shared corpus
//! (`consumer-v1.json`) checking this module checks what actually goes out.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::query::{BuildError, check_name};
use crate::value::{Number, Value};

/// Names checked once, ready to be written into every statement.
#[derive(Debug)]
pub(super) struct Statements {
    /// `USE NAMESPACE …; USE DATABASE …; ` — sent with every statement, because
    /// a connection that reconnected has forgotten any earlier `USE` (§5).
    tenancy: String,
    topic: String,
    group: String,
}

/// Whether a group name may be written into a statement as a quoted literal
/// (§3): the statement cannot take it as a parameter, so it is checked, never
/// escaped.
fn is_group_name(name: &str) -> bool {
    (1..=128).contains(&name.len())
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.:-".contains(character))
}

impl Statements {
    pub(super) fn new(
        (namespace, database): (&str, &str),
        topic: &str,
        group: &str,
    ) -> std::result::Result<Self, BuildError> {
        check_name("namespace", namespace)?;
        check_name("database", database)?;
        check_name("topic", topic)?;
        if !is_group_name(group) {
            return Err(BuildError::NotAGroupName {
                name: group.to_owned(),
            });
        }
        Ok(Self {
            tenancy: format!("USE NAMESPACE {namespace}; USE DATABASE {database}; "),
            topic: topic.to_owned(),
            group: group.to_owned(),
        })
    }

    /// Take up to `limit` messages.
    pub(super) fn read(&self, limit: u64) -> String {
        format!(
            "{}READ FROM {} FOR CONSUMER '{}' LIMIT {limit};",
            self.tenancy, self.topic, self.group
        )
    }

    /// Acknowledge `positions`, bound as `$p0 …`.
    pub(super) fn ack(&self, positions: &[u64]) -> Result<(String, Vec<(String, Value)>)> {
        self.settle("ACK", positions, "")
    }

    /// Hand `positions` back, now or after `delay`.
    pub(super) fn nack(
        &self,
        positions: &[u64],
        delay: Option<Duration>,
    ) -> Result<(String, Vec<(String, Value)>)> {
        // A delay is a duration literal in the grammar, not a parameter, and it
        // is written from a number this function formats, never from a caller's
        // text. Under a millisecond there is nothing to wait for.
        let tail = delay
            .map(|delay| delay.as_millis())
            .filter(|millis| *millis > 0)
            .map_or_else(String::new, |millis| format!(" DELAY {millis}ms"));
        self.settle("NACK", positions, &tail)
    }

    fn settle(
        &self,
        verb: &str,
        positions: &[u64],
        tail: &str,
    ) -> Result<(String, Vec<(String, Value)>)> {
        let mut script = format!(
            "{}{verb} {} FOR CONSUMER '{}' AT ",
            self.tenancy, self.topic, self.group
        );
        let mut parameters = Vec::with_capacity(positions.len());
        for (index, position) in positions.iter().enumerate() {
            if index > 0 {
                script.push_str(", ");
            }
            let name = format!("p{index}");
            script.push('$');
            script.push_str(&name);
            let held = i64::try_from(*position).map_err(|_| Error::Malformed)?;
            parameters.push((name, Value::Number(Number::Integer(held))));
        }
        script.push_str(tail);
        script.push(';');
        Ok((script, parameters))
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
    use crate::value::{Number, Value};

    /// The protocol repository sits beside this one unless the variable says
    /// otherwise; a missing corpus fails rather than passing having found nothing.
    fn corpus() -> Json {
        let path = std::env::var("TESSARI_PROTOCOL_CONFORMANCE").map_or_else(
            |_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../tessaridb-protocol/conformance/consumer-v1.json")
            },
            |directory| PathBuf::from(directory).join("consumer-v1.json"),
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|why| {
            panic!(
                "the consumer corpus is required: {}: {why}\n\
                 set TESSARI_PROTOCOL_CONFORMANCE to the conformance directory",
                path.display()
            )
        });
        serde_json::from_str(&raw).expect("the corpus is JSON")
    }

    fn text<'a>(fields: &'a Json, key: &str) -> &'a str {
        fields[key].as_str().expect("a string field")
    }

    /// What the corpus says a refusal is, from this client's error.
    fn refusal(error: &BuildError) -> Json {
        match error {
            BuildError::NotAName { what, name } => {
                serde_json::json!({"reason": "not-a-name", "what": format!("a {what}"), "name": name})
            }
            BuildError::NotAGroupName { name } => {
                serde_json::json!({"reason": "not-a-name", "what": "a group", "name": name})
            }
            other => panic!("a refusal the corpus does not name: {other:?}"),
        }
    }

    fn rendered(kind: &str, fields: &Json, statements: &Statements) -> (String, Json) {
        let positions: Vec<u64> = fields["positions"]
            .as_array()
            .map(|all| {
                all.iter()
                    .map(|each| each.as_u64().expect("a position"))
                    .collect()
            })
            .unwrap_or_default();
        let (script, parameters) = match kind {
            "read" => (
                statements.read(fields["limit"].as_u64().expect("a limit")),
                Vec::new(),
            ),
            "ack" => statements.ack(&positions).expect("positions fit"),
            "nack" => {
                let delay = fields["delay_ms"].as_u64().map(Duration::from_millis);
                statements.nack(&positions, delay).expect("positions fit")
            }
            other => panic!("a build kind this test does not know: {other}"),
        };
        let mut bound = serde_json::Map::new();
        for (name, value) in parameters {
            let Value::Number(Number::Integer(held)) = value else {
                panic!("a position is bound as an integer");
            };
            bound.insert(name, serde_json::json!({"integer": held.to_string()}));
        }
        (script, Json::Object(bound))
    }

    #[test]
    fn every_consumer_statement_renders_as_the_corpus_says() {
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
            let made = Statements::new(
                (text(fields, "namespace"), text(fields, "database")),
                text(fields, "topic"),
                text(fields, "group"),
            );
            match (made, case.get("refused")) {
                (Ok(statements), None) => {
                    let (script, parameters) = rendered(kind, fields, &statements);
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
