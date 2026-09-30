//! The statements a vault handle sends (vault contract §3), rendered in one place.
//!
//! Every id, value, recipient name and key is bound. Only checked names are
//! written into the text: the vault and an actor bare, a field **quoted** — a
//! vault is exactly where somebody declares a field with a word the language
//! reserves (`password`), and a checked name holds no quote, so quoting it cannot
//! change how the node reads it.

use std::collections::BTreeMap;

use crate::query::{BuildError, check_name};
use crate::value::Value;

/// The most ids one listing may ask for (§3.1).
pub(super) const MOST_IDS: u32 = 10_000;

/// Whether a listing limit is one the contract refuses before sending (§3.1).
pub(super) const fn limit_refused(limit: Option<u32>) -> bool {
    matches!(limit, Some(limit) if limit == 0 || limit > MOST_IDS)
}

/// A statement and what its parameters bind to.
pub(super) type Rendered = (String, Vec<(String, Value)>);

/// Names checked once, ready to be written into every statement.
#[derive(Debug, Clone)]
pub(super) struct Statements {
    /// `USE NAMESPACE …; USE DATABASE …; ` — sent with every statement, because a
    /// connection that reconnected has forgotten any earlier `USE`.
    tenancy: String,
    vault: String,
}

/// `USE NAMESPACE …; USE DATABASE …; ` for two checked names.
pub(super) fn tenancy((namespace, database): (&str, &str)) -> Result<String, BuildError> {
    check_name("namespace", namespace)?;
    check_name("database", database)?;
    Ok(format!(
        "USE NAMESPACE {namespace}; USE DATABASE {database}; "
    ))
}

/// The audit statement (§3.5); `by` is a user name, which the statement takes as
/// a name rather than a value.
pub(super) fn audit(within: (&str, &str), by: Option<&str>) -> Result<String, BuildError> {
    let tenancy = tenancy(within)?;
    Ok(match by {
        Some(actor) => {
            check_name("actor", actor)?;
            format!("{tenancy}INFO FOR AUDIT BY {actor};")
        }
        None => format!("{tenancy}INFO FOR AUDIT;"),
    })
}

/// Checked field names in ascending byte order, each quoted.
fn quoted_fields<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<Vec<String>, BuildError> {
    let mut checked = Vec::new();
    for name in names {
        check_name("field", name)?;
        checked.push(name);
    }
    checked.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    Ok(checked
        .into_iter()
        .map(|name| format!("'{name}'"))
        .collect())
}

impl Statements {
    pub(super) fn new(within: (&str, &str), vault: &str) -> Result<Self, BuildError> {
        let tenancy = tenancy(within)?;
        check_name("vault", vault)?;
        Ok(Self {
            tenancy,
            vault: vault.to_owned(),
        })
    }

    /// `INFO FOR VAULT v RECORDS [AFTER v:$after] [LIMIT n]`; the caller has
    /// already refused a limit outside 1..=10000.
    pub(super) fn list(&self, after: Option<Value>, limit: Option<u32>) -> Rendered {
        let vault = &self.vault;
        let mut script = format!("{}INFO FOR VAULT {vault} RECORDS", self.tenancy);
        let mut given = Vec::new();
        if let Some(after) = after {
            script.push_str(" AFTER ");
            script.push_str(vault);
            script.push_str(":$after");
            given.push(("after".to_owned(), after));
        }
        if let Some(limit) = limit {
            script.push_str(" LIMIT ");
            script.push_str(&limit.to_string());
        }
        script.push(';');
        (script, given)
    }

    /// `REVEAL * | 'f', … FROM v:$id`.
    pub(super) fn reveal(&self, id: Value, fields: &[&str]) -> Result<Rendered, BuildError> {
        let which = if fields.is_empty() {
            "*".to_owned()
        } else {
            quoted_fields(fields.iter().copied())?.join(", ")
        };
        Ok((
            format!("{}REVEAL {which} FROM {}:$id;", self.tenancy, self.vault),
            vec![("id".to_owned(), id)],
        ))
    }

    /// `UPSERT v:$id MERGE { 'f': $f0, … }`; the caller has already refused an
    /// empty set of fields.
    pub(super) fn write(
        &self,
        id: Value,
        fields: &BTreeMap<String, Value>,
    ) -> Result<Rendered, BuildError> {
        let names = quoted_fields(fields.keys().map(String::as_str))?;
        let mut ordered: Vec<(&String, &Value)> = fields.iter().collect();
        ordered.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
        let mut given = vec![("id".to_owned(), id)];
        let mut pairs = Vec::with_capacity(names.len());
        for (index, (name, (_, value))) in names.iter().zip(ordered).enumerate() {
            pairs.push(format!("{name}: $f{index}"));
            given.push((format!("f{index}"), value.clone()));
        }
        Ok((
            format!(
                "{}UPSERT {}:$id MERGE {{ {} }};",
                self.tenancy,
                self.vault,
                pairs.join(", ")
            ),
            given,
        ))
    }

    pub(super) fn recipients(&self, id: Value) -> Rendered {
        (
            format!("{}INFO FOR RECIPIENTS OF {}:$id;", self.tenancy, self.vault),
            vec![("id".to_owned(), id)],
        )
    }

    pub(super) fn add_recipient(&self, id: Value, name: &str, key: &[u8]) -> Rendered {
        (
            format!(
                "{}ADD RECIPIENT $name TO {}:$id KEY $key;",
                self.tenancy, self.vault
            ),
            vec![
                ("id".to_owned(), id),
                ("name".to_owned(), Value::String(name.to_owned())),
                ("key".to_owned(), Value::Bytes(key.to_vec())),
            ],
        )
    }

    pub(super) fn remove_recipient(&self, id: Value, name: &str) -> Rendered {
        (
            format!(
                "{}REMOVE RECIPIENT $name FROM {}:$id;",
                self.tenancy, self.vault
            ),
            vec![
                ("id".to_owned(), id),
                ("name".to_owned(), Value::String(name.to_owned())),
            ],
        )
    }
}
