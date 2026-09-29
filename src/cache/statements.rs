//! The statements a cache handle sends (cache contract §2), rendered in one place.
//!
//! Every key, value, duration and holder is bound; only the space — a checked
//! name — and a listing's limit — an integer this module formats — are written
//! into the text.

use std::fmt::Write as _;
use std::time::Duration;

use crate::query::{BuildError, check_name};
use crate::value::Value;

/// The most keys one listing may ask for (§2).
pub(super) const MOST_KEYS: u32 = 1000;

/// Names checked once, ready to be written into every statement.
#[derive(Debug, Clone)]
pub(super) struct Statements {
    /// `USE NAMESPACE …; USE DATABASE …; ` — sent with every statement, because a
    /// connection that reconnected has forgotten any earlier `USE` (§1).
    tenancy: String,
    space: String,
}

/// A statement and what its parameters bind to.
pub(super) type Rendered = (String, Vec<(&'static str, Value)>);

/// A ttl as the store's duration, or `None` when it is not positive (§2).
pub(super) fn duration(ttl: Duration) -> Option<Value> {
    if ttl.is_zero() {
        return None;
    }
    Some(Value::Duration {
        seconds: i64::try_from(ttl.as_secs()).ok()?,
        nanos: ttl.subsec_nanos(),
    })
}

impl Statements {
    pub(super) fn new(
        (namespace, database): (&str, &str),
        space: &str,
    ) -> std::result::Result<Self, BuildError> {
        check_name("namespace", namespace)?;
        check_name("database", database)?;
        check_name("space", space)?;
        Ok(Self {
            tenancy: format!("USE NAMESPACE {namespace}; USE DATABASE {database}; "),
            space: space.to_owned(),
        })
    }

    fn keyed(&self, statement: &str, key: &str) -> Rendered {
        (
            format!("{}{statement}", self.tenancy),
            vec![("k", Value::String(key.to_owned()))],
        )
    }

    pub(super) fn get(&self, key: &str) -> Rendered {
        self.keyed(&format!("GET {}:$k;", self.space), key)
    }

    /// `SET`, with an optional condition (`" IF ABSENT"`, `" IF PRESENT"`,
    /// `" IF = $e"`) and an optional expiry.
    pub(super) fn set(
        &self,
        key: &str,
        value: Value,
        condition: &str,
        expected: Option<Value>,
        ttl: Option<Value>,
    ) -> Rendered {
        let expiry = if ttl.is_some() { " EXPIRE $t" } else { "" };
        let (script, mut given) = self.keyed(
            &format!("SET {}:$k = $v{condition}{expiry};", self.space),
            key,
        );
        given.push(("v", value));
        if let Some(expected) = expected {
            given.push(("e", expected));
        }
        if let Some(ttl) = ttl {
            given.push(("t", ttl));
        }
        (script, given)
    }

    pub(super) fn delete(&self, key: &str) -> Rendered {
        self.keyed(&format!("DELETE {}:$k RETURN BEFORE;", self.space), key)
    }

    pub(super) fn incr(&self, key: &str, by: i64) -> Rendered {
        let (script, mut given) = self.keyed(&format!("INCR {}:$k BY $n;", self.space), key);
        given.push(("n", Value::from(by)));
        (script, given)
    }

    pub(super) fn ttl(&self, key: &str) -> Rendered {
        self.keyed(&format!("RETURN TTL {}:$k;", self.space), key)
    }

    pub(super) fn expire(&self, key: &str, ttl: Value) -> Rendered {
        let (script, mut given) = self.keyed(&format!("EXPIRE {}:$k $t;", self.space), key);
        given.push(("t", ttl));
        (script, given)
    }

    pub(super) fn persist(&self, key: &str) -> Rendered {
        self.keyed(&format!("PERSIST {}:$k;", self.space), key)
    }

    /// `limit` is checked by the caller to lie in 1–[`MOST_KEYS`].
    pub(super) fn keys(&self, prefix: Option<&str>, after: Option<&str>, limit: u32) -> Rendered {
        let mut script = format!("{}KEYS FROM {}", self.tenancy, self.space);
        let mut given = Vec::new();
        if let Some(prefix) = prefix.filter(|prefix| !prefix.is_empty()) {
            script.push_str(" PREFIX $p");
            given.push(("p", Value::String(prefix.to_owned())));
        }
        if let Some(after) = after {
            script.push_str(" AFTER $a");
            given.push(("a", Value::String(after.to_owned())));
        }
        // Writing into a String cannot fail.
        let _ = write!(script, " LIMIT {limit};");
        (script, given)
    }

    pub(super) fn lock(&self, key: &str, holder: &str, ttl: Value) -> Rendered {
        self.held(
            &format!("SET {}:$k = $h IF ABSENT EXPIRE $t;", self.space),
            key,
            holder,
            Some(ttl),
        )
    }

    pub(super) fn extend(&self, key: &str, holder: &str, ttl: Value) -> Rendered {
        self.held(
            &format!("SET {}:$k = $h IF = $h EXPIRE $t;", self.space),
            key,
            holder,
            Some(ttl),
        )
    }

    /// Never a delete and never a write without an expiry (§4).
    pub(super) fn release(&self, key: &str, holder: &str) -> Rendered {
        self.held(
            &format!("SET {}:$k = 'free' IF = $h EXPIRE 1ms;", self.space),
            key,
            holder,
            None,
        )
    }

    fn held(&self, statement: &str, key: &str, holder: &str, ttl: Option<Value>) -> Rendered {
        let (script, mut given) = self.keyed(statement, key);
        given.push(("h", Value::String(holder.to_owned())));
        if let Some(ttl) = ttl {
            given.push(("t", ttl));
        }
        (script, given)
    }
}

/// A key as the wire spells it, back into the string this handle wrote (§2): a
/// quoted text key loses its quotes and its two escapes; any other kind is
/// returned as it came.
pub(super) fn unquoted(spelled: &str) -> String {
    let Some(inner) = spelled
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
    else {
        return spelled.to_owned();
    };
    let mut out = String::with_capacity(inner.len());
    let mut characters = inner.chars();
    while let Some(character) = characters.next() {
        if character == '\\'
            && let Some(escaped) = characters.next()
        {
            out.push(escaped);
        } else {
            out.push(character);
        }
    }
    out
}

#[cfg(test)]
mod tests;
