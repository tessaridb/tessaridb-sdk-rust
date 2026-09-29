//! One series, and appending a batch of events to it (protocol §5.9).
//!
//! # A batch is one transaction, and one attempt
//!
//! The node writes the batch whole or not at all, and answers how many landed.
//! Unlike a file, a batch is **not** idempotent: sent twice it lands twice. So it
//! is sent once — a transport failure after the request left may mean it landed,
//! and deciding whether to send it again belongs to a caller who knows whether
//! its events carry anything that would make a second copy harmless.
//!
//! # The events are rendered as TessariQL source, because the route reads that
//!
//! The body is one TessariQL value — an array of objects of literals — and §5.9
//! makes the rendering this client's job. The set of kinds an event needs is
//! small and each has one spelling; everything else is refused here, before a
//! byte is sent, rather than approximated into a value the caller did not mean.

use std::fmt::Write as _;

use crate::error::{Error, Result};
use crate::http::{Operations, Presenting, refusal};
use crate::value::{Number, Value};

/// A series, and the batches appended to it.
///
/// Holds the three names so that a caller writes them once. Obtained from
/// [`Operations::series`].
#[derive(Debug, Clone)]
pub struct Series {
    node: Operations,
    namespace: String,
    database: String,
    name: String,
}

impl Series {
    /// The series of this name, in this database, in this namespace.
    pub(crate) fn new(
        node: Operations,
        namespace: impl Into<String>,
        database: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            node,
            namespace: namespace.into(),
            database: database.into(),
            name: name.into(),
        }
    }

    /// Append these events in one transaction, and answer how many landed.
    ///
    /// Every event is a [`Value::Object`]. A field holding [`Value::None`] is
    /// left out, which is what absence means.
    ///
    /// # Errors
    ///
    /// [`Error::NotAnEvent`] before anything is sent, when an event is not an
    /// object or holds a kind an event cannot carry; [`Error::HttpRefused`] with
    /// the node's status — `400` for a name that is not one or an event the
    /// series refuses, `404` when the table is not a series, `401` or `403` on a
    /// closed store — and whatever the transport reports otherwise.
    pub async fn append(&self, events: &[Value]) -> Result<u64> {
        let body = batch(events)?;
        let path = format!("/series/{}/{}/{}", self.namespace, self.database, self.name);
        let reply = self
            .node
            .exchange("POST", &path, Some(body.as_bytes()), Presenting::Whatever)
            .await?;
        if reply.status != 200 {
            return Err(refusal(&reply));
        }
        serde_json::from_slice::<serde_json::Value>(&reply.body)
            .ok()
            .and_then(|answer| answer.get("appended").and_then(serde_json::Value::as_u64))
            .ok_or(Error::Malformed)
    }
}

/// The body: one TessariQL array of the events.
fn batch(events: &[Value]) -> Result<String> {
    let mut body = String::from("[");
    for (position, event) in events.iter().enumerate() {
        if !matches!(event, Value::Object(_)) {
            return Err(Error::NotAnEvent {
                reason: "an event is an object",
            });
        }
        if position > 0 {
            body.push_str(", ");
        }
        literal(event, &mut body)?;
    }
    body.push(']');
    Ok(body)
}

/// One value, spelled as §5.9 spells it.
fn literal(value: &Value, out: &mut String) -> Result<()> {
    match value {
        Value::Null => out.push_str("NULL"),
        Value::Bool(held) => out.push_str(if *held { "true" } else { "false" }),
        Value::Number(Number::Integer(held)) => {
            let _ = write!(out, "{held}");
        }
        Value::Number(Number::Float(held)) => {
            if !held.is_finite() {
                return Err(Error::NotAnEvent {
                    reason: "a float that is not finite has no spelling",
                });
            }
            // `{:?}` always carries a `.` or an exponent, which is what keeps
            // `1.0` a float rather than the integer `1`.
            let _ = write!(out, "{held:?}");
        }
        Value::Number(Number::Decimal { mantissa, scale }) => decimal(*mantissa, *scale, out),
        Value::String(held) => quoted(held, out),
        Value::Datetime { seconds, nanos } => {
            out.push_str("datetime '");
            instant(*seconds, *nanos, out)?;
            out.push('\'');
        }
        Value::Uuid(bytes) => {
            out.push_str("uuid '");
            for (position, byte) in bytes.iter().enumerate() {
                if matches!(position, 4 | 6 | 8 | 10) {
                    out.push('-');
                }
                let _ = write!(out, "{byte:02x}");
            }
            out.push('\'');
        }
        Value::Array(items) => {
            out.push('[');
            for (position, item) in items.iter().enumerate() {
                if matches!(item, Value::None) {
                    return Err(Error::NotAnEvent {
                        reason: "an array cannot hold an absence",
                    });
                }
                if position > 0 {
                    out.push_str(", ");
                }
                literal(item, out)?;
            }
            out.push(']');
        }
        Value::Object(fields) => {
            out.push('{');
            let mut first = true;
            for (name, held) in fields {
                if matches!(held, Value::None) {
                    continue;
                }
                out.push_str(if first { " " } else { ", " });
                first = false;
                quoted(name, out);
                out.push_str(": ");
                literal(held, out)?;
            }
            out.push_str(if first { "}" } else { " }" });
        }
        _ => {
            return Err(Error::NotAnEvent {
                reason: "an event carries null, booleans, numbers, strings, datetimes, uuids, arrays and objects",
            });
        }
    }
    Ok(())
}

/// A string in single quotes, with the two characters that need it escaped.
fn quoted(text: &str, out: &mut String) {
    out.push('\'');
    for character in text.chars() {
        if matches!(character, '\\' | '\'') {
            out.push('\\');
        }
        out.push(character);
    }
    out.push('\'');
}

/// `dec` and the decimal's digits, the point placed `scale` from the right.
fn decimal(mantissa: i128, scale: u32, out: &mut String) {
    let digits = mantissa.unsigned_abs().to_string();
    let scale = usize::try_from(scale).unwrap_or(usize::MAX);
    out.push_str("dec ");
    if mantissa < 0 {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(&digits);
        return;
    }
    let padded = if digits.len() <= scale {
        format!(
            "{}{digits}",
            "0".repeat(scale.saturating_sub(digits.len()).saturating_add(1))
        )
    } else {
        digits
    };
    let (whole, fraction) = padded.split_at(padded.len().saturating_sub(scale));
    let _ = write!(out, "{whole}.{fraction}");
}

/// An instant in RFC 3339, UTC.
fn instant(seconds: i64, nanos: u32, out: &mut String) -> Result<()> {
    let refused = || Error::NotAnEvent {
        reason: "a datetime outside the years 0 to 9999",
    };
    let days = seconds.div_euclid(86_400);
    let of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil(days).ok_or_else(refused)?;
    if !(0..=9_999).contains(&year) {
        return Err(refused());
    }
    let _ = write!(
        out,
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}",
        of_day.div_euclid(3_600),
        of_day.rem_euclid(3_600).div_euclid(60),
        of_day.rem_euclid(60)
    );
    if nanos > 0 {
        let _ = write!(out, ".{nanos:09}");
    }
    out.push('Z');
    Ok(())
}

/// The proleptic Gregorian date `days` after 1970-01-01.
fn civil(days: i64) -> Option<(i64, u32, u32)> {
    let shifted = days.checked_add(719_468)?;
    let era = shifted.div_euclid(146_097);
    let of_era = shifted.rem_euclid(146_097);
    let year_of_era = of_era
        .checked_sub(of_era.div_euclid(1_460))?
        .checked_add(of_era.div_euclid(36_524))?
        .checked_sub(of_era.div_euclid(146_096))?
        .div_euclid(365);
    let day_of_year = of_era.checked_sub(
        year_of_era
            .checked_mul(365)?
            .checked_add(year_of_era.div_euclid(4))?
            .checked_sub(year_of_era.div_euclid(100))?,
    )?;
    let shifted_month = day_of_year.checked_mul(5)?.checked_add(2)?.div_euclid(153);
    let day = day_of_year
        .checked_sub(
            shifted_month
                .checked_mul(153)?
                .checked_add(2)?
                .div_euclid(5),
        )?
        .checked_add(1)?;
    let month = if shifted_month < 10 {
        shifted_month.checked_add(3)?
    } else {
        shifted_month.checked_sub(9)?
    };
    let year = year_of_era
        .checked_add(era.checked_mul(400)?)?
        .checked_add(i64::from(month <= 2))?;
    Some((year, u32::try_from(month).ok()?, u32::try_from(day).ok()?))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn rendered(value: &Value) -> String {
        let mut out = String::new();
        literal(value, &mut out).unwrap_or_else(|error| panic!("{error}"));
        out
    }

    #[test]
    fn each_kind_an_event_carries_has_its_spelling() {
        assert_eq!(rendered(&Value::Null), "NULL");
        assert_eq!(rendered(&Value::Bool(true)), "true");
        assert_eq!(rendered(&Value::Number(Number::Integer(-12))), "-12");
        assert_eq!(rendered(&Value::Number(Number::Float(1.0))), "1.0");
        assert_eq!(rendered(&Value::Number(Number::Float(1.5e300))), "1.5e300");
        assert_eq!(
            rendered(&Value::Number(Number::Decimal {
                mantissa: -1_234,
                scale: 2
            })),
            "dec -12.34"
        );
        assert_eq!(
            rendered(&Value::Number(Number::Decimal {
                mantissa: 5,
                scale: 3
            })),
            "dec 0.005"
        );
        assert_eq!(
            rendered(&Value::String(r"it's \ ok".to_owned())),
            r"'it\'s \\ ok'"
        );
        assert_eq!(
            rendered(&Value::Datetime {
                seconds: 1_790_676_000,
                nanos: 123_456_789
            }),
            "datetime '2026-09-29T10:00:00.123456789Z'"
        );
        assert_eq!(
            rendered(&Value::Datetime {
                seconds: -1,
                nanos: 0
            }),
            "datetime '1969-12-31T23:59:59Z'"
        );
        assert_eq!(
            rendered(&Value::Uuid([
                0x01, 0x90, 0xa0, 0xb1, 0, 0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 1
            ])),
            "uuid '0190a0b1-0000-7000-8000-000000000001'"
        );
        let object = Value::Object(BTreeMap::from([
            ("odd key".to_owned(), Value::Array(vec![Value::Bool(false)])),
            ("gone".to_owned(), Value::None),
        ]));
        assert_eq!(rendered(&object), "{ 'odd key': [false] }");
    }

    #[test]
    fn a_kind_an_event_cannot_carry_is_refused_before_anything_is_sent() {
        for refused in [
            Value::Number(Number::Float(f64::NAN)),
            Value::Bytes(vec![1]),
            Value::Regex("a".to_owned()),
            Value::Array(vec![Value::None]),
        ] {
            let mut out = String::new();
            assert!(literal(&refused, &mut out).is_err(), "{refused:?}");
        }
        assert!(
            batch(&[Value::Bool(true)]).is_err(),
            "an event is an object"
        );
    }
}
