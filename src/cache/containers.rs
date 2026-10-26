//! A key holding a list, a hash, a set of members or a ranking (cache contract
//! 1.1, §2a).
//!
//! Writes are the node's container statements, atomic per key; reads are
//! functions over the key's value, so every call is still one statement. A read
//! of a key that is not there answers the empty form — an empty list, map or
//! set, `0`, `false` — and an element, field, score or rank that is not there
//! answers `None`.

use std::collections::BTreeMap;

use tokio::io::{AsyncRead, AsyncWrite};

use super::Space;
use super::statements::{Bound, End};
use crate::error::{Error, Result};
use crate::value::{Number, Value};

mod rankings;

impl<S> Space<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Push `values` onto the list under `key`, at `end`; the new length.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no values; whatever the node refused.
    pub async fn list_push(&mut self, key: &str, end: End, values: Vec<Value>) -> Result<i64> {
        some(&values, "a push needs a value")?;
        self.integer(self.statements.list_push(key, end, values))
            .await
    }

    /// Take one element from `end`, or `None` when the list is not there.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn list_pop(&mut self, key: &str, end: End) -> Result<Option<Value>> {
        self.present(self.statements.list_pop(key, end, None)).await
    }

    /// Take up to `count` elements from `end`, in the order taken.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a negative count; whatever the node refused.
    pub async fn list_pop_many(&mut self, key: &str, end: End, count: i64) -> Result<Vec<Value>> {
        not_negative(count, "a count is not negative")?;
        self.elements(self.statements.list_pop(key, end, Some(count)))
            .await
    }

    /// `count` elements from `start`.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a negative start or count; whatever the node refused.
    pub async fn list_range(&mut self, key: &str, start: i64, count: i64) -> Result<Vec<Value>> {
        not_negative(start, "a range starts at or after the first element")?;
        not_negative(count, "a count is not negative")?;
        self.elements(self.statements.list_range(key, start, count))
            .await
    }

    /// The element at `index`, a negative one counted from the back.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn list_at(&mut self, key: &str, index: i64) -> Result<Option<Value>> {
        self.present(self.statements.list_at(key, index)).await
    }

    /// How many elements the list holds.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn list_len(&mut self, key: &str) -> Result<i64> {
        self.length(self.statements.list_len(key)).await
    }

    /// Remove `value` — every occurrence, or at most `count` from the front.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a negative count; whatever the node refused.
    pub async fn list_remove(
        &mut self,
        key: &str,
        value: Value,
        count: Option<i64>,
    ) -> Result<i64> {
        if let Some(count) = count {
            not_negative(count, "a count is not negative")?;
        }
        self.integer(self.statements.list_remove(key, value, count))
            .await
    }

    /// Keep the elements from `start` to `stop`, both included, either counted
    /// from the back when negative.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn list_trim(&mut self, key: &str, start: i64, stop: i64) -> Result<()> {
        self.answer(self.statements.list_trim(key, start, stop))
            .await?;
        Ok(())
    }

    /// Set `fields` of the hash under `key`; how many were new.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no fields; whatever the node refused.
    pub async fn hash_set(&mut self, key: &str, fields: Vec<(String, Value)>) -> Result<i64> {
        some(&fields, "a hash write needs a field")?;
        self.integer(self.statements.hash_set(key, fields)).await
    }

    /// One field, or `None` when it is not there.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn hash_get(&mut self, key: &str, field: &str) -> Result<Option<Value>> {
        self.present(self.statements.hash_get(key, field)).await
    }

    /// Every field.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn hash_get_all(&mut self, key: &str) -> Result<BTreeMap<String, Value>> {
        match self.value(self.statements.get(key)).await? {
            Value::Object(fields) => Ok(fields),
            Value::None => Ok(BTreeMap::new()),
            _ => Err(Error::Malformed),
        }
    }

    /// Remove `fields`; how many went.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no fields; whatever the node refused.
    pub async fn hash_remove(&mut self, key: &str, fields: Vec<String>) -> Result<i64> {
        some(&fields, "a hash removal needs a field")?;
        self.integer(self.statements.hash_remove(key, fields)).await
    }

    /// Add `by` to a field; its new value.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn hash_incr(&mut self, key: &str, field: &str, by: i64) -> Result<i64> {
        self.integer(self.statements.hash_incr(key, field, by))
            .await
    }

    /// How many fields the hash holds.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn hash_len(&mut self, key: &str) -> Result<i64> {
        self.length(self.statements.hash_len(key)).await
    }

    /// Add `members`; how many were new.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no members; whatever the node refused.
    pub async fn members_add(&mut self, key: &str, members: Vec<Value>) -> Result<i64> {
        some(&members, "a set write needs a member")?;
        self.integer(self.statements.members("ADD", key, members))
            .await
    }

    /// Remove `members`; how many went.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no members; whatever the node refused.
    pub async fn members_remove(&mut self, key: &str, members: Vec<Value>) -> Result<i64> {
        some(&members, "a set removal needs a member")?;
        self.integer(self.statements.members("REMOVE", key, members))
            .await
    }

    /// Whether `member` is in the set.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn members_has(&mut self, key: &str, member: Value) -> Result<bool> {
        self.flag(self.statements.members_has(key, member)).await
    }

    /// Every member.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn members_all(&mut self, key: &str) -> Result<Vec<Value>> {
        match self.value(self.statements.get(key)).await? {
            Value::Set(members) => Ok(members),
            Value::None => Ok(Vec::new()),
            _ => Err(Error::Malformed),
        }
    }

    /// How many members the set holds.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn members_len(&mut self, key: &str) -> Result<i64> {
        self.length(self.statements.members_len(key)).await
    }

    async fn answer(&mut self, bound: Bound) -> Result<Value> {
        self.value(bound).await
    }

    async fn integer(&mut self, bound: Bound) -> Result<i64> {
        match self.answer(bound).await? {
            Value::Number(Number::Integer(held)) => Ok(held),
            _ => Err(Error::Malformed),
        }
    }

    /// A length, `0` for a key that is not there.
    async fn length(&mut self, bound: Bound) -> Result<i64> {
        match self.answer(bound).await? {
            Value::Number(Number::Integer(held)) => Ok(held),
            Value::None => Ok(0),
            _ => Err(Error::Malformed),
        }
    }

    async fn present(&mut self, bound: Bound) -> Result<Option<Value>> {
        let found = self.answer(bound).await?;
        Ok((found != Value::None).then_some(found))
    }

    /// Elements, none for a key that is not there.
    async fn elements(&mut self, bound: Bound) -> Result<Vec<Value>> {
        match self.answer(bound).await? {
            Value::Array(items) => Ok(items),
            Value::None => Ok(Vec::new()),
            _ => Err(Error::Malformed),
        }
    }
}

fn some<T>(items: &[T], reason: &'static str) -> Result<()> {
    if items.is_empty() {
        return Err(Error::NotACacheArgument { reason });
    }
    Ok(())
}

fn not_negative(count: i64, reason: &'static str) -> Result<()> {
    if count < 0 {
        return Err(Error::NotACacheArgument { reason });
    }
    Ok(())
}
