//! A ranking under a key — members with scores (cache contract 1.1, §2a).

use tokio::io::{AsyncRead, AsyncWrite};

use super::some;
use crate::cache::Space;
use crate::cache::statements::Bound;
use crate::error::{Error, Result};
use crate::value::{Number, Value};

impl<S> Space<'_, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Rank `scores` under `key`; how many members were new.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no members or a score that is not finite;
    /// whatever the node refused.
    pub async fn scores_add(&mut self, key: &str, scores: Vec<(String, f64)>) -> Result<i64> {
        some(&scores, "a ranking write needs a member")?;
        if scores.iter().any(|(_, score)| !score.is_finite()) {
            return Err(Error::NotACacheArgument {
                reason: "a score is a finite number",
            });
        }
        self.integer(self.statements.scores_add(key, scores)).await
    }

    /// Add `by` to a member's score; its new score.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for a step that is not finite; whatever the node refused.
    pub async fn scores_incr(&mut self, key: &str, member: &str, by: f64) -> Result<f64> {
        if !by.is_finite() {
            return Err(Error::NotACacheArgument {
                reason: "a score is a finite number",
            });
        }
        match self
            .answer(self.statements.scores_incr(key, member, by))
            .await?
        {
            Value::Number(Number::Float(score)) => Ok(score),
            _ => Err(Error::Malformed),
        }
    }

    /// Remove `members`; how many went.
    ///
    /// # Errors
    ///
    /// [`Error::NotACacheArgument`] for no members; whatever the node refused.
    pub async fn scores_remove(&mut self, key: &str, members: Vec<String>) -> Result<i64> {
        some(&members, "a ranking removal needs a member")?;
        self.integer(self.statements.scores_remove(key, members))
            .await
    }

    /// A member's score, or `None` when it is not ranked.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn scores_score(&mut self, key: &str, member: &str) -> Result<Option<f64>> {
        match self
            .answer(self.statements.scores_of("score", key, member))
            .await?
        {
            Value::Number(Number::Float(score)) => Ok(Some(score)),
            Value::None => Ok(None),
            _ => Err(Error::Malformed),
        }
    }

    /// A member's place from the lowest score, from 0, or `None` when it is not ranked.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn scores_rank(&mut self, key: &str, member: &str) -> Result<Option<i64>> {
        match self
            .answer(self.statements.scores_of("rank", key, member))
            .await?
        {
            Value::Number(Number::Integer(place)) => Ok(Some(place)),
            Value::None => Ok(None),
            _ => Err(Error::Malformed),
        }
    }

    /// The members at places `start` to `stop`, both included, either counted
    /// from the highest when negative — lowest first.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn scores_range(
        &mut self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(String, f64)>> {
        self.ranked(self.statements.scores_range(key, start, stop))
            .await
    }

    /// The members whose score is from `low` to `high`, both included, an absent
    /// bound leaving that side open — lowest first.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn scores_between(
        &mut self,
        key: &str,
        low: Option<f64>,
        high: Option<f64>,
    ) -> Result<Vec<(String, f64)>> {
        self.ranked(self.statements.scores_between(key, low, high))
            .await
    }

    /// `[member, score]` pairs as `(member, score)`.
    async fn ranked(&mut self, bound: Bound) -> Result<Vec<(String, f64)>> {
        self.elements(bound)
            .await?
            .into_iter()
            .map(|pair| match pair {
                Value::Array(pair) => match <[Value; 2]>::try_from(pair) {
                    Ok([Value::String(member), Value::Number(Number::Float(score))]) => {
                        Ok((member, score))
                    }
                    _ => Err(Error::Malformed),
                },
                _ => Err(Error::Malformed),
            })
            .collect()
    }
}
