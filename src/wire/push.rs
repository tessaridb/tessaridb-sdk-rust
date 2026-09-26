//! Being told rather than asking: what a client subscribes to, and what arrives.

use crate::codec::decode;
use crate::error::{Error, Result};
use crate::value::Value;
use crate::wire::frame::{Body, put_text, put_u64};

/// What a client asked to follow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Follow {
    /// The first log position to read, **inclusive**.
    ///
    /// A position rather than "from now", so a client that was disconnected
    /// resumes exactly where it stopped.
    ///
    /// The arithmetic belongs to the client and is easy to get wrong in both
    /// directions: resuming with the position already handled delivers it twice,
    /// resuming with one not yet reached reports being caught up. Both are
    /// silent. Prefer [`Follow::resuming_after`] over setting this by hand.
    pub from: u64,
    /// The table to watch, or every table in the session's database.
    pub table: Option<String>,
    /// Where to resume a feed over a split table: the [`Change::cursor`] of the
    /// last change handled.
    ///
    /// A split table's changes come from several logs that count separately,
    /// so no one `sequence` says where such a feed was. The node resumes
    /// *after* the change that carried this cursor — no `+1` here. Opaque:
    /// store it and send it back, never build one.
    pub cursor: Option<String>,
}

impl Follow {
    /// Everything the log still holds, for every table.
    #[must_use]
    pub const fn everything() -> Self {
        Self {
            from: 0,
            table: None,
            cursor: None,
        }
    }

    /// Resume after the last change actually handled.
    ///
    /// This is the `+1` that `from` being inclusive requires, done once here
    /// rather than at every call site that could get it wrong.
    #[must_use]
    pub const fn resuming_after(sequence: u64) -> Self {
        Self {
            from: sequence.saturating_add(1),
            table: None,
            cursor: None,
        }
    }

    /// Resume a feed over a split table after the change that carried `cursor`.
    #[must_use]
    pub fn resuming_at(cursor: impl Into<String>) -> Self {
        Self {
            from: 0,
            table: None,
            cursor: Some(cursor.into()),
        }
    }

    /// The same subscription, narrowed to one table.
    #[must_use]
    pub fn to_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// The body of a subscribe frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        put_u64(&mut body, self.from);
        match &self.table {
            Some(name) => {
                body.push(1);
                put_text(&mut body, name);
            }
            None => body.push(0),
        }
        // Last and only when present: a body without it is the frame a node
        // before the cursor existed reads.
        if let Some(cursor) = &self.cursor {
            put_text(&mut body, cursor);
        }
        body
    }
}

/// The byte a change carries to say what happened.
mod kind {
    pub(super) const WRITTEN: u8 = 0;
    pub(super) const REMOVED: u8 = 1;
}

/// What became of a record.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Became {
    /// It now holds this value.
    Written(Value),
    /// It is no longer there.
    Removed,
}

/// One change, as it arrives.
///
/// The table is named rather than identified: an id is meaningless outside the
/// node that minted it, and the catalog is on the node.
#[derive(Debug, Clone, PartialEq)]
pub struct Change {
    /// The commit this change was part of.
    ///
    /// Shared by every change of one commit, which is what lets a subscriber
    /// apply them as the unit they were written as — and what it stores in order
    /// to resume. Pass it to [`Follow::resuming_after`].
    pub sequence: u64,
    /// The table, by name.
    pub table: String,
    /// The record's identity, as the node spells it.
    pub id: String,
    /// What became of it.
    pub became: Became,
    /// On a feed over a split table, where to resume after this change — pass
    /// it to [`Follow::resuming_at`]. `None` on every other feed, where
    /// `sequence` is the position.
    pub cursor: Option<String>,
}

impl Change {
    /// Read one out of a change frame's body.
    pub fn decode(body: &[u8]) -> Result<Self> {
        let mut reader = Body::new(body);
        let sequence = reader.take_u64()?;
        let table = reader.take_text()?;
        let id = reader.take_text()?;
        let became = match reader.take_u8()? {
            kind::WRITTEN => {
                let bytes = reader.take_bytes()?;
                Became::Written(decode(&bytes).map_err(Error::from)?)
            }
            kind::REMOVED => Became::Removed,
            // Unlike an outcome tag, this one is not forward-compatible by
            // design: an unrecognised value means the frame's shape is not what
            // this build expects, so nothing after it can be located.
            _ => return Err(Error::Malformed),
        };
        // The frame grows by appending: bytes after the change are its cursor,
        // and their absence means the feed has none.
        let cursor = if reader.remaining() > 0 {
            Some(reader.take_text()?)
        } else {
            None
        };
        Ok(Self {
            sequence,
            table,
            id,
            became,
            cursor,
        })
    }
}
