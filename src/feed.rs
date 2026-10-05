//! Changes, as they arrive.
//!
//! # This is a cursor, not a callback
//!
//! A subscriber that is busy is *behind*, never lossy — the log is the buffer.
//! So the honest shape is one that lets a caller take the next change when it is
//! ready for it. A callback would invert that and put the node's pace in charge
//! of the subscriber's.
//!
//! # The node will drop a subscriber that stops reading
//!
//! When a client stops reading, its socket fills and the node's write blocks.
//! Rather than hold a thread indefinitely the node ends the connection after
//! about thirty seconds. Nothing is lost — resume from the last handled
//! [`Change::sequence`] with [`Follow::resuming_after`] — but **a reconnect path
//! is not optional**, and that arithmetic is what makes reconnecting correct.

use tokio::io::AsyncRead;

use crate::error::{Error, Result};
use crate::wire::frame::{self, Kind};
use crate::wire::push::Change;

/// A stream of changes on a connection that asked to follow.
#[derive(Debug)]
pub struct Feed<S> {
    stream: S,
    finished: bool,
}

impl<S> Feed<S>
where
    S: AsyncRead + Unpin,
{
    pub(crate) const fn new(stream: S) -> Self {
        Self {
            stream,
            finished: false,
        }
    }

    /// The next change, or `None` when the node closed the stream.
    ///
    /// `None` is not an error and not the end of the data — it is this
    /// connection ending. The changes after it are still in the log, and a new
    /// subscription resuming after the last handled sequence will deliver them.
    pub async fn next(&mut self) -> Result<Option<Change>> {
        // Once the stream has ended, keep saying so rather than reading a closed
        // socket again — the second read's error would be about the socket
        // rather than about what happened.
        if self.finished {
            return Ok(None);
        }
        match frame::read(&mut self.stream).await? {
            None => {
                self.finished = true;
                Ok(None)
            }
            Some((Kind::Change, body)) => Change::decode(&body).map(Some),
            // A refusal can still arrive here: the node answers a subscription
            // it will not serve — an unwatchable table, or a tenancy the caller
            // may not see — with its own words rather than with silence.
            Some((Kind::Refusal, body)) => {
                self.finished = true;
                let (class, message) = crate::refusal::read(&body);
                Err(Error::Refused { message, class })
            }
            Some((other, _)) => {
                self.finished = true;
                Err(Error::UnknownFrame { tag: other.tag() })
            }
        }
    }

    /// Whether this feed has ended.
    #[must_use]
    pub const fn is_finished(&self) -> bool {
        self.finished
    }
}
