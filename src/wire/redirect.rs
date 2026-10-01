//! The `Elsewhere` frame (protocol §3.12): a node did not run the request, and
//! names the node that should.
//!
//! A redirect is an instruction rather than a failure, so it has its own type
//! and never reaches a caller through [`crate::Error::Refused`].

use crate::error::{Error, Result};
use crate::wire::frame::Body;

/// Whether a redirect says where the data lives, or only where to go this once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    /// Where the data lives now: a client may stay there.
    Settled,
    /// Where to go for this request only: a client must not stay.
    Transient,
}

/// Where a node sent a request instead of running it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Redirect {
    /// The node to expect at [`Self::endpoint`].
    pub node: [u8; 16],
    /// The leadership that node last claimed.
    pub epoch: u64,
    /// Whether to stay there.
    pub settlement: Settlement,
    /// The address to dial.
    pub endpoint: String,
}

impl Redirect {
    /// Decode an `Elsewhere` body: node, epoch, settlement, endpoint, in that
    /// order. A settlement byte that is neither 1 nor 2 is malformed rather than
    /// a third meaning, because zero is what a truncated buffer holds.
    pub fn decode(body: &[u8]) -> Result<Self> {
        let mut body = Body::new(body);
        let node = <[u8; 16]>::try_from(body.take(16)?).map_err(|_| Error::Malformed)?;
        let epoch = body.take_u64()?;
        let settlement = match body.take_u8()? {
            1 => Settlement::Settled,
            2 => Settlement::Transient,
            _ => return Err(Error::Malformed),
        };
        let endpoint = body.take_text()?;
        Ok(Self {
            node,
            epoch,
            settlement,
            endpoint,
        })
    }
}
