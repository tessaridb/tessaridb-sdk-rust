//! Frames, the greeting, and the one rule that makes reading them safe.
//!
//! # A length from a stranger is not a promise
//!
//! Every frame declares its own size. A client that allocated whatever a peer
//! declared would be one packet away from being out of memory, so a length above
//! [`CEILING`] is refused **before anything is allocated** and the connection is
//! finished.
//!
//! The ceiling applies outbound as well. A peer that emits what it would refuse
//! to read is running two protocols.
//!
//! # These integers are plain
//!
//! Everything here is plain big-endian. The **value** codec inverts the sign bit
//! of an `i64`; this layer does not. See [`crate::codec`].

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};
use crate::protocol::{CEILING, GREETING, GREETING_BYTES, HEADER, MAJOR, MINOR};

/// What a frame is.
///
/// Numbered explicitly and **never renumbered**: a byte that once meant one
/// thing cannot be asked about after the fact by a peer of a different build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Kind {
    /// A script to run, with optional credentials.
    Request,
    /// One outcome per statement.
    Answer,
    /// The store refused, and said why.
    Refusal,
    /// Follow the changes from a position onward.
    Subscribe,
    /// One change, sent because it happened.
    Change,
    /// The node did not run the request and names the node that should
    /// (protocol §3.12, minor 1).
    Elsewhere,
    /// Unseal, seal, change the passphrase or ask the seal status, the
    /// passphrase a field of its own (protocol §3.14). Numbered 17: the link
    /// nodes use between themselves holds 6-12 and 14-16.
    Vault,
    /// How far a feed that named a condition read (protocol §3.15, minor 4).
    Progress,
}

impl Kind {
    /// The byte this kind is written as.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Request => 1,
            Self::Answer => 2,
            Self::Refusal => 3,
            Self::Subscribe => 4,
            Self::Change => 5,
            Self::Elsewhere => 13,
            Self::Vault => 17,
            Self::Progress => 37,
        }
    }

    /// The kind a byte names, or `None` for one this build does not have.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Request),
            2 => Some(Self::Answer),
            3 => Some(Self::Refusal),
            4 => Some(Self::Subscribe),
            5 => Some(Self::Change),
            13 => Some(Self::Elsewhere),
            17 => Some(Self::Vault),
            37 => Some(Self::Progress),
            _ => None,
        }
    }
}

/// Exchange greetings, and return the peer's **minor**.
///
/// Both sides send; both check. Refusing here rather than mid-conversation is
/// what makes a version mismatch one clear message instead of a decode failure
/// somewhere in the middle that reads like corruption.
///
/// A differing **major** is that refusal. A differing **minor** is not: the two
/// sides agree about frames and about every value, and the newer one simply
/// knows more outcome kinds — which the older one steps over by their lengths.
/// The peer's minor is returned rather than discarded because it is the only
/// thing that decides what this client may *send* to an older node.
pub async fn greet<S>(stream: &mut S) -> Result<u8>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut sent = Vec::with_capacity(GREETING_BYTES);
    sent.extend_from_slice(GREETING);
    sent.push(MAJOR);
    sent.push(MINOR);
    stream.write_all(&sent).await?;
    stream.flush().await?;

    // The magic is read and judged on its own, before the version bytes. A peer
    // that is not a node owes us nothing and may send four bytes and hang up;
    // reading all six at once would report that as an unexpected end of file,
    // which sends the reader to the network for a problem that is an address.
    let mut magic = [0_u8; 4];
    stream.read_exact(&mut magic).await?;
    if magic != *GREETING {
        return Err(Error::NotThisProtocol);
    }

    let mut version = [0_u8; 2];
    stream.read_exact(&mut version).await?;
    let major = version.first().copied().ok_or(Error::Truncated)?;
    let minor = version.get(1).copied().ok_or(Error::Truncated)?;
    if major == MAJOR {
        Ok(minor)
    } else {
        Err(Error::WrongVersion {
            found: major,
            supported: MAJOR,
        })
    }
}

/// Write one frame.
pub async fn write<S>(stream: &mut S, kind: Kind, body: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let length = u32::try_from(body.len()).unwrap_or(u32::MAX);
    if length > CEILING {
        return Err(Error::TooLarge { length });
    }
    let mut header = [0_u8; HEADER];
    // Writing through a fixed array rather than three `write_all` calls, so one
    // frame is one syscall's worth of work and a partially written header is not
    // a state this function can leave behind.
    if let Some(slot) = header.first_mut() {
        *slot = kind.tag();
    }
    if let Some(slot) = header.get_mut(1..HEADER) {
        slot.copy_from_slice(&length.to_be_bytes());
    }
    stream.write_all(&header).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

/// Read one frame, or `None` when the peer hung up cleanly between frames.
///
/// Nothing at all is a clean goodbye; a partial header is not.
pub async fn read<S>(stream: &mut S) -> Result<Option<(Kind, Vec<u8>)>>
where
    S: AsyncRead + Unpin,
{
    let mut header = [0_u8; HEADER];
    let mut held = 0_usize;
    while held < HEADER {
        let Some(slot) = header.get_mut(held..) else {
            break;
        };
        let read = stream.read(slot).await?;
        if read == 0 {
            return if held == 0 {
                Ok(None)
            } else {
                Err(Error::Truncated)
            };
        }
        held = held.saturating_add(read);
    }

    let tag = header.first().copied().ok_or(Error::Truncated)?;
    let Some(kind) = Kind::from_tag(tag) else {
        return Err(Error::UnknownFrame { tag });
    };
    let length_bytes = header.get(1..HEADER).ok_or(Error::Truncated)?;
    let length =
        u32::from_be_bytes(<[u8; 4]>::try_from(length_bytes).map_err(|_| Error::Truncated)?);
    // Checked before the allocation, which is the whole point of the ceiling.
    if length > CEILING {
        return Err(Error::TooLarge { length });
    }
    let capacity = usize::try_from(length).map_err(|_| Error::Malformed)?;
    let mut body = vec![0_u8; capacity];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|_| Error::Truncated)?;
    Ok(Some((kind, body)))
}

// --- body primitives: plain big-endian, this layer only ---

pub(crate) fn put_u32(into: &mut Vec<u8>, value: u32) {
    into.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_u64(into: &mut Vec<u8>, value: u64) {
    into.extend_from_slice(&value.to_be_bytes());
}

pub(crate) fn put_text(into: &mut Vec<u8>, text: &str) {
    put_bytes(into, text.as_bytes());
}

pub(crate) fn put_bytes(into: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(into, u32::try_from(bytes.len()).unwrap_or(u32::MAX));
    into.extend_from_slice(bytes);
}

/// A cursor over a frame body.
pub(crate) struct Body<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Body<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(count).ok_or(Error::Malformed)?;
        let slice = self.bytes.get(self.at..end).ok_or(Error::Malformed)?;
        self.at = end;
        Ok(slice)
    }

    pub(crate) fn take_u8(&mut self) -> Result<u8> {
        self.take(1)?.first().copied().ok_or(Error::Malformed)
    }

    pub(crate) fn take_u32(&mut self) -> Result<u32> {
        let bytes = <[u8; 4]>::try_from(self.take(4)?).map_err(|_| Error::Malformed)?;
        Ok(u32::from_be_bytes(bytes))
    }

    pub(crate) fn take_u64(&mut self) -> Result<u64> {
        let bytes = <[u8; 8]>::try_from(self.take(8)?).map_err(|_| Error::Malformed)?;
        Ok(u64::from_be_bytes(bytes))
    }

    pub(crate) fn take_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.take_u32()?;
        let len = usize::try_from(len).map_err(|_| Error::Malformed)?;
        Ok(self.take(len)?.to_vec())
    }

    pub(crate) fn take_text(&mut self) -> Result<String> {
        String::from_utf8(self.take_bytes()?).map_err(|_| Error::Malformed)
    }

    /// How many bytes of this body have not been read.
    ///
    /// Asked where a field is a later addition to a frame: the frame grows by
    /// appending, so absent means the node had nothing to add rather than that
    /// the body was cut short. That distinction is only safe to draw because
    /// each outcome is decoded inside a cursor bounded by its own length.
    pub(crate) const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.at)
    }
}
