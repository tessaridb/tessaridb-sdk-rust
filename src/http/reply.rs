//! Reading one HTTP/1.1 response, and refusing the shapes this build will not
//! guess at.

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use crate::error::{Error, Result};

/// The largest header block this client will read.
///
/// A ceiling before an allocation, the same reason the wire protocol has one: a
/// length — or here, a header stream — arriving from a stranger is not a
/// promise. Generous next to the node's own headers, which run to a few hundred
/// bytes.
const HEADER_CEILING: usize = 64 * 1024;

/// The largest body this client will read into memory.
///
/// Matches the wire protocol's frame ceiling so the two surfaces refuse at the
/// same size rather than at two numbers nobody can remember.
const BODY_CEILING: u64 = 16 * 1024 * 1024;

/// What the node answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    /// The status code.
    pub status: u16,
    /// The body, which is empty when the response carries none.
    pub body: Vec<u8>,
    /// The length the node **declared**, kept even where no body followed it.
    ///
    /// On a `GET` this is the same number as `body.len()` and nothing reads it.
    /// On a `HEAD` it is the only thing the exchange carries — the node answers
    /// the length the equivalent `GET` would have sent and then sends nothing —
    /// so discarding it here, as this reader used to, threw away the entire
    /// point of the request.
    ///
    /// `None` where the node declared none, which this build has never seen: it
    /// is kept as a distinct state rather than defaulted to zero, because a file
    /// of unknown size and a file of no bytes are different answers and only one
    /// of them is true.
    pub length: Option<u64>,
}

/// Read one response off the stream.
///
/// # The body length comes from `Content-Length` and from nothing else
///
/// Measured against the shipped node: every route answers with
/// `Content-Length`, and none uses `Transfer-Encoding: chunked`. This build
/// therefore reads exactly that many bytes and **refuses** anything else rather
/// than inferring a length.
///
/// Refusing is the point. A client that guessed at a chunked body would return a
/// plausible wrong answer — the chunk-size lines read as data — and nothing
/// downstream could tell. The node changing its framing must break this loudly,
/// which is why the contract belongs in the protocol specification (LR-SDK-008)
/// rather than in this comment alone.
///
/// # Errors
///
/// [`Error::Malformed`] for a status line or header block that is not one,
/// [`Error::TooLarge`] past either ceiling, [`Error::Truncated`] if the stream
/// ends mid-response.
pub async fn read<S>(stream: &mut BufReader<S>, expects_body: bool) -> Result<Reply>
where
    S: AsyncRead + Unpin,
{
    let status = read_status(stream).await?;
    let length = read_headers(stream).await?;

    // A HEAD answers with the `Content-Length` the equivalent GET would carry
    // and sends no body at all. Trusting the header here is not a slow read, it
    // is a permanent hang — the bytes are never coming. The same holds for 204
    // and 304, which are defined to carry no body.
    let bodyless = !expects_body || status == 204 || status == 304;
    if bodyless {
        return Ok(Reply {
            status,
            body: Vec::new(),
            length,
        });
    }

    Ok(Reply {
        status,
        body: read_body(stream, length).await?,
        length,
    })
}

/// Read one response, sending a successful body to `sink` instead of holding it.
///
/// For the one route whose answer has no bound. `/backup` returns a snapshot of
/// the store, or its log from a position, which on any real store is larger than [`BODY_CEILING`] — so the
/// ceiling is not a safety limit there, it is a cap on whether the call works at
/// all. Nothing here is allocated in proportion to the body, so there is nothing
/// for a ceiling to bound and none is applied.
///
/// # A refusal never reaches the sink
///
/// `401`, `403`, `400` and `500` all answer with a small JSON object, and a
/// reader that copied every body would write `{"error":"…"}` into the caller's
/// backup file and return. That leaves a sixty-byte file which looks like a
/// backup, passes every check anyone makes at the time, and fails at restore
/// months later. So the status is decided **before any byte moves**: only `200`
/// copies, and every other status is read the ordinary bounded way and comes back
/// in `body` for the caller to turn into an error.
///
/// The returned [`Reply`] therefore reads differently by status. On `200` the
/// `body` is empty because those bytes went to `sink`, and `length` is how many
/// were copied. On anything else the `body` is the refusal, exactly as
/// [`read`] would have returned it, and `sink` has not been touched.
///
/// # Errors
///
/// As [`read`], plus [`Error::Io`] from `sink` itself — a full disk and a dropped
/// connection arrive as the same variant, which is worth knowing before treating
/// one as the other.
///
/// A stream that ends mid-copy is [`Error::Truncated`] **with bytes already
/// written**. Nothing here can un-write another party's sink, so the partial
/// write is reported rather than repaired.
pub async fn read_into<S, W>(stream: &mut BufReader<S>, sink: &mut W) -> Result<Reply>
where
    S: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let status = read_status(stream).await?;
    let length = read_headers(stream).await?;

    if status != 200 {
        // 204 and 304 are defined to carry no body; every other refusal carries
        // its sentence, and both are read the same bounded way `read` uses.
        let bodyless = status == 204 || status == 304;
        let body = if bodyless {
            Vec::new()
        } else {
            read_body(stream, length).await?
        };
        return Ok(Reply {
            status,
            body,
            length,
        });
    }

    let declared = length.ok_or(Error::Malformed)?;
    copy_exact(stream, sink, declared).await?;
    Ok(Reply {
        status,
        body: Vec::new(),
        length: Some(declared),
    })
}

/// The declared number of bytes, read into memory under the ceiling.
///
/// One function so there is one ceiling. Two copies of this check is how one of
/// them gets raised for a good reason and the other quietly stays behind.
async fn read_body<S>(stream: &mut BufReader<S>, length: Option<u64>) -> Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let length = length.ok_or(Error::Malformed)?;
    if length > BODY_CEILING {
        return Err(Error::TooLarge {
            length: u32::try_from(BODY_CEILING).unwrap_or(u32::MAX),
        });
    }
    // The narrowing is bounded by the check above, which is why it is here and
    // not at the top: `BODY_CEILING` fits a `usize` on every target this builds
    // for.
    let mut body = vec![0_u8; usize::try_from(length).map_err(|_| Error::Malformed)?];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|_| Error::Truncated)?;
    Ok(body)
}

/// How much of an unbounded body is held in memory at once.
///
/// The only allocation `copy_exact` makes, and it is fixed rather than derived
/// from the body: that is the whole difference between this path and the
/// buffering one.
const COPY_CHUNK: usize = 64 * 1024;

/// Move exactly `total` bytes from the stream to the sink.
///
/// `read_exact` rather than a copy loop that stops at end-of-stream: the node
/// declared a length, and a body that ends early is a truncated answer rather
/// than a short one. Copying whatever arrived and reporting success is how a
/// half-written backup becomes a file nobody knows is half-written.
async fn copy_exact<S, W>(stream: &mut BufReader<S>, sink: &mut W, total: u64) -> Result<()>
where
    S: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; COPY_CHUNK];
    let mut left = total;
    while left > 0 {
        // Total on every target: a `left` too large for a `usize` is necessarily
        // larger than one chunk, and a chunk is what would be taken anyway.
        let want = usize::try_from(left).unwrap_or(COPY_CHUNK).min(COPY_CHUNK);
        let slice = buffer.get_mut(..want).ok_or(Error::Malformed)?;
        stream
            .read_exact(slice)
            .await
            .map_err(|_| Error::Truncated)?;
        sink.write_all(slice).await?;
        left = left.saturating_sub(u64::try_from(want).map_err(|_| Error::Malformed)?);
    }
    // The caller handed us a writer, not a file; whatever buffering it does is
    // its own, and leaving the last chunk inside it would end the call with the
    // backup incomplete and nothing saying so.
    sink.flush().await?;
    Ok(())
}

/// Read `HTTP/1.1 <code> <reason>` and keep the code.
async fn read_status<S>(stream: &mut BufReader<S>) -> Result<u16>
where
    S: AsyncRead + Unpin,
{
    let line = read_line(stream).await?;
    let mut parts = line.split(' ');
    let version = parts.next().ok_or(Error::Malformed)?;
    if !version.starts_with("HTTP/1.") {
        // Not "unsupported version" but "this is not the protocol at all": the
        // greeting's counterpart, refused before anything else is read.
        return Err(Error::NotThisProtocol);
    }
    parts
        .next()
        .ok_or(Error::Malformed)?
        .parse::<u16>()
        .map_err(|_| Error::Malformed)
}

/// Read headers to the blank line; answer with `Content-Length` if it was given.
async fn read_headers<S>(stream: &mut BufReader<S>) -> Result<Option<u64>>
where
    S: AsyncRead + Unpin,
{
    let mut length = None;
    let mut spent = 0_usize;
    loop {
        let line = read_line(stream).await?;
        if line.is_empty() {
            return Ok(length);
        }
        spent = spent.saturating_add(line.len());
        if spent > HEADER_CEILING {
            return Err(Error::TooLarge {
                length: u32::try_from(HEADER_CEILING).unwrap_or(u32::MAX),
            });
        }

        let Some((name, value)) = line.split_once(':') else {
            return Err(Error::Malformed);
        };
        // Field names are case-insensitive, and a client that compares them
        // literally works against one server and mysteriously not against the
        // next. Compared in lowercase rather than hoping for a convention.
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();

        if name == "transfer-encoding" {
            // Refused, never guessed at. See this module's documentation.
            return Err(Error::Malformed);
        }
        if name == "content-length" {
            length = Some(value.parse::<u64>().map_err(|_| Error::Malformed)?);
        }
    }
}

/// One CRLF-terminated line, without its terminator.
async fn read_line<S>(stream: &mut BufReader<S>) -> Result<String>
where
    S: AsyncRead + Unpin,
{
    let mut line = String::new();
    let read = stream
        .read_line(&mut line)
        .await
        .map_err(|_| Error::Truncated)?;
    if read == 0 {
        return Err(Error::Truncated);
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_owned())
}
