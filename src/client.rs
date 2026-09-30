//! The connection, and what you can ask it.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::error::{Error, Result};
use crate::feed::Feed;
use crate::value::Value;
use crate::wire::frame::{self, Kind};
use crate::wire::message::{Answer, Request, decode_answers};
use crate::wire::push::Follow;

/// A connection to one node.
///
/// # One connection is one session
///
/// `USE NAMESPACE prod;` is still in force in the next statement on this
/// client — that is what a connection means. Two clients are two sessions and
/// share nothing but the store.
#[derive(Debug)]
pub struct Client<S = TcpStream> {
    stream: S,
    /// The minor version the node said at the greeting, which decides what this
    /// client may send it; `None` for a stream handed in already greeted, whose
    /// minor this client was never told.
    peer_minor: Option<u8>,
}

impl Client<TcpStream> {
    /// Connect to `address` and exchange greetings.
    ///
    /// The greeting is where a wrong protocol or a wrong version is refused, so
    /// a mismatch is one clear error here rather than a decode failure later
    /// that reads like corruption.
    pub async fn connect(address: impl ToSocketAddrs) -> Result<Self> {
        let mut stream = TcpStream::connect(address).await?;
        // Nagle batches small writes, and every frame here ends with a flush
        // because the peer is waiting for it. Leaving it on adds latency to
        // exactly the pattern this protocol is made of.
        stream.set_nodelay(true)?;
        // Kept because the vault frame is minor-gated (protocol §2.3): it is the
        // one thing this client sends that an older node cannot read. A stream
        // handed in through `with_stream` has no known minor, and gets `None`
        // rather than an invented number.
        let peer_minor = Some(frame::greet(&mut stream).await?);
        Ok(Self { stream, peer_minor })
    }
}

impl<S> Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Drive an already-connected, already-greeted stream.
    ///
    /// For tests and for callers that own their transport. [`Client::connect`]
    /// is the ordinary way in.
    ///
    /// Such a client does not know the node's minor, so a vault call on it is
    /// sent without the version check [`Client::connect`] makes.
    pub const fn with_stream(stream: S) -> Self {
        Self {
            stream,
            peer_minor: None,
        }
    }

    /// Run a script and read what came back.
    ///
    /// One [`Answer`] per statement, in order.
    pub async fn run(
        &mut self,
        script: &str,
        credentials: Option<(&str, &str)>,
    ) -> Result<Vec<Answer>> {
        let mut request = Request::new(script);
        if let Some((name, password)) = credentials {
            request = request.as_user(name, password);
        }
        self.send(&request).await
    }

    /// Run a script whose parameters take the values bound to them.
    ///
    /// The values travel in the store's own codec, so all seventeen types cross
    /// unchanged and the node never has to *read* one. That is what keeps the
    /// grammar's rule intact at this distance: a supplied value cannot become
    /// syntax, and nothing about being remote gives that back.
    pub async fn run_with<I, K>(
        &mut self,
        script: &str,
        credentials: Option<(&str, &str)>,
        parameters: I,
    ) -> Result<Vec<Answer>>
    where
        I: IntoIterator<Item = (K, Value)>,
        K: Into<String>,
    {
        let mut request = Request::new(script);
        if let Some((name, password)) = credentials {
            request = request.as_user(name, password);
        }
        for (name, value) in parameters {
            request.parameters.insert(name.into(), value);
        }
        self.send(&request).await
    }

    /// Send a request already built.
    pub async fn send(&mut self, request: &Request) -> Result<Vec<Answer>> {
        frame::write(&mut self.stream, Kind::Request, &request.encode()).await?;
        let Some((kind, body)) = frame::read(&mut self.stream).await? else {
            return Err(Error::Truncated);
        };
        match kind {
            Kind::Answer => decode_answers(&body),
            Kind::Refusal => Err(refusal(body)),
            // A node does not send a request, and a change only arrives on a
            // connection that asked to follow — which this one has not.
            Kind::Request | Kind::Subscribe | Kind::Change | Kind::Vault => {
                Err(Error::UnknownFrame { tag: kind.tag() })
            }
        }
    }

    /// Send one vault frame (protocol §3.14) and read the status back.
    pub(crate) async fn vault_frame(&mut self, body: &[u8]) -> Result<Value> {
        if let Some(found) = self.peer_minor
            && found < VAULT_MINOR
        {
            return Err(Error::NodeTooOld {
                found,
                needed: VAULT_MINOR,
            });
        }
        frame::write(&mut self.stream, Kind::Vault, body).await?;
        let Some((kind, answer)) = frame::read(&mut self.stream).await? else {
            return Err(Error::Truncated);
        };
        match kind {
            Kind::Answer => match decode_answers(&answer)?.pop() {
                Some(Answer::Value { value, .. }) => Ok(value),
                _ => Err(Error::Malformed),
            },
            Kind::Refusal => Err(refusal(answer)),
            Kind::Request | Kind::Subscribe | Kind::Change | Kind::Vault => {
                Err(Error::UnknownFrame { tag: kind.tag() })
            }
        }
    }

    /// Stop asking, and start being told.
    ///
    /// Consumes the client, because the connection stops being a conversation: a
    /// socket delivering changes is not also answering scripts, and a type that
    /// let a caller try would be promising a multiplexing this protocol does not
    /// do. A caller that wants both opens two connections.
    pub async fn follow(mut self, asked: &Follow) -> Result<Feed<S>> {
        frame::write(&mut self.stream, Kind::Subscribe, &asked.encode()).await?;
        Ok(Feed::new(self.stream))
    }
}

/// The node minor at which the vault frame exists (protocol §2.3).
const VAULT_MINOR: u8 = 2;

/// A refusal body is the node's own message, as the whole body.
///
/// Carried through verbatim: the node already writes messages that name the
/// place in the script, and rewording them here would make this crate a second
/// author for one error.
fn refusal(body: Vec<u8>) -> Error {
    match String::from_utf8(body) {
        Ok(message) => Error::Refused { message },
        // A refusal this client cannot read is still a refusal. Reporting it as
        // malformed would hide the one fact that is certain.
        Err(_) => Error::Refused {
            message: "the node refused, in bytes this client could not read".to_owned(),
        },
    }
}
