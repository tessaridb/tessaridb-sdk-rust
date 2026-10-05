//! The connection, and what you can ask it.

mod follow;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::error::{Error, Result};
use crate::feed::Feed;
use crate::value::Value;
use crate::wire::frame::{self, Kind};
use crate::wire::message::{Answer, Request, decode_answers};
use crate::wire::push::Follow;
use crate::wire::redirect::Redirect;
use follow::{Dial, Signed};

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
    /// How to reach the node a redirect names; `None` for a stream handed in,
    /// which this client cannot dial again, so a redirect is returned instead.
    dial: Option<Dial<S>>,
    /// The credentials this session last presented, presented again on the
    /// node a redirect sends the request to.
    signed: Option<Signed>,
}

/// What one request came back as.
enum Exchanged {
    Answers(Vec<Answer>),
    Elsewhere(Redirect),
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
        Ok(Self {
            stream,
            peer_minor,
            dial: Some(Dial::tcp()),
            signed: None,
        })
    }
}

#[cfg(feature = "tls")]
impl Client<crate::Secured> {
    /// Connect to `address` over TLS, trusting `tls`, and exchange greetings.
    ///
    /// The node's certificate must chain to what `tls` trusts and carry the
    /// host part of `address`. A redirect is followed with the same trust, so a
    /// node it names is checked exactly as this one was.
    ///
    /// # Errors
    ///
    /// As [`Client::connect`], and [`Error::Tls`] when the handshake fails.
    pub async fn connect_tls(address: &str, tls: &crate::Tls) -> Result<Self> {
        let socket = TcpStream::connect(address).await?;
        socket.set_nodelay(true)?;
        let mut stream = tls.wire(address, socket).await?;
        let peer_minor = Some(frame::greet(&mut stream).await?);
        Ok(Self {
            stream,
            peer_minor,
            dial: Some(Dial::tls(tls.clone())),
            signed: None,
        })
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
            dial: None,
            signed: None,
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
    ///
    /// A redirect (protocol §3.12) is followed: the request is sent to the node
    /// it names, at most three hops, after checking that the node answering
    /// there is the one named and selecting the session's namespace and
    /// database again. A `settled` redirect moves this connection to that node;
    /// a `transient` one answers this request and leaves it where it was.
    pub async fn send(&mut self, request: &Request) -> Result<Vec<Answer>> {
        if let Some((name, password)) = &request.credentials {
            self.signed = Some(Signed::new(name, password));
        }
        match self.exchange(request).await? {
            Exchanged::Answers(answers) => Ok(answers),
            Exchanged::Elsewhere(redirect) => self.followed(request, redirect).await,
        }
    }

    /// Send a request and read one reply, a redirect included.
    async fn exchange(&mut self, request: &Request) -> Result<Exchanged> {
        frame::write(&mut self.stream, Kind::Request, &request.encode()).await?;
        let Some((kind, body)) = frame::read(&mut self.stream).await? else {
            return Err(Error::Truncated);
        };
        match kind {
            Kind::Answer => decode_answers(&body).map(Exchanged::Answers),
            Kind::Refusal => Err(refusal(&body)),
            Kind::Elsewhere => Redirect::decode(&body).map(Exchanged::Elsewhere),
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
            Kind::Refusal => Err(refusal(&answer)),
            Kind::Request | Kind::Subscribe | Kind::Change | Kind::Vault | Kind::Elsewhere => {
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

/// A refusal body: the class byte when the node sent one, then its own words.
///
/// The words are carried through verbatim: the node already writes messages
/// that name the place in the script, and rewording them here would make this
/// crate a second author for one error.
fn refusal(body: &[u8]) -> Error {
    let (class, message) = crate::refusal::read(body);
    Error::Refused { message, class }
}
