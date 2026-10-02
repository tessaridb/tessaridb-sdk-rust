//! Following a redirect (protocol §3.12) to the node that should answer.
//!
//! # What is checked, and why
//!
//! - **At most three hops.** A fourth redirect is a loop, or a cluster moving
//!   faster than a request can follow it; either way going on would not tell
//!   the two apart from progress.
//! - **Epochs never go backwards.** A redirect dated by an older leadership than
//!   one already followed was decided before it, and points at the past.
//! - **The node there is the node named.** An address alone cannot be checked;
//!   `session::context()` on arrival answers which node took the connection,
//!   and a different one is refused before the request is sent.
//! - **The tenancy goes with the request.** A connection is a session, and the
//!   new one has selected nothing: the namespace and database the session had
//!   here are selected there first. They are names, so each is checked against
//!   a plain-name pattern and never quoted into the script.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use super::{Client, Exchanged};
use crate::error::{Error, Result};
use crate::value::Value;
use crate::wire::message::{Answer, Request};
use crate::wire::redirect::{Redirect, Settlement};

/// The most redirects one request follows.
const MOST_HOPS: u8 = 3;

/// What a session reports about itself.
const CONTEXT: &str = "RETURN session::context();";

/// A connection being opened to the node a redirect named.
type Dialing<S> = Pin<Box<dyn Future<Output = Result<Client<S>>> + Send>>;

/// How this client opens a connection to another node. Shared, immutable.
pub(super) struct Dial<S>(Arc<dyn Fn(String) -> Dialing<S> + Send + Sync>);

impl Dial<TcpStream> {
    /// Over TCP, greeting as [`Client::connect`] does.
    pub(super) fn tcp() -> Self {
        Self(Arc::new(|endpoint| Box::pin(Client::connect(endpoint))))
    }
}

#[cfg(feature = "tls")]
impl Dial<crate::Secured> {
    /// Over TLS with the same trust, greeting as [`Client::connect_tls`] does.
    pub(super) fn tls(tls: crate::Tls) -> Self {
        Self(Arc::new(move |endpoint| {
            let tls = tls.clone();
            Box::pin(async move { Client::connect_tls(&endpoint, &tls).await })
        }))
    }
}

impl<S> Clone for Dial<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S> fmt::Debug for Dial<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Dial")
    }
}

/// Credentials this session presented, kept to present on a followed node.
#[derive(Clone)]
pub(super) struct Signed {
    name: String,
    password: String,
}

impl Signed {
    pub(super) fn new(name: &str, password: &str) -> Self {
        Self {
            name: name.to_owned(),
            password: password.to_owned(),
        }
    }
}

/// The name only: a credential answering `{:?}` with its secret is how a
/// password reaches a log line.
impl fmt::Debug for Signed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signed")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// What `session::context()` answered.
struct Context {
    node: Option<[u8; 16]>,
    namespace: Option<String>,
    database: Option<String>,
}

impl<S> Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Send `request` where `first` says, and on, until something answers.
    pub(super) async fn followed(
        &mut self,
        request: &Request,
        first: Redirect,
    ) -> Result<Vec<Answer>> {
        let Some(dial) = self.dial.clone() else {
            return Err(Error::Redirected(first));
        };
        let selecting = selection(&self.context().await?)?;
        let mut redirect = first;
        let mut floor = 0_u64;
        let mut hops = 0_u8;
        loop {
            if hops >= MOST_HOPS {
                return Err(Error::RedirectLoop { hops });
            }
            if redirect.epoch < floor {
                return Err(Error::StaleRedirect {
                    epoch: redirect.epoch,
                    floor,
                });
            }
            floor = redirect.epoch;
            let mut there = (dial.0)(redirect.endpoint.clone()).await?;
            there.signed.clone_from(&self.signed);
            if there.context().await?.node != Some(redirect.node) {
                return Err(Error::WrongNode {
                    expected: redirect.node,
                });
            }
            if let Some(script) = &selecting {
                let selected = Request {
                    credentials: there.credentials(),
                    ..Request::new(script)
                };
                if let Exchanged::Elsewhere(again) = there.exchange(&selected).await? {
                    return Err(Error::Redirected(again));
                }
            }
            hops = hops.saturating_add(1);
            match there.exchange(request).await? {
                Exchanged::Answers(answers) => {
                    if redirect.settlement == Settlement::Settled {
                        *self = there;
                    }
                    return Ok(answers);
                }
                Exchanged::Elsewhere(next) => redirect = next,
            }
        }
    }

    /// The credentials to present, as a request carries them.
    fn credentials(&self) -> Option<(String, String)> {
        self.signed
            .as_ref()
            .map(|signed| (signed.name.clone(), signed.password.clone()))
    }

    /// Ask this session which node it is on and what it has selected.
    async fn context(&mut self) -> Result<Context> {
        let asked = Request {
            credentials: self.credentials(),
            ..Request::new(CONTEXT)
        };
        let answers = match self.exchange(&asked).await? {
            Exchanged::Answers(answers) => answers,
            Exchanged::Elsewhere(redirect) => return Err(Error::Redirected(redirect)),
        };
        let Some(Answer::Value {
            value: Value::Object(fields),
            ..
        }) = answers.into_iter().last()
        else {
            return Err(Error::Malformed);
        };
        Ok(Context {
            node: match fields.get("node") {
                Some(Value::Uuid(node)) => Some(*node),
                _ => None,
            },
            namespace: text(&fields, "namespace"),
            database: text(&fields, "database"),
        })
    }
}

fn text(fields: &BTreeMap<String, Value>, key: &str) -> Option<String> {
    match fields.get(key) {
        Some(Value::String(name)) => Some(name.clone()),
        _ => None,
    }
}

/// The `USE` that selects `context`'s tenancy again, or `None` when it
/// selected nothing.
fn selection(context: &Context) -> Result<Option<String>> {
    let mut script = String::new();
    for (word, name) in [
        ("NAMESPACE", &context.namespace),
        ("DATABASE", &context.database),
    ] {
        let Some(name) = name else { continue };
        if !plain(name) {
            return Err(Error::NotFollowable { name: name.clone() });
        }
        script.push_str("USE ");
        script.push_str(word);
        script.push(' ');
        script.push_str(name);
        script.push_str("; ");
    }
    Ok((!script.is_empty()).then_some(script))
}

/// `^[A-Za-z_][A-Za-z0-9_]*$` — narrower than the node's lexer, so a name that
/// passes cannot be read as anything else.
fn plain(name: &str) -> bool {
    let mut characters = name.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|rest| rest.is_ascii_alphanumeric() || rest == '_')
}
