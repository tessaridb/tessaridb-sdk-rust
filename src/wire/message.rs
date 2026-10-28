//! What a client asks, and what it gets back.

use std::collections::BTreeMap;

use crate::codec::{decode, encode};
use crate::error::{Error, Result};
use crate::value::Value;
use crate::wire::frame::{Body, put_bytes, put_text, put_u32};

/// The values a script's parameters are bound to.
pub type Parameters = BTreeMap<String, Value>;

/// What the tables an answer references are called.
///
/// A reference holds a table id, and an id is meaningless outside the node that
/// minted it — the catalog lives on the node. Without these names a client can
/// only render an opaque reference, and the point of this protocol is that a
/// client decides nothing.
pub type Names = BTreeMap<u32, String>;

/// A script to run, and who is running it.
#[derive(Clone, PartialEq)]
pub struct Request {
    /// The script.
    pub script: String,
    /// The credentials, when the caller has any.
    ///
    /// Optional because a store with no users declared is open and runs
    /// anything, which is what keeps an empty one usable. A closed store's
    /// refusal comes from the node, not from a second rule here.
    pub credentials: Option<(String, String)>,
    /// The values the script's parameters bind to.
    ///
    /// Carried in the store's own codec rather than as text the node parses. A
    /// value the node has to *read* is a value that can be read as something
    /// else, and binding after parsing exists to make that impossible — undoing
    /// it at the wire would be a strange place to give it back.
    pub parameters: Parameters,
    /// Keys that open vaults in key custody for this request alone (protocol
    /// §3.4, minor 7). Never part of the script and never a parameter.
    pub keys: Vec<PresentedKey>,
}

/// A 256-bit key that opens one vault in key custody (protocol §3.14).
///
/// The bytes are reachable only through [`VaultKey::expose`], are never
/// printed, and are wiped when the value is dropped.
#[derive(Clone, PartialEq, Eq)]
pub struct VaultKey([u8; 32]);

impl VaultKey {
    /// A key from its 32 bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The key's bytes, for the one place that has to send them.
    #[must_use]
    pub const fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Drop for VaultKey {
    fn drop(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.0);
    }
}

impl std::fmt::Debug for VaultKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("VaultKey(..)")
    }
}

/// One key a request presents, and the vault it opens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedKey {
    /// The vault's namespace.
    pub namespace: String,
    /// The vault's database.
    pub database: String,
    /// The vault.
    pub vault: String,
    /// The key that opens it.
    pub key: VaultKey,
}

/// Written by hand rather than derived, because the derived one prints the
/// password.
///
/// Nothing in this crate prints a request. But a struct holding a credential and
/// answering `{:?}` with it is how a password reaches a log line, and the line
/// that does it is always somewhere else and written later. The name is shown,
/// because knowing *who* a refused request claimed to be is the whole value of
/// printing one; the secret is not.
impl std::fmt::Debug for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Request")
            .field("script", &self.script)
            .field(
                "as",
                &self
                    .credentials
                    .as_ref()
                    .map_or("nobody", |(name, _)| name.as_str()),
            )
            .field("parameters", &self.parameters.keys())
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}

impl Request {
    /// A request to run `script` as nobody in particular.
    #[must_use]
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
            credentials: None,
            parameters: Parameters::new(),
            keys: Vec::new(),
        }
    }

    /// The same request, signed in as `name`.
    #[must_use]
    pub fn as_user(mut self, name: impl Into<String>, password: impl Into<String>) -> Self {
        self.credentials = Some((name.into(), password.into()));
        self
    }

    /// The same request with `name` bound to `value`.
    ///
    /// The value travels in the codec. It never becomes part of the script text,
    /// so a value cannot turn into syntax however it is spelled.
    #[must_use]
    pub fn bind(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    /// The same request presenting `key` for one vault (protocol §3.4, minor 7).
    ///
    /// The client refuses to send it to a node below minor 7, which would read
    /// past the keys and run the script without them.
    #[must_use]
    pub fn presenting(mut self, place: (&str, &str, &str), key: VaultKey) -> Self {
        self.keys.push(PresentedKey {
            namespace: place.0.to_owned(),
            database: place.1.to_owned(),
            vault: place.2.to_owned(),
            key,
        });
        self
    }

    /// The body of a request frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        put_text(&mut body, &self.script);
        match &self.credentials {
            Some((name, password)) => {
                body.push(1);
                put_text(&mut body, name);
                put_text(&mut body, password);
            }
            None => body.push(0),
        }
        put_u32(
            &mut body,
            u32::try_from(self.parameters.len()).unwrap_or(u32::MAX),
        );
        for (name, value) in &self.parameters {
            put_text(&mut body, name);
            put_bytes(&mut body, &encode(value));
        }
        // From minor 7 the keys follow the stream byte, which is written as 0
        // because this client never asks for a streamed snapshot.
        if !self.keys.is_empty() {
            body.push(0);
            put_u32(
                &mut body,
                u32::try_from(self.keys.len()).unwrap_or(u32::MAX),
            );
            for presented in &self.keys {
                put_text(&mut body, &presented.namespace);
                put_text(&mut body, &presented.database);
                put_text(&mut body, &presented.vault);
                body.extend_from_slice(presented.key.expose());
            }
        }
        body
    }
}

/// The tag that opens an outcome.
mod tag {
    pub(super) const DONE: u8 = 0;
    pub(super) const RECORDS: u8 = 1;
    pub(super) const VALUE: u8 = 2;
    pub(super) const KEYS: u8 = 3;
    pub(super) const REMOVED: u8 = 4;
}

/// What a node said about whether its answer is exact.
///
/// Two states, because the third — a node that never sent the field — lives one
/// level up as the `Option` wrapping this. Keeping them apart is the whole reason
/// this is not a `bool`: a caller holding `Option<bool>` writes `unwrap_or(true)`
/// sooner or later, and the value invented there is a promise nobody sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exact {
    /// The node called the answer provably the records the question names.
    Yes,
    /// It did not, and said why.
    No {
        /// The reason, in the node's own words.
        ///
        /// Carried rather than derived from the access path on this side: a
        /// client that phrased it itself would be describing a read it did not
        /// perform, and would keep describing it that way after the node's own
        /// wording changed.
        reason: String,
    },
}

/// One word the query asked for, and the word the collection holds instead.
///
/// `typed` is the term **after the field's analyzer ran** — lowercased, folded
/// and stemmed — rather than the substring the reader wrote. A caller that
/// highlights the correction inside the original query string matches on that
/// basis or not at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Correction {
    /// The term as the query asked for it, analyzed.
    pub typed: String,
    /// The nearest term the collection's dictionary holds.
    pub instead: String,
}

/// What the node thinks the query might have meant.
///
/// Advice about a **different** question. The records beside this are the
/// records the query as typed returns, and a caller that re-runs the read with a
/// correction substituted in has asked something else: the substituted read
/// answers with different records at an entirely plausible score, and nothing in
/// the answer says the question changed.
///
/// [`Self::NotSought`] and [`Self::NothingNearer`] are the pair that must not
/// collapse. `NothingNearer` is a claim about the collection — a dictionary was
/// asked and holds every term the query named. `NotSought` is the absence of
/// one, and it is what nearly every read carries, because only a search index
/// has a dictionary at all. A caller that renders both as *no suggestions*
/// reports a negative the node never checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Suggested {
    /// No term dictionary was consulted for this read.
    NotSought,
    /// One was, and it holds every term the query named.
    NothingNearer,
    /// One was, and here is what it holds instead.
    DidYouMean(Vec<Correction>),
}

/// Something a read volunteered about how it answered.
///
/// A **kind and a message**, not a structure. A client's two uses are to group
/// by the first and show the second, and a typed note would put the node's whole
/// vocabulary into this build for no gain.
///
/// The kinds a node sends today are `fell-back`, `approximate`,
/// `compared-across-kinds`, `cursor-walked` and `subquery-ceiling`. That list is
/// **not** closed and is not matched on here: an unfamiliar kind is a newer node
/// saying something this build has no name for, which is not an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Note {
    /// Which kind of thing the read is saying.
    pub kind: String,
    /// What it says, in words meant for a person.
    pub message: String,
}

/// What a client got back for one statement.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Answer {
    /// The statement did its work.
    Done,
    /// Records, each with its identity as written and its value.
    ///
    /// Marked `#[non_exhaustive]` as well as the enum. The enum's attribute
    /// covers a new *variant*; this one covers a new *field*, and this variant
    /// keeps gaining them — the notes, the `ONLY` flag, the exactness and the
    /// suggestion below — because the frame grows by appending. Without it every
    /// such addition is a breaking change for anyone matching the fields out.
    #[non_exhaustive]
    Records {
        /// What was found — identity as the node spells it, and the value.
        ///
        /// The identity is spelled exactly as [`Answer::Keys`] describes: the
        /// id half alone, in the language's own form, with the quoting that
        /// separates a text id from an integer one. A record **reference**
        /// stored inside the value is a different thing and keeps its
        /// qualified `table:id` form.
        records: Vec<(String, Value)>,
        /// How the node found them: `record`, `index`, `ordered`, or `scan`.
        ///
        /// Reported, not requested. An unrecognised path reads as `scan`, which
        /// is the honest name for a path this build does not know: it is the one
        /// that promises nothing.
        path: String,
        /// What the tables these records reference are called.
        names: Names,
        /// What the read volunteered about how it answered.
        ///
        /// Empty when the node had nothing to say, and also when the node
        /// predates notes entirely — the two are the same claim, so they are not
        /// distinguished.
        notes: Vec<Note>,
        /// Whether the statement wrote `ONLY`.
        ///
        /// An assertion by the statement's author that at most one record
        /// answers. A caller that offers the clause should render such an answer
        /// as the record itself rather than as a list holding it; ignoring the
        /// flag is conforming, and produces a one-element array with nothing to
        /// say it was asked for differently.
        only: bool,
        /// Whether the node called this answer provably the records the
        /// question names — and `None` when it did not say.
        ///
        /// The one field of this variant where absence is **not** the default.
        /// `notes` above states the opposite rule explicitly: an empty list and
        /// a node that predates notes are the same claim, so they are not
        /// distinguished. Here they are different claims. A node older than this
        /// field made no statement about exactness, and `None` is that — not
        /// `Some(Exact::Yes)`.
        ///
        /// A caller that cannot represent the third state should surface such an
        /// answer as unverified rather than as exact. An approximate answer and
        /// an exact one are otherwise the same shape, the same length, and
        /// frequently the same records.
        exact: Option<Exact>,
        /// What the node thinks the query might have meant — and `None` when it
        /// did not say.
        ///
        /// The `Option` is a **fourth** state beside [`Suggested`]'s three, and
        /// unlike `exact` above it is not a trap: a node built before this field
        /// consulted no dictionary either, so `None` and
        /// `Some(Suggested::NotSought)` tell a caller the same thing and differ
        /// only in what they say about the node. They are kept apart because
        /// that difference is the one a caller diagnosing an absent suggestion
        /// needs — *this read asked nothing* against *this node asks nothing* —
        /// and neither may be read as `NothingNearer`.
        suggestion: Option<Suggested>,
    },
    /// One value, and the names of the tables it references.
    Value {
        /// What was answered.
        value: Value,
        /// What the tables it references are called.
        names: Names,
    },
    /// Keys, as the node spells them.
    ///
    /// The **id half alone** — there is no `table:` prefix and no colon,
    /// because the statement that asked already named the table. `KEYS FROM
    /// notes` over two records answers `["42", "'42'"]`, not `["notes:42", …]`.
    ///
    /// Each string is the identity in the **language's own form**, so it can be
    /// written straight back into a script: `1`, `'ada'`, `uuid '…'`, `0x…`.
    /// The quoting is load-bearing and is what separates the integer id `42`
    /// from the text id `'42'`, which are two different records. Do not strip
    /// it, and do not compare a key against an unquoted string.
    ///
    /// This differs from the same outcome over HTTP, where §5.7.1 specifies a
    /// plain form that collapses those two ids into `"42"` and forbids parsing
    /// it back. The wire carries the type; the JSON surface does not.
    Keys(Vec<String>),
    /// How many records a conditional delete removed.
    Removed(u64),
    /// An outcome this build has no type for.
    ///
    /// Not an error. A newer node may answer with something this client has
    /// never seen, and saying so is honest where guessing at its content is not.
    /// A caller sees it rather than losing the outcome silently.
    Unknown,
}

/// Read every outcome out of an answer frame's body.
///
/// # Each outcome carries its own length, and that is what makes this safe
///
/// An outcome is a `u32` length and then its tagged body. A tag this build does
/// not know is therefore *skippable*: the reader yields [`Answer::Unknown`],
/// steps to the end of that outcome, and carries on with the next one. So a
/// newer node may introduce an outcome kind **anywhere** in an answer without
/// breaking this client, which is the whole content of the promise that a minor
/// version bump is compatible.
///
/// Each outcome is decoded inside a cursor bounded by its own length, so a
/// recognised outcome cannot read into the next one either — a body that claims
/// more than its length allows is malformed rather than a mis-parse of
/// everything after it.
///
/// Bytes left over **inside** one outcome are skipped rather than refused. That
/// is deliberate: it lets a later minor add a trailing field to an outcome kind
/// this build already knows, and an older client keeps reading the part it
/// understands.
pub fn decode_answers(body: &[u8]) -> Result<Vec<Answer>> {
    let mut reader = Body::new(body);
    let count = reader.take_u32()?;
    let mut answers = Vec::new();
    // The count is not used to pre-allocate: it is a stranger's number, and the
    // frame ceiling bounds the bytes rather than the claim.
    for _ in 0..count {
        let length = reader.take_u32()?;
        let length = usize::try_from(length).map_err(|_| Error::Malformed)?;
        let mut outcome = Body::new(reader.take(length)?);
        answers.push(decode_one(&mut outcome)?);
    }
    Ok(answers)
}

fn decode_one(reader: &mut Body<'_>) -> Result<Answer> {
    let found = reader.take_u8()?;
    match found {
        tag::DONE => Ok(Answer::Done),
        tag::RECORDS => {
            let path = path_name(reader.take_u8()?).to_owned();
            let names = take_names(reader)?;
            let count = reader.take_u32()?;
            let mut records = Vec::new();
            for _ in 0..count {
                let id = reader.take_text()?;
                let bytes = reader.take_bytes()?;
                records.push((id, decode(&bytes).map_err(Error::from)?));
            }
            // Everything past the records is a later addition, and the frame
            // grows by appending — so each field is read only if it is there,
            // and absent means the node had nothing to add rather than that the
            // body was truncated. The outcome's own length is what makes that
            // distinction safe to draw.
            let notes = if reader.remaining() > 0 {
                let count = reader.take_u32()?;
                let mut notes = Vec::new();
                for _ in 0..count {
                    let kind = reader.take_text()?;
                    let message = reader.take_text()?;
                    notes.push(Note { kind, message });
                }
                notes
            } else {
                Vec::new()
            };
            let only = reader.remaining() > 0 && reader.take_u8()? != 0;
            // And here the rule above stops applying, which is why this field is
            // an `Option` where `only` is a `bool`. For every other appended
            // field, absent means the node had nothing to add — a node that
            // never heard of `ONLY` served reads that were not `ONLY` reads, so
            // `false` is the truth about them. Absent exactness is not like
            // that: such a node did not serve exact answers and forget to say
            // so, it made no claim at all, and reading the silence as `Yes`
            // would put a promise in its mouth. §3.5 of the specification
            // requires the three states to stay apart.
            let exact = if reader.remaining() > 0 {
                let approximate = reader.take_u8()? != 0;
                let reason = reader.take_text()?;
                Some(if approximate {
                    Exact::No { reason }
                } else {
                    Exact::Yes
                })
            } else {
                None
            };
            // One state byte, and the same reason as exactness for reading its
            // absence as silence rather than as a value: a node that predates
            // the field never walked a dictionary, so it made no claim about one.
            // What differs is the consequence — here silence and state `0` say
            // the same thing to a caller, so nothing is put in an older node's
            // mouth by reading them alike. They stay apart because they differ
            // about the NODE, and `1` is the state neither may collapse into.
            let suggestion = if reader.remaining() > 0 {
                match reader.take_u8()? {
                    0 => Some(Suggested::NotSought),
                    1 => Some(Suggested::NothingNearer),
                    2 => {
                        let count = reader.take_u32()?;
                        let mut corrections = Vec::new();
                        for _ in 0..count {
                            let typed = reader.take_text()?;
                            let instead = reader.take_text()?;
                            corrections.push(Correction { typed, instead });
                        }
                        Some(Suggested::DidYouMean(corrections))
                    }
                    // A newer node saying something in a vocabulary this build
                    // lacks, which reads as silence rather than as a malformed
                    // answer. The outcome's own length carries the caller past
                    // whatever followed the byte.
                    _ => None,
                }
            } else {
                None
            };
            Ok(Answer::Records {
                records,
                path,
                names,
                notes,
                only,
                exact,
                suggestion,
            })
        }
        tag::VALUE => {
            let names = take_names(reader)?;
            let bytes = reader.take_bytes()?;
            Ok(Answer::Value {
                value: decode(&bytes).map_err(Error::from)?,
                names,
            })
        }
        tag::KEYS => {
            let count = reader.take_u32()?;
            let mut keys = Vec::new();
            for _ in 0..count {
                keys.push(reader.take_text()?);
            }
            Ok(Answer::Keys(keys))
        }
        tag::REMOVED => Ok(Answer::Removed(reader.take_u64()?)),
        // Consumes nothing further, and does not need to: the caller reads this
        // outcome inside a cursor bounded by its own length and steps to the end
        // of it either way.
        _ => Ok(Answer::Unknown),
    }
}

fn take_names(reader: &mut Body<'_>) -> Result<Names> {
    let count = reader.take_u32()?;
    let mut names = Names::new();
    for _ in 0..count {
        let table = reader.take_u32()?;
        let name = reader.take_text()?;
        names.insert(table, name);
    }
    Ok(names)
}

/// The access path a byte names.
const fn path_name(tag: u8) -> &'static str {
    match tag {
        0 => "record",
        1 => "index",
        3 => "ordered",
        _ => "scan",
    }
}
