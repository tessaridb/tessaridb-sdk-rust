//! The vault frame (protocol §3.14) and the status it answers with.

use std::time::{Duration, SystemTime};

use crate::error::{Error, Result};
use crate::value::Value;
use crate::wire::frame::put_text;

/// One act on the vault, as the frame carries it.
///
/// Holds a passphrase, so it has no derived `Debug`: a type that prints what it
/// holds is how a passphrase reaches a log line written somewhere else, later.
pub(crate) enum Act<'a> {
    Status,
    Unseal(&'a str),
    Seal,
    Change { current: &'a str, new: &'a str },
}

impl std::fmt::Debug for Act<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Status => "Status",
            Self::Unseal(_) => "Unseal(..)",
            Self::Seal => "Seal",
            Self::Change { .. } => "Change(..)",
        })
    }
}

/// One vault carrying its own passphrase, as the frame names it — three
/// names already checked by the handle that holds them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Place<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) database: &'a str,
    pub(crate) vault: &'a str,
}

/// The body of a Vault frame. No credentials: a vault call runs as whoever the
/// connection's session already is, as every other call on a handle does. The
/// target is the store (`None`) or one vault.
pub(crate) fn body(act: &Act<'_>, place: Option<Place<'_>>) -> Vec<u8> {
    let mut body = vec![0];
    match place {
        None => body.push(0),
        Some(place) => {
            body.push(1);
            put_text(&mut body, place.namespace);
            put_text(&mut body, place.database);
            put_text(&mut body, place.vault);
        }
    }
    match act {
        Act::Status => body.push(1),
        Act::Unseal(passphrase) => {
            body.push(2);
            put_text(&mut body, passphrase);
        }
        Act::Seal => body.push(3),
        Act::Change { current, new } => {
            body.push(4);
            put_text(&mut body, current);
            put_text(&mut body, new);
        }
    }
    body
}

/// Whether the node can open secrets right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealState {
    /// The store has no passphrase yet; the first unseal sets it.
    Uninitialised,
    /// No key is held; nothing can be revealed.
    Sealed,
    /// A key is held until [`VaultStatus::seals_at`].
    Unsealed,
}

/// What opens a vault: its own passphrase, or the store's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Custody {
    /// The vault's own passphrase; the store's opens nothing in it.
    Own,
    /// The store's passphrase, as for every vault declared without one. The
    /// state reported beside it is the store's.
    Store,
}

/// The node's answer to every vault call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultStatus {
    /// Whether the node can open secrets.
    pub state: SealState,
    /// When an unsealed store seals itself; `None` unless unsealed.
    pub seals_at: Option<SystemTime>,
    /// How long an unseal lasts on this node.
    pub unseal_for: Duration,
    /// Whether this unseal set the store's first passphrase — the moment a
    /// mistyped passphrase became the passphrase.
    pub initialised: bool,
    /// For a call on one vault, what opens it; `None` for the store's own.
    pub custody: Option<Custody>,
}

impl VaultStatus {
    /// Read the status object the node answers with.
    pub(crate) fn from_value(value: Value) -> Result<Self> {
        let Value::Object(mut fields) = value else {
            return Err(Error::Malformed);
        };
        let state = match fields.remove("state") {
            Some(Value::String(state)) => match state.as_str() {
                "uninitialised" => SealState::Uninitialised,
                "sealed" => SealState::Sealed,
                "unsealed" => SealState::Unsealed,
                _ => return Err(Error::Malformed),
            },
            _ => return Err(Error::Malformed),
        };
        let seals_at = match fields.remove("seals_at") {
            Some(Value::Datetime { seconds, nanos }) => {
                let seconds = u64::try_from(seconds).map_err(|_| Error::Malformed)?;
                SystemTime::UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
            }
            Some(Value::None) | None => None,
            Some(_) => return Err(Error::Malformed),
        };
        let unseal_for = match fields.remove("unseal_for") {
            Some(Value::Duration { seconds, nanos }) => {
                Duration::new(u64::try_from(seconds).map_err(|_| Error::Malformed)?, nanos)
            }
            _ => return Err(Error::Malformed),
        };
        let initialised = matches!(fields.remove("initialised"), Some(Value::Bool(true)));
        let custody = match fields.remove("custody") {
            None => None,
            Some(Value::String(custody)) => match custody.as_str() {
                "own" => Some(Custody::Own),
                "store" => Some(Custody::Store),
                _ => return Err(Error::Malformed),
            },
            Some(_) => return Err(Error::Malformed),
        };
        Ok(Self {
            state,
            seals_at,
            unseal_for,
            initialised,
            custody,
        })
    }
}
