//! What a refusal says to do next (protocol §3.6, from minor 3).
//!
//! The message is prose for a person and changes between releases; the class is
//! what code branches on. The wire carries it as one byte before the message and
//! HTTP as the word in an error body's `code`.

/// The class of a refusal — what the caller should do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RefusalClass {
    /// Fix the request; repeating it unchanged cannot succeed.
    Invalid,
    /// Sign in, or sign in again.
    Unauthenticated,
    /// Stop: signing in again will not help.
    Forbidden,
    /// Wait, then repeat.
    Throttled,
    /// Send it to the node the message names.
    Elsewhere,
    /// Run the transaction again from its start.
    Retry,
    /// Re-read: the state the request assumed is not the state there is.
    Conflict,
    /// Try later or another node; the request itself was fine.
    Unavailable,
    /// A defect, damaged data, or a format the node cannot read — report it.
    Internal,
    /// The node could not class it, or named a class this client does not
    /// know. Treat it as not retriable.
    Unknown,
}

impl RefusalClass {
    /// The class a wire byte names: `0` and anything past the table are
    /// [`Self::Unknown`].
    #[must_use]
    pub const fn from_byte(byte: u8) -> Self {
        match byte {
            1 => Self::Invalid,
            2 => Self::Unauthenticated,
            3 => Self::Forbidden,
            4 => Self::Throttled,
            5 => Self::Elsewhere,
            6 => Self::Retry,
            7 => Self::Conflict,
            8 => Self::Unavailable,
            9 => Self::Internal,
            _ => Self::Unknown,
        }
    }

    /// The class an HTTP error body's `code` names.
    #[must_use]
    pub fn from_word(word: &str) -> Self {
        match word {
            "invalid" => Self::Invalid,
            "unauthenticated" => Self::Unauthenticated,
            "forbidden" => Self::Forbidden,
            "throttled" => Self::Throttled,
            "elsewhere" => Self::Elsewhere,
            "retry" => Self::Retry,
            "conflict" => Self::Conflict,
            "unavailable" => Self::Unavailable,
            "internal" => Self::Internal,
            _ => Self::Unknown,
        }
    }

    /// The word the protocol uses for this class; `"unknown"` for
    /// [`Self::Unknown`].
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Unauthenticated => "unauthenticated",
            Self::Forbidden => "forbidden",
            Self::Throttled => "throttled",
            Self::Elsewhere => "elsewhere",
            Self::Retry => "retry",
            Self::Conflict => "conflict",
            Self::Unavailable => "unavailable",
            Self::Internal => "internal",
            Self::Unknown => "unknown",
        }
    }
}

/// A refusal body read: its class when it carried one, and its message.
///
/// A first byte of `0`–`9` is a class; anything else is the first byte of a
/// message from a node before protocol 1.3, which carries no class at all.
pub(crate) fn read(body: &[u8]) -> (Option<RefusalClass>, String) {
    let (class, words) = match body.split_first() {
        Some((&first, rest)) if first <= 9 => (Some(RefusalClass::from_byte(first)), rest),
        _ => (None, body),
    };
    let message = String::from_utf8(words.to_vec()).unwrap_or_else(|_| {
        // A refusal this client cannot read is still a refusal; reporting it as
        // malformed would hide the one fact that is certain.
        "the node refused, in bytes this client could not read".to_owned()
    });
    (class, message)
}
