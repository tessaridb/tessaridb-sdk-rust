//! The wire protocol: framing, requests, answers, and the change stream.
//!
//! Statements and subscriptions travel here rather than over HTTP, because this
//! is the surface that carries the store's full value model. The HTTP routes
//! answer JSON, which cannot tell a decimal from a string that spells one.

pub mod frame;
pub mod message;
pub mod push;
pub mod redirect;
