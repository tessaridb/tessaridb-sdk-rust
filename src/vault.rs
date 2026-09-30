//! A vault — vault contract 1.0 (`spec/vault-v1.md` in the protocol repository).
//!
//! Two halves. The store-wide acts — [`Client::vault_status`],
//! [`Client::unseal`], [`Client::seal`], [`Client::change_passphrase`] — go in a
//! frame of their own (protocol §3.14), so a passphrase is a field and never a
//! statement: statement text is what a console keeps and a client logs on
//! failure. A [`Vault`] handle then lists, reveals, writes and shares the records
//! of one vault with statements whose every id and value is bound.
//!
//! What a vault promises, said as narrowly as it is true: the stored bytes,
//! backups and replicas are ciphertext; a running node that is unsealed can
//! decrypt, because it must to answer a reveal. An unseal lasts the node's
//! period (ten minutes unless it was started otherwise) and then the node seals
//! itself.
//!
//! A passphrase given to these functions is sent and dropped. It is in no error
//! this crate raises and no `Debug` of any type here; the node's own refusals
//! never quote it either.

use std::collections::BTreeMap;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::client::Client;
use crate::error::{Error, Result};
use crate::query::BuildError;
use crate::value::Value;
use crate::wire::message::Answer;

mod call;
mod statements;
#[cfg(test)]
mod tests;

use call::{Act, Place, body};
pub use call::{Custody, SealState, VaultStatus};
use statements::{Rendered, Statements, audit, limit_refused};

impl<S> Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Whether the node can open secrets, and until when.
    ///
    /// # Errors
    ///
    /// [`Error::NodeTooOld`] before sending to a node without the vault frame;
    /// whatever the node refused; the transport failure.
    pub async fn vault_status(&mut self) -> Result<VaultStatus> {
        self.vault_act(&Act::Status, None).await
    }

    /// Present the passphrase. The first one ever presented to a store becomes
    /// its passphrase, and the answer says so with `initialised`.
    ///
    /// # Errors
    ///
    /// As [`Client::vault_status`]. A wrong passphrase is the node's refusal; a
    /// run of them makes the node refuse even a right one for a while, which
    /// means **wait** and is not retried here.
    pub async fn unseal(&mut self, passphrase: &str) -> Result<VaultStatus> {
        self.vault_act(&Act::Unseal(passphrase), None).await
    }

    /// Drop the key: nothing can be revealed until the next unseal.
    ///
    /// # Errors
    ///
    /// As [`Client::vault_status`].
    pub async fn seal(&mut self) -> Result<VaultStatus> {
        self.vault_act(&Act::Seal, None).await
    }

    /// Wrap the store's key under a new passphrase. No secret is re-encrypted,
    /// the old passphrase stops unsealing, and a backup taken before the change
    /// still opens with the old one.
    ///
    /// # Errors
    ///
    /// As [`Client::unseal`].
    pub async fn change_passphrase(&mut self, current: &str, new: &str) -> Result<VaultStatus> {
        self.vault_act(&Act::Change { current, new }, None).await
    }

    /// The store's trail of vault reads, optionally one user's (§3.5). Answered
    /// only to a caller who administers the whole store.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] (as [`Error::Build`]) for a name that is not one,
    /// before sending; whatever the node refused.
    pub async fn vault_audit(
        &mut self,
        within: (&str, &str),
        by: Option<&str>,
    ) -> Result<Vec<Value>> {
        let script = audit(within, by)?;
        match self.run(&script, None).await?.pop() {
            Some(Answer::Value {
                value: Value::Object(mut report),
                ..
            }) => match report.remove("audit") {
                Some(Value::Array(entries)) => Ok(entries),
                _ => Err(Error::Malformed),
            },
            _ => Err(Error::Malformed),
        }
    }

    async fn vault_act(&mut self, act: &Act<'_>, place: Option<Place<'_>>) -> Result<VaultStatus> {
        VaultStatus::from_value(self.vault_frame(&body(act, place)).await?)
    }
}

/// One page of a vault's record ids.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// The ids, in key order.
    pub ids: Vec<Value>,
    /// The id to ask for the next page after; `None` on the last page.
    pub next: Option<Value>,
}

/// A vault in one namespace and database, over a connection this borrows.
///
/// Every call sends its own `USE` with its statement, so a connection that
/// reconnected underneath cannot read another database.
#[derive(Debug)]
pub struct Vault<'c, S> {
    client: &'c mut Client<S>,
    statements: Statements,
    /// The three checked names, for the acts that go in the vault frame.
    namespace: String,
    database: String,
    name: String,
}

impl<'c, S> Vault<'c, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// The vault `vault` in `namespace`/`database`, over `client`.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] when any of the three is not a name — refused
    /// before anything is sent.
    pub fn new(
        client: &'c mut Client<S>,
        within: (&str, &str),
        vault: &str,
    ) -> std::result::Result<Self, BuildError> {
        Ok(Self {
            client,
            statements: Statements::new(within, vault)?,
            namespace: within.0.to_owned(),
            database: within.1.to_owned(),
            name: vault.to_owned(),
        })
    }

    /// This vault's seal status, with [`VaultStatus::custody`] saying what opens
    /// it. For a vault that opens with the store's passphrase the state is the
    /// store's.
    ///
    /// # Errors
    ///
    /// As [`Client::vault_status`].
    pub async fn status(&mut self) -> Result<VaultStatus> {
        self.act(&Act::Status).await
    }

    /// Unseal this vault with its own passphrase, for the node's period.
    ///
    /// A vault that opens with the store's passphrase is refused by the node
    /// rather than unsealed through the store, because that would open every
    /// other vault in the store's custody; use [`Client::unseal`] for those.
    ///
    /// # Errors
    ///
    /// As [`Client::unseal`].
    pub async fn unseal(&mut self, passphrase: &str) -> Result<VaultStatus> {
        self.act(&Act::Unseal(passphrase)).await
    }

    /// Seal this vault; the store and every other vault stay as they were.
    ///
    /// # Errors
    ///
    /// As [`Client::vault_status`].
    pub async fn seal(&mut self) -> Result<VaultStatus> {
        self.act(&Act::Seal).await
    }

    /// Wrap this vault's key under a new passphrase. No secret is re-encrypted,
    /// and a backup taken before the change still opens with the old one.
    ///
    /// # Errors
    ///
    /// As [`Client::unseal`].
    pub async fn change_passphrase(&mut self, current: &str, new: &str) -> Result<VaultStatus> {
        self.act(&Act::Change { current, new }).await
    }

    async fn act(&mut self, act: &Act<'_>) -> Result<VaultStatus> {
        let place = Place {
            namespace: &self.namespace,
            database: &self.database,
            vault: &self.name,
        };
        self.client.vault_act(act, Some(place)).await
    }

    /// One page of ids, after `after` (the previous page's `next`), at most
    /// `limit` of them (1 to 10 000; the node's own 1 000 when `None`).
    ///
    /// # Errors
    ///
    /// [`Error::NotAVaultArgument`] for a limit out of range; whatever the node
    /// refused.
    pub async fn list(&mut self, after: Option<Value>, limit: Option<u32>) -> Result<Page> {
        if limit_refused(limit) {
            return Err(Error::NotAVaultArgument {
                reason: "a listing asks for 1 to 10000 ids",
            });
        }
        let Value::Object(mut page) = self.value(self.statements.list(after, limit)).await? else {
            return Err(Error::Malformed);
        };
        let Some(Value::Array(ids)) = page.remove("records") else {
            return Err(Error::Malformed);
        };
        let next = page.remove("next").filter(|next| *next != Value::None);
        Ok(Page { ids, next })
    }

    /// The named secret fields of one record, or every secret field when
    /// `fields` is empty. Each call is recorded by the node before it answers.
    /// The answer is the caller's; this crate keeps no copy.
    ///
    /// # Errors
    ///
    /// [`BuildError::NotAName`] for a field that is not a name; whatever the
    /// node refused — a field that is not `SECRET`, a sealed store.
    pub async fn reveal(
        &mut self,
        id: impl Into<Value>,
        fields: &[&str],
    ) -> Result<BTreeMap<String, Value>> {
        let rendered = self.statements.reveal(id.into(), fields)?;
        match self.value(rendered).await? {
            Value::Object(revealed) => Ok(revealed),
            _ => Err(Error::Malformed),
        }
    }

    /// Set these fields on the record, creating it when absent and keeping every
    /// other field and every recipient.
    ///
    /// # Errors
    ///
    /// [`Error::NotAVaultArgument`] for no fields; [`BuildError::NotAName`] for a
    /// field that is not a name; whatever the node refused.
    pub async fn write(
        &mut self,
        id: impl Into<Value>,
        fields: &BTreeMap<String, Value>,
    ) -> Result<()> {
        if fields.is_empty() {
            return Err(Error::NotAVaultArgument {
                reason: "a write sets at least one field",
            });
        }
        let rendered = self.statements.write(id.into(), fields)?;
        self.send(rendered).await.map(drop)
    }

    /// Who may one day open this record: name → the key material they hold.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn recipients(&mut self, id: impl Into<Value>) -> Result<BTreeMap<String, Vec<u8>>> {
        let Value::Object(mut report) = self.value(self.statements.recipients(id.into())).await?
        else {
            return Err(Error::Malformed);
        };
        let Some(Value::Object(held)) = report.remove("recipients") else {
            return Err(Error::Malformed);
        };
        held.into_iter()
            .map(|(name, key)| match key {
                Value::Bytes(key) => Ok((name, key)),
                _ => Err(Error::Malformed),
            })
            .collect()
    }

    /// Add a recipient. The store keeps the name and the key and interprets
    /// neither; a name already present is refused, never replaced.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn add_recipient(
        &mut self,
        id: impl Into<Value>,
        name: &str,
        key: &[u8],
    ) -> Result<()> {
        let rendered = self.statements.add_recipient(id.into(), name, key);
        self.send(rendered).await.map(drop)
    }

    /// Remove a recipient; one that is not there is refused, never answered `ok`.
    ///
    /// # Errors
    ///
    /// Whatever the node refused.
    pub async fn remove_recipient(&mut self, id: impl Into<Value>, name: &str) -> Result<()> {
        let rendered = self.statements.remove_recipient(id.into(), name);
        self.send(rendered).await.map(drop)
    }

    async fn send(&mut self, (script, given): Rendered) -> Result<Vec<Answer>> {
        self.client.run_with(&script, None, given).await
    }

    async fn value(&mut self, rendered: Rendered) -> Result<Value> {
        match self.send(rendered).await?.pop() {
            Some(Answer::Value { value, .. }) => Ok(value),
            _ => Err(Error::Malformed),
        }
    }
}
