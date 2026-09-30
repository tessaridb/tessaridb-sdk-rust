//! A vault against a running node (vault contract §7): status, unseal, write,
//! list across two pages, reveal, recipients, audit, a passphrase change, seal,
//! and an unseal with the new passphrase; then a vault with its own passphrase —
//! its status, seal, a refused unseal with the store's passphrase, an unseal with
//! its own, and a change — with every error this client raises on the way
//! scanned for the passphrase.

use std::collections::BTreeMap;

use tessaridb_client::{Client, Custody, Error, SealState, Value, Vault};

use super::Node;

const PASSPHRASE: &str = "an operator passphrase 4b71";
const NEXT: &str = "the next passphrase 9c02";
const PLANTED: &str = "correct-horse-battery-staple-9f2b";

fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(name, value)| ((*name).to_owned(), Value::String((*value).to_owned())))
        .collect()
}

/// Every rendering of an error a caller might log.
fn shown(error: &Error) -> String {
    format!("{error} {error:?}")
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn the_whole_vault_contract_runs_against_a_node() {
    let node = Node::start().await;
    let mut client = node.client().await;

    assert_eq!(
        client.vault_status().await.unwrap().state,
        SealState::Uninitialised
    );
    let first = client.unseal(PASSPHRASE).await.unwrap();
    assert!(
        first.initialised,
        "the first unseal did not say it set the passphrase"
    );
    assert_eq!(first.state, SealState::Unsealed);
    assert!(
        first.seals_at.is_some(),
        "an unsealed store did not say when it seals"
    );
    assert_eq!(first.unseal_for, std::time::Duration::from_secs(600));

    client
        .run(
            "DEFINE NAMESPACE app; USE NAMESPACE app; DEFINE DATABASE main; USE DATABASE main; \
             DEFINE VAULT team; DEFINE FIELD 'password' ON team TYPE string SECRET; \
             DEFINE FIELD login ON team TYPE string;",
            None,
        )
        .await
        .unwrap();

    let mut vault = Vault::new(&mut client, ("app", "main"), "team").unwrap();
    vault
        .write(
            "github",
            &fields(&[("password", PLANTED), ("login", "boog")]),
        )
        .await
        .unwrap();
    vault
        .write("gitlab", &fields(&[("password", "second")]))
        .await
        .unwrap();
    // A second write names one field and keeps the other.
    vault
        .write("github", &fields(&[("password", PLANTED)]))
        .await
        .unwrap();

    let first_page = vault.list(None, Some(1)).await.unwrap();
    assert_eq!(first_page.ids, vec![Value::from("github")]);
    let second_page = vault.list(first_page.next, Some(1)).await.unwrap();
    assert_eq!(second_page.ids, vec![Value::from("gitlab")]);
    let last = vault.list(second_page.next, Some(1)).await.unwrap();
    assert!(last.ids.is_empty() && last.next.is_none(), "{last:?}");

    let revealed = vault.reveal("github", &["password"]).await.unwrap();
    assert_eq!(revealed.get("password"), Some(&Value::from(PLANTED)));
    assert_eq!(
        vault.reveal("github", &[]).await.unwrap(),
        revealed,
        "`*` differs from the one secret"
    );

    recipients_come_and_go(&mut vault).await;

    let trail = client.vault_audit(("app", "main"), None).await.unwrap();
    assert!(
        trail.len() >= 2,
        "two reveals left fewer entries: {trail:?}"
    );
    assert!(
        !format!("{trail:?}").contains(PLANTED),
        "the audit trail holds a value"
    );

    let wrong = client.change_passphrase("not it", NEXT).await.unwrap_err();
    assert!(
        !shown(&wrong).contains(NEXT) && !shown(&wrong).contains("not it"),
        "{}",
        shown(&wrong)
    );
    client.change_passphrase(PASSPHRASE, NEXT).await.unwrap();

    assert_eq!(client.seal().await.unwrap().state, SealState::Sealed);
    let old = client.unseal(PASSPHRASE).await.unwrap_err();
    assert!(!shown(&old).contains(PASSPHRASE), "{}", shown(&old));
    assert_eq!(
        client.unseal(NEXT).await.unwrap().state,
        SealState::Unsealed
    );
    let mut vault = Vault::new(&mut client, ("app", "main"), "team").unwrap();
    assert_eq!(
        vault
            .reveal("github", &["password"])
            .await
            .unwrap()
            .get("password"),
        Some(&Value::from(PLANTED)),
        "a secret did not open after the passphrase changed"
    );

    a_vault_with_its_own_passphrase(&mut client).await;
}

#[tokio::test]
async fn a_node_without_the_vault_frame_is_refused_before_anything_is_sent() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let older = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut theirs = [0_u8; 6];
        socket.read_exact(&mut theirs).await.unwrap();
        socket.write_all(b"TESS\x01\x01").await.unwrap();
        // Whatever the client sends next; an honest client sends nothing.
        let mut after = Vec::new();
        socket.read_to_end(&mut after).await.unwrap();
        after
    });
    // Bounded, so a client that sent the frame anyway fails here rather than
    // hanging on a node that never answers it.
    let bound = std::time::Duration::from_secs(10);
    let mut client = tokio::time::timeout(bound, Client::connect(address))
        .await
        .expect("connecting took longer than the bound")
        .unwrap();
    let refused = tokio::time::timeout(bound, client.unseal(PASSPHRASE))
        .await
        .expect("the client waited on a node without the vault frame")
        .unwrap_err();
    assert!(
        matches!(
            refused,
            Error::NodeTooOld {
                found: 1,
                needed: 2
            }
        ),
        "{refused:?}"
    );
    drop(client);
    let sent = tokio::time::timeout(bound, older)
        .await
        .expect("the fake node never saw the connection close")
        .unwrap();
    assert!(
        sent.is_empty(),
        "the client sent a frame the node cannot read"
    );
}

/// A vault with its own passphrase, declared beside `team`, which opens with
/// the store's: the store's passphrase opens nothing in it.
async fn a_vault_with_its_own_passphrase(client: &mut Client<tokio::net::TcpStream>) {
    const TEAM: &str = "the team passphrase 5d13";
    client
        .run(
            &format!(
                "USE NAMESPACE app; USE DATABASE main; DEFINE VAULT own PASSPHRASE '{TEAM}'; \
                 DEFINE FIELD token ON own TYPE string SECRET;"
            ),
            None,
        )
        .await
        .unwrap();
    let mut own = Vault::new(client, ("app", "main"), "own").unwrap();
    let status = own.status().await.unwrap();
    assert_eq!(
        (status.custody, status.state),
        (Some(Custody::Own), SealState::Unsealed)
    );
    own.write("github", &fields(&[("token", PLANTED)]))
        .await
        .unwrap();
    assert_eq!(own.seal().await.unwrap().state, SealState::Sealed);
    let refused = own.unseal(NEXT).await.unwrap_err();
    assert!(!shown(&refused).contains(NEXT), "{}", shown(&refused));
    assert_eq!(own.unseal(TEAM).await.unwrap().state, SealState::Unsealed);
    own.change_passphrase(TEAM, "the next team one")
        .await
        .unwrap();
    assert_eq!(
        own.reveal("github", &["token"]).await.unwrap().get("token"),
        Some(&Value::from(PLANTED))
    );
    // One in the store's custody reports the store and refuses the vault's own verbs.
    let mut team = Vault::new(client, ("app", "main"), "team").unwrap();
    assert_eq!(team.status().await.unwrap().custody, Some(Custody::Store));
    let refused = team.unseal(NEXT).await.unwrap_err();
    assert!(matches!(refused, Error::Refused { .. }), "{refused:?}");
    assert!(!shown(&refused).contains(NEXT), "{}", shown(&refused));
}

/// A recipient added, listed, removed, and a second removal refused.
async fn recipients_come_and_go(vault: &mut Vault<'_, tokio::net::TcpStream>) {
    vault
        .add_recipient("github", "bob", &[1, 2, 3])
        .await
        .unwrap();
    assert_eq!(
        vault.recipients("github").await.unwrap(),
        BTreeMap::from([("bob".to_owned(), vec![1, 2, 3])])
    );
    vault.remove_recipient("github", "bob").await.unwrap();
    assert!(vault.recipients("github").await.unwrap().is_empty());
    let refused = vault.remove_recipient("github", "bob").await.unwrap_err();
    assert!(matches!(refused, Error::Refused { .. }), "{refused:?}");
}
