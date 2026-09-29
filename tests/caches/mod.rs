//! A space as a cache against a running node (cache contract §6).
//!
//! The release test waits on the wall clock because an expiry is an instant the
//! NODE compares with its own clock.

use std::time::Duration;

use tessaridb_client::{Client, Error, Lease, Space, Ttl, Value};

use super::Node;

async fn with_space() -> (Node, Client) {
    let node = Node::start().await;
    let mut client = node.client().await;
    client
        .run(
            "DEFINE NAMESPACE app; USE NAMESPACE app; DEFINE DATABASE main; USE DATABASE main; \
             DEFINE SPACE cache;",
            None,
        )
        .await
        .unwrap();
    (node, client)
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn a_value_is_stored_read_counted_expired_and_deleted() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();

    let at = Value::Datetime {
        seconds: 1_790_000_000,
        nanos: 5,
    };
    cache
        .set("user:42", at.clone(), Some(Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(
        cache.get("user:42").await.unwrap(),
        Some(at),
        "the value came back changed"
    );
    assert!(
        matches!(cache.ttl("user:42").await.unwrap(), Ttl::Expires(left) if left <= Duration::from_secs(30))
    );

    // A plain set clears the expiry — the rule every cache over a space must know.
    cache.set("user:42", Value::from(1), None).await.unwrap();
    assert_eq!(cache.ttl("user:42").await.unwrap(), Ttl::Never);
    assert!(
        cache
            .expire("user:42", Duration::from_secs(60))
            .await
            .unwrap()
    );
    assert!(cache.persist("user:42").await.unwrap());
    assert_eq!(cache.ttl("nobody").await.unwrap(), Ttl::Absent);

    assert_eq!(cache.incr("hits", 5).await.unwrap(), 5);
    assert_eq!(cache.incr("hits", 1).await.unwrap(), 6);

    assert!(
        cache
            .set_if_absent("once", Value::from(1), None)
            .await
            .unwrap()
    );
    assert!(
        !cache
            .set_if_absent("once", Value::from(2), None)
            .await
            .unwrap()
    );
    assert!(
        !cache
            .set_if_present("never", Value::from(1), None)
            .await
            .unwrap()
    );
    assert!(
        !cache
            .compare_and_set("once", Value::from(9), Value::from(3), None)
            .await
            .unwrap()
    );
    assert!(
        cache
            .compare_and_set("once", Value::from(1), Value::from(3), None)
            .await
            .unwrap()
    );

    cache.set("it's\\here", Value::Null, None).await.unwrap();
    let keys = cache.keys(Some("it"), None, 10).await.unwrap();
    assert_eq!(
        keys,
        vec!["it's\\here".to_owned()],
        "a quoted key did not come back as the string"
    );
    assert_eq!(
        cache.keys(None, Some("it's\\here"), 1).await.unwrap(),
        vec!["once".to_owned()]
    );

    assert!(
        cache.delete("it's\\here").await.unwrap(),
        "a key holding NULL is a key"
    );
    assert!(!cache.delete("it's\\here").await.unwrap());
    assert_eq!(cache.get("it's\\here").await.unwrap(), None);

    assert!(matches!(
        cache.expire("once", Duration::ZERO).await,
        Err(Error::NotACacheArgument { .. })
    ));
    assert!(matches!(
        cache.keys(None, None, 0).await,
        Err(Error::NotACacheArgument { .. })
    ));
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn get_or_set_loads_once_and_then_answers_what_is_stored() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();
    let ttl = Duration::from_secs(30);
    let first = cache
        .get_or_set("page", ttl, || async { Value::from("rendered") })
        .await
        .unwrap();
    let second = cache
        .get_or_set("page", ttl, || async { Value::from("again") })
        .await
        .unwrap();
    assert_eq!(
        (first, second),
        (Value::from("rendered"), Value::from("rendered"))
    );
    assert!(
        matches!(cache.ttl("page").await.unwrap(), Ttl::Expires(_)),
        "stored without its ttl"
    );
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn a_lease_is_extended_by_its_holder_and_released_so_the_next_can_take_it() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();
    let ttl = Duration::from_secs(30);

    let lease: Lease = cache
        .lock("report", ttl, None)
        .await
        .unwrap()
        .expect("a free lock");
    assert_eq!(
        lease.holder.len(),
        32,
        "a default holder is 128 bits of hex"
    );
    assert!(
        cache
            .lock("report", ttl, Some("other"))
            .await
            .unwrap()
            .is_none(),
        "a held lock was taken"
    );
    assert!(
        cache.extend(&lease, None).await.unwrap(),
        "its holder could not extend it"
    );
    let stranger = Lease {
        holder: "other".to_owned(),
        ..lease.clone()
    };
    assert!(
        !cache.release(stranger).await.unwrap(),
        "another holder released it"
    );
    assert!(cache.release(lease).await.unwrap());

    // Released by an expiring write: a hand-back without one would leave the key
    // for ever and every later lock would be refused.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        cache
            .lock("report", ttl, Some("next"))
            .await
            .unwrap()
            .is_some(),
        "a released lock could not be taken again, so the release left it permanent"
    );
    let Some(Value::String(held)) = cache.get("report").await.unwrap() else {
        panic!("the lock holds no holder")
    };
    assert_eq!(held, "next");
}
