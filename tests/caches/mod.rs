//! A space as a cache against a running node (cache contract §6).
//!
//! The release test waits on the wall clock because an expiry is an instant the
//! NODE compares with its own clock.

use std::time::Duration;

use tessaridb_client::{Client, End, Error, Lease, Space, Ttl, Value};

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

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn a_key_holds_a_list() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();
    let text = |items: &[&str]| {
        items
            .iter()
            .map(|item| Value::from(*item))
            .collect::<Vec<_>>()
    };

    assert_eq!(
        cache
            .list_push("l", End::Back, text(&["a", "b", "c", "d"]))
            .await
            .unwrap(),
        4
    );
    assert_eq!(cache.list_len("l").await.unwrap(), 4);
    assert_eq!(
        cache.list_at("l", -1).await.unwrap(),
        Some(Value::from("d"))
    );
    assert_eq!(
        cache.list_range("l", 1, 2).await.unwrap(),
        text(&["b", "c"])
    );
    assert_eq!(
        cache.list_pop("l", End::Front).await.unwrap(),
        Some(Value::from("a"))
    );
    assert_eq!(
        cache.list_pop_many("l", End::Back, 2).await.unwrap(),
        text(&["d", "c"])
    );
    assert_eq!(
        cache
            .list_remove("l", Value::from("b"), None)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        cache.list_len("l").await.unwrap(),
        0,
        "an emptied list is no key"
    );
    assert_eq!(
        cache.list_range("nothing", 0, 5).await.unwrap(),
        Vec::<Value>::new()
    );
    cache
        .list_push("t", End::Back, text(&["1", "2", "3"]))
        .await
        .unwrap();
    cache.list_trim("t", 0, 1).await.unwrap();
    assert_eq!(cache.list_len("t").await.unwrap(), 2);

    assert!(matches!(
        cache.list_push("l", End::Back, Vec::new()).await,
        Err(Error::NotACacheArgument { .. })
    ));
    assert!(matches!(
        cache.list_range("l", -1, 2).await,
        Err(Error::NotACacheArgument { .. })
    ));
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn a_key_holds_a_hash() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();
    assert_eq!(
        cache
            .hash_set(
                "h",
                vec![
                    ("x".to_owned(), Value::from(1)),
                    ("y".to_owned(), Value::from("why"))
                ]
            )
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        cache.hash_get("h", "y").await.unwrap(),
        Some(Value::from("why"))
    );
    assert_eq!(cache.hash_get("h", "z").await.unwrap(), None);
    assert_eq!(cache.hash_incr("h", "x", 4).await.unwrap(), 5);
    assert_eq!(cache.hash_len("h").await.unwrap(), 2);
    assert_eq!(
        cache.hash_remove("h", vec!["y".to_owned()]).await.unwrap(),
        1
    );
    assert_eq!(
        cache.hash_get_all("h").await.unwrap().get("x"),
        Some(&Value::from(5))
    );
    assert!(cache.hash_get_all("nothing").await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn a_key_holds_a_set_of_members() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();
    let text = |items: &[&str]| {
        items
            .iter()
            .map(|item| Value::from(*item))
            .collect::<Vec<_>>()
    };

    assert_eq!(cache.members_add("s", text(&["p", "q"])).await.unwrap(), 2);
    assert!(cache.members_has("s", Value::from("p")).await.unwrap());
    assert!(
        !cache
            .members_has("nothing", Value::from("p"))
            .await
            .unwrap()
    );
    assert_eq!(cache.members_len("s").await.unwrap(), 2);
    assert_eq!(cache.members_remove("s", text(&["q"])).await.unwrap(), 1);
    assert_eq!(cache.members_all("s").await.unwrap(), text(&["p"]));
}

#[tokio::test]
#[ignore = "needs TESSARIDB_BIN; see the module docs"]
async fn a_key_holds_a_ranking() {
    let (_node, mut client) = with_space().await;
    let mut cache = Space::new(&mut client, ("app", "main"), "cache").unwrap();
    assert_eq!(
        cache
            .scores_add(
                "r",
                vec![
                    ("ann".to_owned(), 3.0),
                    ("bob".to_owned(), 1.0),
                    ("cid".to_owned(), 2.0)
                ]
            )
            .await
            .unwrap(),
        3
    );
    assert_eq!(cache.scores_incr("r", "bob", 0.5).await.unwrap(), 1.5);
    assert_eq!(cache.scores_score("r", "ann").await.unwrap(), Some(3.0));
    assert_eq!(cache.scores_rank("r", "ann").await.unwrap(), Some(2));
    assert_eq!(cache.scores_rank("r", "eve").await.unwrap(), None);
    assert_eq!(
        cache.scores_range("r", 0, 1).await.unwrap(),
        vec![("bob".to_owned(), 1.5), ("cid".to_owned(), 2.0)]
    );
    assert_eq!(
        cache.scores_between("r", Some(2.0), None).await.unwrap(),
        vec![("cid".to_owned(), 2.0), ("ann".to_owned(), 3.0)]
    );
    assert_eq!(
        cache
            .scores_remove("r", vec!["ann".to_owned()])
            .await
            .unwrap(),
        1
    );
}
