//! The topic consumer against a running node (consumer contract §7).
//!
//! Wall-clock waits appear here and nowhere else in the crate's tests: a
//! group's deadline is a written instant the NODE compares with its own clock,
//! so no paused test clock on this side can move it.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tessaridb_client::{Client, Consumer, Message, Number, Settle, Value};

use super::Node;

/// A node with `app.jobs` holding `count` messages and a group `workers` on it.
async fn topic_with(count: u64, group: &str) -> (Node, Client) {
    let node = Node::start().await;
    let mut client = node.client().await;
    let mut script = String::from(
        "DEFINE NAMESPACE app; USE NAMESPACE app; DEFINE DATABASE main; USE DATABASE main; \
         DEFINE TOPIC jobs;",
    );
    for n in 1..=count {
        let _ = write!(script, " CREATE jobs:'j{n}' = {{ n: {n} }};");
    }
    script.push(' ');
    script.push_str(group);
    client.run(&script, None).await.unwrap();
    (node, client)
}

fn n_of(message: &Message) -> i64 {
    let Value::Object(fields) = &message.value else {
        panic!("{message:?}")
    };
    match fields.get("n") {
        Some(Value::Number(Number::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

async fn in_flight(client: &mut Client) -> i64 {
    let answers = client
        .run(
            "USE NAMESPACE app; USE DATABASE main; INFO FOR TOPIC jobs;",
            None,
        )
        .await
        .unwrap();
    let Some(tessaridb_client::Answer::Value {
        value: Value::Object(report),
        ..
    }) = answers.last()
    else {
        panic!("{answers:?}")
    };
    let Some(Value::Object(groups)) = report.get("groups") else {
        panic!("{report:?}")
    };
    let Some(Value::Object(group)) = groups.get("workers") else {
        panic!("{groups:?}")
    };
    match group.get("in_flight") {
        Some(Value::Number(Number::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn auto_hands_every_message_to_the_handler_in_order_and_leaves_nothing_in_flight() {
    let (node, mut admin) =
        topic_with(12, "DEFINE GROUP 'workers' ON TOPIC jobs ACK DEADLINE 30s;").await;
    let mut consumer = Consumer::new(node.client().await, ("app", "main"), "jobs", "workers")
        .unwrap()
        .batch(5);
    let stopper = consumer.stopper();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handled = Arc::clone(&seen);
    consumer
        .run_auto(move |message| {
            let handled = Arc::clone(&handled);
            let stopper = stopper.clone();
            async move {
                let mut held = handled.lock().unwrap();
                held.push(n_of(&message));
                if held.len() == 12 {
                    stopper.stop();
                }
                Ok::<(), ()>(())
            }
        })
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), (1..=12).collect::<Vec<_>>());
    assert_eq!(in_flight(&mut admin).await, 0);
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_failing_handler_sees_the_same_message_again_one_delivery_later() {
    let (node, _admin) =
        topic_with(2, "DEFINE GROUP 'workers' ON TOPIC jobs ACK DEADLINE 30s;").await;
    let mut consumer =
        Consumer::new(node.client().await, ("app", "main"), "jobs", "workers").unwrap();
    let stopper = consumer.stopper();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handled = Arc::clone(&seen);
    consumer
        .run_auto(move |message| {
            let handled = Arc::clone(&handled);
            let stopper = stopper.clone();
            async move {
                let mut held = handled.lock().unwrap();
                held.push((message.position, message.deliveries));
                // The first delivery of the first message fails once.
                let failed = message.position == 1 && message.deliveries == 1;
                if held.len() == 3 {
                    stopper.stop();
                }
                if failed { Err(()) } else { Ok(()) }
            }
        })
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![(1, 1), (1, 2), (2, 1)]);
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn manual_leaves_a_message_and_the_group_hands_it_out_again_after_the_deadline() {
    let (node, _admin) = topic_with(
        1,
        "DEFINE GROUP 'workers' ON TOPIC jobs ACK DEADLINE 300ms;",
    )
    .await;
    let mut consumer =
        Consumer::new(node.client().await, ("app", "main"), "jobs", "workers").unwrap();
    let stopper = consumer.stopper();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handled = Arc::clone(&seen);
    tokio::time::timeout(
        Duration::from_secs(10),
        consumer.run_manual(move |message| {
            let handled = Arc::clone(&handled);
            let stopper = stopper.clone();
            async move {
                let mut held = handled.lock().unwrap();
                held.push(message.deliveries);
                if message.deliveries == 1 {
                    Settle::Leave
                } else {
                    stopper.stop();
                    Settle::Ack
                }
            }
        }),
    )
    .await
    .expect("the message came back within ten seconds")
    .unwrap();
    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
}

#[test]
fn names_that_cannot_be_written_into_a_statement_are_refused_before_sending() {
    // Checked without a node: the refusal happens before a byte is written.
    let (near, _far) = tokio::io::duplex(64);
    let client = Client::with_stream(near);
    assert!(Consumer::new(client, ("app", "main"), "jobs; DROP", "workers").is_err());
    let (near, _far) = tokio::io::duplex(64);
    let client = Client::with_stream(near);
    assert!(Consumer::new(client, ("app", "main"), "jobs", "it's").is_err());
}
