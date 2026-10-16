//! The queue worker against a running node (queue contract §7).
//!
//! Wall-clock waits appear here only where the node compares an instant with
//! its own clock — a release's `NOT BEFORE` — which no test clock can move.

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tessaridb_client::query::field;
use tessaridb_client::{Client, Handled, Job, Number, Queue, Value};

use super::Node;

/// A node with `app.jobs`, declared with `clauses`, holding `count` records.
async fn queue_with(count: u64, clauses: &str) -> (Node, Client) {
    let node = Node::start().await;
    let mut client = node.client().await;
    let mut script = format!(
        "DEFINE NAMESPACE app; USE NAMESPACE app; DEFINE DATABASE main; USE DATABASE main; \
         DEFINE QUEUE jobs {clauses};"
    );
    for n in 1..=count {
        let kind = if n % 2 == 0 { "image" } else { "video" };
        let _ = write!(script, " CREATE jobs:{n} = {{ n: {n}, kind: '{kind}' }};");
    }
    client.run(&script, None).await.unwrap();
    (node, client)
}

fn n_of(job: &Job) -> i64 {
    let Value::Object(fields) = job.value() else {
        panic!("{job:?}")
    };
    match fields.get("n") {
        Some(Value::Number(Number::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

async fn left(client: &mut Client) -> usize {
    let answers = client
        .run(
            "USE NAMESPACE app; USE DATABASE main; SELECT * FROM jobs;",
            None,
        )
        .await
        .unwrap();
    match answers.last() {
        Some(tessaridb_client::Answer::Records { records, .. }) => records.len(),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn auto_takes_every_record_once_and_leaves_the_queue_empty() {
    let (node, mut admin) = queue_with(12, "TIMEOUT 30s").await;
    let mut worker = Queue::new(node.client().await, ("app", "main"), "jobs", "resizer")
        .unwrap()
        .batch(5);
    let stopper = worker.stopper();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handled = Arc::clone(&seen);
    worker
        .run_auto(move |job| {
            let handled = Arc::clone(&handled);
            let stopper = stopper.clone();
            async move {
                let mut held = handled.lock().unwrap();
                held.push(n_of(&job));
                if held.len() == 12 {
                    stopper.stop();
                }
                Ok::<(), ()>(())
            }
        })
        .await
        .unwrap();
    assert_eq!(*seen.lock().unwrap(), (1..=12).collect::<Vec<_>>());
    assert_eq!(left(&mut admin).await, 0);
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_failing_handler_sees_the_same_record_again_one_attempt_later() {
    let (node, _admin) = queue_with(2, "TIMEOUT 30s ATTEMPTS 3").await;
    let mut worker = Queue::new(node.client().await, ("app", "main"), "jobs", "resizer").unwrap();
    let stopper = worker.stopper();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let handled = Arc::clone(&seen);
    worker
        .run_auto(move |job| {
            let handled = Arc::clone(&handled);
            let stopper = stopper.clone();
            async move {
                let mut held = handled.lock().unwrap();
                held.push((n_of(&job), job.attempts()));
                let failed = n_of(&job) == 1 && job.attempts() == 1;
                if held.len() == 3 {
                    stopper.stop();
                }
                if failed { Err(()) } else { Ok(()) }
            }
        })
        .await
        .unwrap();
    // Released at once and claimed again first: arrival order.
    assert_eq!(*seen.lock().unwrap(), vec![(1, 1), (2, 1), (1, 2)]);
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_condition_takes_only_the_records_that_match() {
    let (node, mut admin) = queue_with(6, "TIMEOUT 30s").await;
    let mut worker = Queue::new(node.client().await, ("app", "main"), "jobs", "resizer").unwrap();
    let images = field("kind").eq("image");
    let taken = worker.claim(10, Some(&images)).await.unwrap();
    assert_eq!(taken.iter().map(n_of).collect::<Vec<_>>(), vec![2, 4, 6]);
    for job in &taken {
        worker.done(job).await.unwrap();
    }
    assert_eq!(left(&mut admin).await, 3);
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_release_with_a_delay_hands_the_record_out_only_after_it() {
    let (node, _admin) = queue_with(1, "TIMEOUT 30s NOT BEFORE due").await;
    let mut worker = Queue::new(node.client().await, ("app", "main"), "jobs", "resizer").unwrap();
    let taken = worker.claim(1, None).await.unwrap();
    assert_eq!(taken.len(), 1);
    worker
        .release(&taken[0], Some(Duration::from_millis(400)))
        .await
        .unwrap();
    assert!(
        worker.claim(1, None).await.unwrap().is_empty(),
        "handed out before its delay"
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    let again = worker.claim(1, None).await.unwrap();
    assert_eq!(
        again
            .iter()
            .map(|job| (n_of(job), job.attempts()))
            .collect::<Vec<_>>(),
        vec![(1, 2)]
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn manual_leave_keeps_the_hold_and_stop_hands_back_the_rest() {
    let (node, mut admin) = queue_with(3, "TIMEOUT 30s").await;
    let mut worker = Queue::new(node.client().await, ("app", "main"), "jobs", "resizer")
        .unwrap()
        .batch(3);
    let stopper = worker.stopper();
    worker
        .run_manual(move |_job| {
            let stopper = stopper.clone();
            async move {
                // Stop after the first: the two the handler never saw go back.
                stopper.stop();
                Handled::Leave
            }
        })
        .await
        .unwrap();
    let mut other = Queue::new(node.client().await, ("app", "main"), "jobs", "other").unwrap();
    let free = other.claim(3, None).await.unwrap();
    assert_eq!(free.iter().map(n_of).collect::<Vec<_>>(), vec![2, 3]);
    assert_eq!(left(&mut admin).await, 3);
}
