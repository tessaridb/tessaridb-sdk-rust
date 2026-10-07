//! The client against a node that is actually running.
//!
//! Every other test in this crate drives a mock: a duplex, or a `tokio::spawn`
//! that writes the bytes this client expects. Those tests are worth having — they
//! pin the framing exactly — but they share one blind spot, and it is the
//! important one. A mock is written from the same understanding of the protocol
//! as the client, by the same author, at the same time. When that understanding
//! is wrong, the mock is wrong in precisely the same direction, and the pair
//! agree with each other all the way to production.
//!
//! So this file exists to be the one tier that can disagree. The node here is the
//! shipped binary, started as a child process, speaking whatever it actually
//! speaks.
//!
//! # Why these are `#[ignore]`
//!
//! `cargo test` must stay hermetic: no binary, no network peer, no environment.
//! These run when asked for, which is also when the binary is known to exist:
//!
//! ```text
//! TESSARIDB_BIN=/path/to/tessaridb cargo test --test node -- --ignored
//! ```
//!
//! # Why the binary comes from the environment
//!
//! This crate depends on the database's repository by no mechanism — no path
//! dependency, no git revision, no vendored source. Naming a binary at run time
//! is a dependency of *this test*, not of the crate, and nothing about it reaches
//! `Cargo.toml`. A client for this protocol has to be writable in a language that
//! cannot link the server at all, and that stays true here.
//!
//! # Why a missing binary fails instead of skipping
//!
//! A test that quietly skips when its environment is absent reports success for a
//! node nobody connected to. That is the same shape as a search that returns zero
//! because it was pointed at nothing, and this project has already been caught by
//! it twice. Asking for these tests and getting a pass must mean a node answered.

// Assertions are exactly where a panic is the correct outcome; these lints target
// production paths, where a panic is a defect.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]

mod caches;
mod consumers;
mod secured;
mod support;
mod vaults;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tessaridb_client::query::{Order, Select, field as on_field};
use tessaridb_client::{
    Answer, Arrival, Became, Bucket, Change, Client, Condition, Error, Feed, Follow, FromRecord,
    MappingFault, Number, Operations, Row, Value,
};

use crate::support::{build, expected_parameters, read_corpus};

/// How long a node gets to bind its port before the test gives up.
///
/// Bounded rather than open-ended: a wait with no deadline turns "the node did
/// not start" into a suite that hangs, which is the failure that costs the most
/// to diagnose.
const STARTUP_BUDGET: Duration = Duration::from_secs(10);

/// Held from choosing a port until the node has actually bound it.
///
/// Asking the OS for a free port answers a question about *now*: the probe
/// listener is closed so the child can take the number, and in that gap any
/// other test asking the same question can be handed the same one. With one port
/// per node it is a rare loss; with two ports and several nodes starting at once
/// it stops being rare, and it arrives as a ten-second startup timeout whose
/// message says nothing about ports.
///
/// Serialising the window is what keeps the answer true until it is used. It
/// costs the suite a few hundred milliseconds in total, and node startup was
/// never the expensive part.
static STARTING: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// A node started for one test, and stopped when that test ends.
struct Node {
    child: Child,
    address: String,
}

impl Node {
    /// Start the shipped binary on a free port and wait until it answers.
    async fn start() -> Self {
        let binary = std::env::var("TESSARIDB_BIN").unwrap_or_else(|_| {
            panic!(
                "TESSARIDB_BIN is not set.\n\
                 These tests exercise a real node and have nothing to fall back \
                 on. A version of this that skipped here would report success \
                 for a client that never connected to anything, which is worse \
                 than a failure because it looks like evidence.\n\
                 Set it to the shipped binary and run with `--ignored`."
            )
        });

        let _slot = STARTING.lock().await;

        // Port 0 asks the OS for one that is free, which is what lets these run
        // beside the database's own suites — those bind fixed ports, and a fixed
        // port chosen here would collide with them exactly when both are running.
        let port = {
            let probe = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap_or_else(|e| panic!("could not find a free port: {e}"));
            probe.local_addr().unwrap().port()
        };
        let address = format!("127.0.0.1:{port}");

        // The store is in memory: no path argument, so it exists for this child
        // and is gone when it dies. Nothing on disk to clean up, and no way for
        // one test to see another's writes.
        let child = Command::new(&binary)
            .arg("--serve")
            .arg(&address)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("could not start {binary}: {e}"));

        let node = Self { child, address };
        node.wait_until_listening().await;
        node
    }

    /// Poll until the port answers, or fail naming how long was spent.
    async fn wait_until_listening(&self) {
        // Measured as elapsed rather than as a deadline instant: adding a
        // duration to an instant can overflow, and this crate denies bare
        // arithmetic for exactly that class of thing. Elapsed time only compares.
        let started = std::time::Instant::now();
        loop {
            if tokio::net::TcpStream::connect(&self.address).await.is_ok() {
                return;
            }
            assert!(
                started.elapsed() < STARTUP_BUDGET,
                "the node did not accept a connection on {} within {:?}",
                self.address,
                STARTUP_BUDGET
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A client already connected to this node.
    async fn client(&self) -> Client {
        Client::connect(&self.address)
            .await
            .unwrap_or_else(|e| panic!("could not connect to the node at {}: {e}", self.address))
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        // By handle, never by name: the database's own suites run this same
        // binary, and killing by pattern would take theirs down too.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A namespace, a database and a collection, so a record has somewhere to live.
///
/// One connection is one session, so the `USE` statements below are still in
/// force for every later statement on the same client — which is what makes this
/// a setup step rather than a prefix every test has to repeat.
///
/// `DEFINE COLLECTION` rather than `DEFINE TABLE`: the records these tests write
/// carry fields nobody declared, and a bare `DEFINE TABLE` is now refused for
/// exactly that — *"the table users declares no fields, so there is nothing for
/// it to be strict about"*. The node names two ways out and this is the one that
/// says what these tests actually mean; `SCHEMALESS` is for a table that does
/// declare fields and admits others too.
const PREAMBLE: &str = "DEFINE NAMESPACE app; USE NAMESPACE app; \
                        DEFINE DATABASE main; USE DATABASE main; \
                        DEFINE COLLECTION users;";

/// Pull the records out of an answer, or say what arrived instead.
fn records(answer: &Answer) -> &Vec<(String, Value)> {
    match answer {
        Answer::Records { records, .. } => records,
        other => panic!("expected records, and the node answered {other:?}"),
    }
}

/// Read one field of a record's value.
fn field<'a>(value: &'a Value, name: &str) -> &'a Value {
    match value {
        Value::Object(fields) => fields
            .get(name)
            .unwrap_or_else(|| panic!("no field {name} in {value:?}")),
        other => panic!("a record should be an object, and this is {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_node_that_is_really_running_completes_the_greeting() {
    let node = Node::start().await;

    // Connecting is the assertion. The greeting is where a wrong protocol or a
    // wrong version is refused, so reaching this line at all means the magic, the
    // major and the minor were what this client believes them to be — against the
    // node itself rather than against a fixture repeating the belief back.
    let mut client = node.client().await;

    // And the minor this client announced is the one that earns a class: the
    // node sends the class byte only to a greeting of minor 3 or later.
    match client.run("SELEKT * FROM nothing;", None).await {
        Err(Error::Refused { class, .. }) => assert_eq!(
            class,
            Some(tessaridb_client::RefusalClass::Invalid),
            "a statement the node cannot read is classed invalid"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_script_runs_against_a_real_node_and_its_records_come_back() {
    let node = Node::start().await;
    let mut client = node.client().await;

    let answers = client
        .run(
            &format!("{PREAMBLE} CREATE users:1 = {{ name: 'ada', age: 36 }};"),
            None,
        )
        .await
        .expect("the preamble and the write should be accepted");
    assert_eq!(
        answers.len(),
        6,
        "one answer per statement, in order; got {answers:?}"
    );

    let answers = client
        .run("SELECT * FROM users;", None)
        .await
        .expect("the read should be accepted");
    let found = records(&answers[0]);
    assert_eq!(found.len(), 1, "one record was written; got {found:?}");

    let (id, value) = &found[0];
    assert_eq!(
        id, "1",
        "the identity comes back as the node spells it — bare, not qualified by \
         its table, because the answer already says which table it read"
    );
    assert_eq!(field(value, "name"), &Value::String("ada".to_owned()));
    assert_eq!(field(value, "age"), &Value::Number(Number::Integer(36)));
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_bound_value_reaches_a_real_node_as_data_and_never_as_script() {
    let node = Node::start().await;
    let mut client = node.client().await;

    client
        .run(PREAMBLE, None)
        .await
        .expect("the preamble should be accepted");

    // Punctuation that would end the statement and begin a destructive one, if
    // this ever became syntax. The mock tests already assert it stays in the
    // parameter map; what they cannot show is what the *node* does with it, and
    // that is the half that matters.
    let hostile = "'; DROP TABLE users; --";
    client
        .run_with(
            "CREATE users:2 = { name: $name };",
            None,
            [("name", Value::from(hostile))],
        )
        .await
        .expect("a bound value is data, so this is an ordinary write");

    let answers = client
        .run("SELECT * FROM users;", None)
        .await
        .expect("the table should still be there — which is the point");
    let found = records(&answers[0]);

    assert_eq!(
        found.len(),
        1,
        "the write landed and the table survives; got {found:?}"
    );
    assert_eq!(
        field(&found[0].1, "name"),
        &Value::String(hostile.to_owned()),
        "the value came back exactly as sent, having never been read as grammar"
    );
}

/// The struct the mapping is aimed at.
///
/// Declared here rather than shared with `mapping.rs`: an integration test
/// binary is its own crate, and a shared fixture between the two would be a
/// third thing to keep honest for no gain.
#[derive(Debug, PartialEq)]
struct User {
    id: String,
    name: String,
    age: i64,
    nickname: Option<String>,
}

impl FromRecord for User {
    fn from_row(mut row: Row) -> Result<Self, MappingFault> {
        Ok(Self {
            id: row.id().to_owned(),
            name: row.take("name")?,
            age: row.take("age")?,
            nickname: row.take("nickname")?,
        })
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_real_answer_maps_into_a_declared_struct() {
    // The mapping's hermetic tests convert values this process built, which
    // proves the conversions agree with their author. This is the tier that can
    // disagree: the values here were encoded by the node, crossed a socket, and
    // were decoded by the codec (LR-SDK-006).
    let node = Node::start().await;
    let mut client = node.client().await;

    client
        .run(
            &format!(
                "{PREAMBLE} CREATE users:1 = {{ name: 'ada', age: 36, nickname: 'countess' }}; \
                 CREATE users:2 = {{ name: 'grace', age: 45 }};"
            ),
            None,
        )
        .await
        .expect("the preamble and both writes should be accepted");

    let answers = client
        .run("SELECT * FROM users;", None)
        .await
        .expect("the read should be accepted");
    let mut users: Vec<User> = answers
        .into_iter()
        .next()
        .expect("one statement, one answer")
        .records_into()
        .expect("the records map into the declared struct");
    users.sort_by(|left, right| left.id.cmp(&right.id));

    assert_eq!(
        users,
        vec![
            User {
                id: "1".to_owned(),
                name: "ada".to_owned(),
                age: 36,
                nickname: Some("countess".to_owned()),
            },
            User {
                id: "2".to_owned(),
                name: "grace".to_owned(),
                age: 45,
                // Written without the field at all. Whether the node answers
                // with the key missing or with an explicit absent marker, the
                // caller's `Option` gives the same answer — which is the whole
                // reason `FromValue::absent` exists.
                nickname: None,
            },
        ]
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_field_the_node_did_not_write_comes_back_as_no_key_at_all() {
    // MEASURED 2026-08-30 against the shipped binary: a field that was never
    // written is **absent from the object**. The node does not send the key
    // carrying `Value::None`.
    //
    // That is the fact this whole design turns on. `FromValue::absent` — the
    // default method that lets `Option<T>` answer for a key that is not there —
    // is not an ergonomic nicety here; without it EVERY optional field would
    // report `NoSuchField` against a real node, and `Option` would mean
    // "nullable" while being unable to express "optional".
    //
    // The mapping accepts the other shape too, because accepting it costs
    // nothing and a store may later write an explicit marker. But this
    // assertion pins what is true today, so a change on the node's side fails
    // here loudly instead of quietly altering what `Option` means.
    let node = Node::start().await;
    let mut client = node.client().await;

    client
        .run(
            &format!("{PREAMBLE} CREATE users:1 = {{ name: 'grace' }};"),
            None,
        )
        .await
        .expect("the preamble and the write should be accepted");

    let answers = client
        .run("SELECT * FROM users;", None)
        .await
        .expect("the read should be accepted");
    let found = records(&answers[0]);
    let Value::Object(fields) = &found[0].1 else {
        panic!("a record holds an object; got {:?}", found[0].1);
    };

    assert_eq!(
        fields.get("nickname"),
        None,
        "an unwritten field is absent from the object; the node does not send the \
         key carrying an explicit marker. `FromValue::absent` is what makes \
         `Option` work against that, so this is the assertion that justifies it"
    );

    // The neighbouring field is present, which is what stops the assertion above
    // from passing against a record that came back empty for some other reason.
    assert_eq!(
        fields.get("name"),
        Some(&Value::String("grace".to_owned())),
        "the record did arrive and does carry the field that was written"
    );
}

/// The session context a second connection needs.
///
/// The namespace, database and table are already defined by whoever ran
/// [`PREAMBLE`]; what a new connection lacks is not the schema but the `USE`
/// statements, because one connection is one session.
const USE_CONTEXT: &str = "USE NAMESPACE app; USE DATABASE main;";

/// How long a change gets to arrive before the test says it did not.
///
/// Bounded on purpose: `Feed::next` waits for the node to push, so an
/// unbounded wait would hang the whole suite rather than fail it, and a hung
/// suite reports nothing at all.
const CHANGE_BUDGET: Duration = Duration::from_secs(10);

/// The next change, or a failure naming the budget it outlived.
///
/// The stream type is spelled out because `Feed<S>` carries no default, unlike
/// `Client<S = TcpStream>` beside it. Recorded rather than fixed here: the
/// asymmetry is real and the fix is one word, but it changes published API and
/// this test was written for something else.
async fn next_change(feed: &mut Feed<tokio::net::TcpStream>) -> Change {
    match next_arrival(feed).await {
        Arrival::Change(change) => change,
        other => panic!("a feed with no condition sent {other:?}"),
    }
}

/// The next thing a feed hands over, or a failure naming the budget it outlived.
async fn next_arrival(feed: &mut Feed<tokio::net::TcpStream>) -> Arrival {
    match tokio::time::timeout(CHANGE_BUDGET, feed.next()).await {
        Ok(Ok(Some(arrival))) => arrival,
        Ok(Ok(None)) => panic!("the feed ended before the change arrived"),
        Ok(Err(error)) => panic!("the feed failed: {error}"),
        Err(elapsed) => panic!("no change arrived within {CHANGE_BUDGET:?} ({elapsed})"),
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_change_written_on_another_connection_arrives_on_the_subscription() {
    // The two connections are the criterion, not an implementation detail. A
    // test that subscribes and asserts the subscription was *accepted* passes
    // with no push ever occurring, and one that writes and reads on a single
    // connection may be reading its own echo. Only a change crossing between
    // two sockets shows that the node pushed anything.
    let node = Node::start().await;

    let mut writer = node.client().await;
    writer
        .run(PREAMBLE, None)
        .await
        .expect("the preamble should be accepted");

    // `follow` takes the client by value: a socket delivering changes is not
    // also answering scripts. So the watcher is a second connection, and needs
    // its own session context before it subscribes.
    let mut watcher = node.client().await;
    watcher
        .run(USE_CONTEXT, None)
        .await
        .expect("the watcher's session context should be accepted");
    let mut feed = watcher
        .follow(&Follow::everything().to_table("users"))
        .await
        .expect("the subscription should be accepted");

    writer
        .run("CREATE users:1 = { name: 'ada' };", None)
        .await
        .expect("the write should be accepted");

    let change = next_change(&mut feed).await;

    assert_eq!(change.table, "users");
    assert_eq!(change.id, "1");
    let Became::Written(value) = &change.became else {
        panic!("a CREATE writes; got {:?}", change.became);
    };
    assert_eq!(
        field(value, "name"),
        &Value::String("ada".to_owned()),
        "the pushed value is the one that was written, not merely a notification \
         that something happened"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn the_table_filter_excludes_a_table_it_was_not_given() {
    // Proven by ORDERING rather than by a timeout. The obvious test writes to
    // the excluded table and waits for nothing to arrive — which passes when the
    // filter works and equally when the node is merely slow, and costs its whole
    // budget every run.
    //
    // Here the excluded write goes FIRST and the watched write SECOND. If the
    // filter leaks, the excluded change is already in the feed ahead of the one
    // being waited for, and the assertion fails on it. The positive control is
    // built in: the `users` change must arrive, or the budget fails the test.
    let node = Node::start().await;

    let mut writer = node.client().await;
    writer
        .run(&format!("{PREAMBLE} DEFINE COLLECTION logs;"), None)
        .await
        .expect("the preamble and the second table should be accepted");

    let mut watcher = node.client().await;
    watcher
        .run(USE_CONTEXT, None)
        .await
        .expect("the watcher's session context should be accepted");
    let mut feed = watcher
        .follow(&Follow::everything().to_table("users"))
        .await
        .expect("the subscription should be accepted");

    writer
        .run("CREATE logs:1 = { line: 'ignored' };", None)
        .await
        .expect("the excluded write should be accepted");
    writer
        .run("CREATE users:1 = { name: 'ada' };", None)
        .await
        .expect("the watched write should be accepted");

    // Read until the watched change appears. `Follow::everything` starts at
    // position 0, so the log is replayed from the beginning and unrelated
    // entries may precede it; what must never appear is the excluded table.
    loop {
        let change = next_change(&mut feed).await;
        assert_ne!(
            change.table, "logs",
            "a subscription narrowed to `users` delivered a change for `logs`; \
             the filter is not filtering"
        );
        if change.table == "users" && change.id == "1" {
            break;
        }
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_narrowed_feed_sends_the_match_the_leaving_and_how_far_it_read() {
    // Protocol §3.7 and §3.15 against a node of minor 4: the feed holds exactly
    // the records the condition matches — one that stops matching arrives as a
    // removal — and a run of skipped changes is told as a Progress, so the
    // resume point keeps up with the log.
    let node = Node::start().await;
    let mut writer = node.client().await;
    writer
        .run(PREAMBLE, None)
        .await
        .expect("the preamble should be accepted");
    let mut watcher = node.client().await;
    watcher
        .run(USE_CONTEXT, None)
        .await
        .expect("the watcher's session context should be accepted");
    let mut feed = watcher
        .follow(
            &Follow::everything()
                .to_table("users")
                .matching("age > $least")
                .binding("least", Value::from(40_i64)),
        )
        .await
        .expect("the narrowed subscription should be accepted");

    for script in [
        "CREATE users:1 = { name: 'ada', age: 36 };",
        "CREATE users:2 = { name: 'grace', age: 45 };",
        "UPDATE users:2 SET age = 30;",
        "CREATE users:3 = { name: 'alan', age: 20 };",
    ] {
        writer
            .run(script, None)
            .await
            .expect("the write is accepted");
    }

    // A Progress may come whenever the feed idled after a skip — after the
    // preamble as well as after users:3 — so the changes are read past any.
    let mut changes = Vec::new();
    while changes.len() < 2 {
        if let Arrival::Change(change) = next_arrival(&mut feed).await {
            changes.push(change);
        }
    }
    let [matched, left] = changes.as_slice() else {
        unreachable!("two changes were read");
    };
    assert_eq!(matched.id, "2", "users:1 does not match and is not sent");
    assert!(matches!(matched.became, Became::Written(_)));
    assert_eq!((left.id.as_str(), &left.became), ("2", &Became::Removed));
    // users:3 is skipped, and the feed says how far it read past the removal.
    loop {
        match next_arrival(&mut feed).await {
            Arrival::Progress(read) if read.sequence > left.sequence => break,
            Arrival::Progress(_) => {}
            other => panic!("only users:3, which does not match, follows: {other:?}"),
        }
    }
}

/// A node serving the HTTP surface as well as the wire protocol.
///
/// A separate constructor rather than a flag on [`Node::start`]: the HTTP
/// surface is a second port, and every wire test would otherwise pay for a
/// listener it never touches.
///
/// Both ports, because the object routes need a namespace, a database and a
/// bucket to exist before they will do anything, and there is no way to declare
/// those over HTTP that this client offers — `/script` is a route the SDK does
/// not produce. So the fixture is written over the wire and read over HTTP,
/// which is also the arrangement a caller ends up with.
struct HttpNode {
    child: Child,
    address: String,
    wire: String,
}

impl HttpNode {
    /// A node with **no user**, so every route runs for anyone.
    async fn start() -> Self {
        Self::start_with(None).await
    }

    /// A node **closed** by an initial user, so every route needs a credential.
    ///
    /// The posture has to be chosen at startup because it is not something a
    /// running node changes on request — and a credential test against an open
    /// store passes whether or not the header was ever sent, which is the one
    /// way this wave could have shipped nothing while reporting four green
    /// tests.
    ///
    /// Both variables or neither: the node refuses to start on half of them, and
    /// that refusal is worth keeping rather than working around, because a store
    /// that came up open through a typo looks exactly like one that came up
    /// right.
    async fn start_closed(name: &str, password: &str) -> Self {
        Self::start_with(Some((name, password))).await
    }

    async fn start_with(closed_as: Option<(&str, &str)>) -> Self {
        let binary = std::env::var("TESSARIDB_BIN").unwrap_or_else(|_| {
            panic!(
                "TESSARIDB_BIN is not set.\n\
                 These tests exercise a real node and have nothing to fall back \
                 on. Set it to the shipped binary and run with `--ignored`."
            )
        });

        let _slot = STARTING.lock().await;

        // Both probes are held at once and released together. Asking twice in
        // succession, releasing each before the next, lets the OS hand back the
        // number it just took away — and the node is then told to bind one port
        // for two surfaces.
        let http_probe = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("could not find a free port: {e}"));
        let wire_probe = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("could not find a second free port: {e}"));
        let address = format!("127.0.0.1:{}", http_probe.local_addr().unwrap().port());
        let wire = format!("127.0.0.1:{}", wire_probe.local_addr().unwrap().port());
        assert_ne!(address, wire, "the two surfaces need two ports");
        drop(http_probe);
        drop(wire_probe);

        let mut command = Command::new(&binary);
        command
            .arg("--http")
            .arg(&address)
            .arg("--serve")
            .arg(&wire)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if let Some((name, password)) = closed_as {
            command
                .env("TESSARIDB_INITIAL_USER", name)
                .env("TESSARIDB_INITIAL_PASSWORD", password);
        }
        let child = command
            .spawn()
            .unwrap_or_else(|e| panic!("could not start {binary}: {e}"));

        let mut node = Self {
            child,
            address,
            wire,
        };
        // Both ports, because a test that writes its fixture over the wire the
        // instant the HTTP port answers is racing a listener that has not bound
        // yet — and it would fail perhaps one run in fifty, which is the worst
        // rate for finding out why.
        let started = std::time::Instant::now();
        loop {
            let http_up = tokio::net::TcpStream::connect(&node.address).await.is_ok();
            let wire_up = tokio::net::TcpStream::connect(&node.wire).await.is_ok();
            if http_up && wire_up {
                return node;
            }
            // Whether the child is still running separates "slow to bind" from
            // "refused to start and exited", which are the same ten-second
            // silence from out here and want opposite things looked at.
            let alive = match node.child.try_wait() {
                Ok(None) => "still running".to_owned(),
                Ok(Some(status)) => format!("already exited with {status}"),
                Err(e) => format!("could not be asked: {e}"),
            };
            assert!(
                started.elapsed() < STARTUP_BUDGET,
                "the node ({alive}, http {http_up}, wire {wire_up}) did not accept \
                 connections on {} and {} within {STARTUP_BUDGET:?}",
                node.address,
                node.wire
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn operations(&self) -> Operations {
        Operations::at(&self.address)
    }

    /// Declare a namespace, a database and a bucket, and hand back the bucket.
    ///
    /// Written over the wire because the object routes refuse until these exist,
    /// and because a bucket is not a table: `DEFINE TABLE files` of the same
    /// name is refused by the node with *"files is not a bucket"*, which is a
    /// distinction this fixture would otherwise get wrong silently.
    async fn bucket(&self) -> Bucket {
        let mut client = Client::connect(&self.wire)
            .await
            .unwrap_or_else(|e| panic!("could not reach the wire port at {}: {e}", self.wire));
        client
            .run(
                "DEFINE NAMESPACE app; USE NAMESPACE app; \
                 DEFINE DATABASE main; USE DATABASE main; \
                 DEFINE BUCKET files;",
                None,
            )
            .await
            .expect("the node should accept a namespace, a database and a bucket");
        self.operations().bucket("app", "main", "files")
    }

    /// Ask the node to stop, and return while it is still serving.
    ///
    /// The node answers a `SIGTERM` with a staged shutdown whose first stage
    /// says *not ready* and keeps serving for a few seconds, so a load balancer
    /// learns it before the port goes. That window is the only way to observe
    /// the leaving state, and it is what `ready` exists to report.
    ///
    /// Sent through `kill` rather than through a signalling crate: this is the
    /// one place a test needs a signal, and it is not worth a dependency.
    fn ask_to_stop(&self) {
        let sent = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .unwrap_or_else(|e| panic!("could not send SIGTERM: {e}"));
        assert!(sent.success(), "kill -TERM did not succeed: {sent}");
    }
}

impl Drop for HttpNode {
    fn drop(&mut self) {
        // By handle, never by name — the database's own suites run this binary.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn the_nodes_own_metrics_come_back_over_http() {
    // U04 of the SGE inventory, and the route that proves the transport: it is
    // the one open-auth route that answers `text/plain`, so it exercises the
    // whole request/response path without also settling the JSON question.
    let node = HttpNode::start().await;

    let metrics = node
        .operations()
        .metrics()
        .await
        .expect("the node should answer /metrics");

    // Asserted on content, not on length. A body that came back empty, or a
    // status page from something else entirely, would satisfy `!is_empty()`.
    assert!(
        metrics.contains("# HELP") || metrics.contains("# TYPE"),
        "this should be the Prometheus text format the node emits; got {} bytes \
         beginning {:?}",
        metrics.len(),
        metrics.chars().take(80).collect::<String>()
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_peer_that_is_not_speaking_http_is_refused_rather_than_mis_read() {
    // Pointed at the WIRE port, which accepts the connection and then answers
    // with something that is not an HTTP response at all.
    //
    // This is the failure worth pinning: the transport reads a status line, and
    // a reader that shrugged at an unrecognised one would carry on into the
    // header loop and return a plausible empty answer. `/metrics` returning ""
    // reads as "this node has no metrics" rather than "you asked the wrong
    // port", and nothing downstream could tell the difference.
    let wire_only = Node::start().await;
    let operations = Operations::at(&wire_only.address);

    let refused = operations
        .metrics()
        .await
        .expect_err("the wire port does not speak HTTP, so this must not succeed");

    assert!(
        matches!(refused, Error::NotThisProtocol | Error::Truncated),
        "the refusal should name what went wrong; got {refused:?}"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_well_node_reports_ok_and_how_far_it_has_committed() {
    let node = HttpNode::start().await;

    let condition = node
        .operations()
        .health()
        .await
        .expect("a node that just started should report its condition");

    // Matched rather than compared, because the commit position of a fresh
    // in-memory store is not this test's claim — that it arrives at all is.
    assert!(
        matches!(condition, Condition::Ok { .. }),
        "a node that just started should be well; got {condition:?}"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn readiness_and_health_diverge_when_the_node_is_leaving() {
    // The two routes agree on every healthy node, which is exactly what makes
    // collapsing them into one method tempting. They are worth separating only
    // where they stop agreeing, so that is what this asserts.
    //
    // A supervisor acts on the difference: not-ready means *stop sending
    // traffic*, not-healthy means *restart it*. A client that reported one for
    // the other would take a node out of service that needed restarting, or
    // restart one that was deliberately draining.
    let node = HttpNode::start().await;

    // Well first, so the divergence below is a change rather than a starting
    // condition — without this the test would pass against a client that
    // hard-coded `Leaving`.
    assert!(
        matches!(
            node.operations()
                .ready()
                .await
                .expect("ready before stopping"),
            Condition::Ok { .. }
        ),
        "a node that has not been asked to stop should be ready"
    );

    node.ask_to_stop();

    // The node keeps serving through the lame-duck window; this races that
    // window rather than waiting for it, so it asks immediately and treats a
    // still-ready answer as the retry rather than as the verdict.
    let mut leaving = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        match node.operations().ready().await {
            Ok(Condition::Leaving) => {
                leaving = Some(Condition::Leaving);
                break;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            Err(e) => panic!("the node stopped serving before it said it was leaving: {e}"),
        }
    }
    assert_eq!(
        leaving,
        Some(Condition::Leaving),
        "a node in staged shutdown should report itself leaving"
    );

    // The same node, at the same moment, on the other route. This is the half
    // that makes the test about divergence rather than about shutdown: if
    // `health` were `ready` under another name, this would read `Leaving` too.
    let health = node
        .operations()
        .health()
        .await
        .expect("a leaving node is still serving, so health must still answer");
    assert!(
        matches!(health, Condition::Ok { .. }),
        "a node that is leaving is not thereby unwell; got {health:?}"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_file_written_through_the_sdk_reads_back_byte_for_byte() {
    // U09 and U11 of the SGE inventory, together, because neither is observable
    // alone: a write that reported success and stored nothing looks exactly like
    // a write that worked until something reads it back.
    let node = HttpNode::start().await;
    let bucket = node.bucket().await;

    bucket
        .put("notes.txt", b"the node keeps what it is given")
        .await
        .expect("a bucket that exists should take a file");

    let read = bucket
        .get("notes.txt")
        .await
        .expect("reading a file just written should not fail");
    assert_eq!(
        read.as_deref(),
        Some(b"the node keeps what it is given".as_slice()),
        "the file should come back exactly as it went in"
    );

    // Bytes that are not text, because a transport that stringified the body
    // somewhere would pass the assertion above and fail this one. 0xFF and 0xFE
    // are not valid UTF-8 in any position.
    let raw: &[u8] = &[0x00, 0x01, 0xFF, 0xFE];
    bucket
        .put("raw.bin", raw)
        .await
        .expect("a file is bytes, not text");
    assert_eq!(
        bucket.get("raw.bin").await.expect("it was just written"),
        Some(raw.to_vec()),
        "bytes that are not UTF-8 must survive the round trip unchanged"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_batch_of_events_lands_whole_in_event_time_order_or_not_at_all() {
    // Protocol §5.9. The node is the oracle for the rendering: every spelling in
    // the batch below is read back through its own parser, and a value this
    // client spelled wrongly fails the batch or comes back different.
    let node = HttpNode::start().await;
    let mut client = Client::connect(&node.wire)
        .await
        .unwrap_or_else(|e| panic!("could not reach the wire port at {}: {e}", node.wire));
    client
        .run(
            "DEFINE NAMESPACE app; USE NAMESPACE app; DEFINE DATABASE main; USE DATABASE main; \
             DEFINE SERIES readings RETAIN 36500d TIME at;",
            None,
        )
        .await
        .expect("the node should accept a series");
    let series = node.operations().series("app", "main", "readings");
    let event = |second: i64, sensor: &str| {
        Value::Object(std::collections::BTreeMap::from([
            ("sensor".to_owned(), Value::String(sensor.to_owned())),
            ("v".to_owned(), Value::Number(Number::Float(0.5))),
            ("note".to_owned(), Value::String("it's \\ fine".to_owned())),
            (
                "at".to_owned(),
                Value::Datetime {
                    seconds: 1_790_676_000 + second,
                    nanos: 0,
                },
            ),
        ]))
    };

    let landed = series
        .append(&[event(2, "b"), event(1, "a")])
        .await
        .expect("a batch for a series should land");
    assert_eq!(landed, 2);

    // An event with no time: the series refuses it, and the whole batch with it.
    let refused = series
        .append(&[
            event(3, "c"),
            Value::Object(std::collections::BTreeMap::new()),
        ])
        .await
        .expect_err("an event-time series refuses an event with no time");
    assert!(
        matches!(refused, Error::HttpRefused { status: 400, .. }),
        "{refused:?}"
    );

    let answers = client
        .run("SELECT sensor, note FROM readings;", None)
        .await
        .expect("the read should be accepted");
    let held = records(&answers[0]);
    let sensors: Vec<&Value> = held
        .iter()
        .map(|(_, value)| field(value, "sensor"))
        .collect();
    assert_eq!(
        sensors,
        vec![
            &Value::String("a".to_owned()),
            &Value::String("b".to_owned())
        ],
        "event-time order, and nothing of the refused batch"
    );
    assert_eq!(
        field(&held[0].1, "note"),
        &Value::String("it's \\ fine".to_owned())
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn an_absent_file_and_an_empty_file_are_different_answers() {
    // The distinction the node draws and a client can lose: absent answers 404,
    // and empty answers 200 with a declared length of zero. A `get` that
    // returned an empty vector for both would be wrong in a way no assertion
    // about a written file could ever catch.
    let node = HttpNode::start().await;
    let bucket = node.bucket().await;

    assert_eq!(
        bucket
            .get("was-never-written.txt")
            .await
            .expect("a missing file is an answer, not a failure"),
        None,
        "a file that does not exist should read as nothing, and not as an error"
    );

    bucket
        .put("empty.bin", b"")
        .await
        .expect("a file may be empty");
    assert_eq!(
        bucket.get("empty.bin").await.expect("it exists now"),
        Some(Vec::new()),
        "an empty file exists, and must not read the same as one that does not"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_refusal_carries_the_status_and_not_the_json_that_wrapped_it() {
    // The protocol enumerates its refusals by status code and says a client
    // branches on the code, never on the sentence. So the status has to survive
    // as a number a caller can match on — and the sentence has to arrive as a
    // sentence rather than as the JSON object it travelled in.
    //
    // Reached through the public API rather than through a raw-path escape
    // hatch: a hyphen is outside the `[A-Za-z0-9_]+` the node allows in a name,
    // so this is a refusal a caller can actually provoke by mistake.
    let node = HttpNode::start().await;
    let bucket = node.operations().bucket("app", "main", "not-a-name");

    let refused = bucket
        .get("anything.txt")
        .await
        .expect_err("a bucket name the node will not accept must not succeed");

    let Error::HttpRefused {
        status,
        message,
        class,
    } = refused
    else {
        panic!("an HTTP refusal should say so and carry its status; got {refused:?}");
    };
    assert_eq!(
        status, 400,
        "the node answers 400 for a name it will not take"
    );
    assert_eq!(
        class,
        Some(tessaridb_client::RefusalClass::Invalid),
        "the body's code names the class"
    );
    assert!(
        !message.contains('{') && !message.contains("\"error\""),
        "the message should be the node's sentence, not the JSON around it; got {message:?}"
    );
    assert!(
        !message.is_empty(),
        "unwrapping the JSON must not throw the sentence away as well"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_file_name_that_needs_encoding_survives_the_round_trip() {
    // Three characters, each breaking differently if the client leaves it raw:
    // a space makes the request line unparseable, a percent asks the node to
    // decode an escape that is not one, and a slash is part of the name rather
    // than a directory and must reach the node as itself.
    let node = HttpNode::start().await;
    let bucket = node.bucket().await;

    let awkward = "holiday photos/100% done.txt";
    bucket
        .put(awkward, b"encoded and decoded")
        .await
        .expect("a file may be called anything at all");

    assert_eq!(
        bucket
            .get(awkward)
            .await
            .expect("the same name must reach the same file"),
        Some(b"encoded and decoded".to_vec()),
        "a name needing encoding must round-trip through it"
    );

    // The neighbouring name, to show the encoding is reversible rather than
    // merely consistent: if the client encoded a space as something the node
    // decodes to something else, both calls above would still agree with each
    // other while naming a file nobody asked for.
    assert_eq!(
        bucket
            .get("holiday photos/100%25 done.txt")
            .await
            .expect("this asks for a different file, which does not exist"),
        None,
        "a percent that was already an escape must not collide with one that was not"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn deleting_a_file_removes_it_and_deleting_it_again_is_still_fine() {
    // U15. The node answers 204 whether or not the file was there and reports no
    // difference between the two, so idempotence is what the client can honestly
    // promise — and a caller who has to guess will write the wrong retry.
    let node = HttpNode::start().await;
    let bucket = node.bucket().await;

    bucket
        .put("temporary.txt", b"here for now")
        .await
        .expect("the file should be written");
    assert!(
        bucket
            .get("temporary.txt")
            .await
            .expect("it exists")
            .is_some(),
        "the file must be there before deleting it proves anything"
    );

    bucket
        .delete("temporary.txt")
        .await
        .expect("deleting a file that is there should succeed");
    assert_eq!(
        bucket.get("temporary.txt").await.expect("asking is fine"),
        None,
        "the file should be gone"
    );

    bucket
        .delete("temporary.txt")
        .await
        .expect("deleting a file that is already gone should also succeed");
}

/// The initial user every closed-store test in this file signs in as.
///
/// A space in the password on purpose: it is the one character that would make a
/// credential survive a naive encoder and fail a correct one, so a header built
/// by concatenation rather than by base64 dies here rather than in production.
const OWNER: (&str, &str) = ("root", "s3cr3t pw");

/// A user who exists, whose password is right, and whose reach stops at one
/// database.
///
/// `ROLE owner` deliberately, so the only thing that can refuse this user is the
/// **scope**. A narrower role would refuse too, and the test would pass while
/// proving something else.
const SCOPED: (&str, &str) = ("narrow", "a narrow one");

impl HttpNode {
    /// Declare the fixture on a **closed** store, over the wire, as the owner.
    ///
    /// Separate from [`bucket`](Self::bucket) because the wire half needs the
    /// credential too: a closed store refuses an anonymous `DEFINE` exactly as
    /// firmly as it refuses an anonymous read, so a fixture written the open way
    /// fails before the test it was setting up ever runs.
    async fn closed_fixture(&self) {
        let mut client = Client::connect(&self.wire)
            .await
            .unwrap_or_else(|e| panic!("could not reach the wire port at {}: {e}", self.wire));
        // Interpolated from the constant rather than repeated as a literal, so
        // the user this declares and the user the test signs in as cannot drift
        // apart. Written into the script text only because both halves are
        // literals in this file — a credential a *caller* supplies belongs in a
        // parameter, which is the whole of what `run_with` is for.
        let script = format!(
            "DEFINE NAMESPACE app; USE NAMESPACE app; \
             DEFINE DATABASE main; USE DATABASE main; \
             DEFINE BUCKET files; \
             DEFINE USER {} ON app.main ROLE owner PASSWORD '{}';",
            SCOPED.0, SCOPED.1
        );
        client
            .run(&script, Some(OWNER))
            .await
            .expect("the owner of a closed store should be able to declare the fixture");
    }
}

/// The status a refusal carries, or a panic naming what arrived instead.
fn refusal_status(error: &Error) -> u16 {
    match error {
        Error::HttpRefused { status, .. } => *status,
        other => panic!("expected an HTTP refusal carrying a status; got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_closed_store_refuses_a_call_that_carries_no_credential() {
    // D2. The test has to run against a **closed** store, and that is the whole
    // design of this wave rather than a detail of it: on an open store every
    // call succeeds whether or not the header was sent, so a credential suite
    // written there passes with the feature deleted.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;

    let anonymous = node.operations().bucket("app", "main", "files");
    let refused = anonymous
        .get("greeting.txt")
        .await
        .expect_err("a closed store must not answer an uncredentialed read");

    assert_eq!(
        refusal_status(&refused),
        401,
        "an absent credential is 401 — the status that means *present one*"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn the_same_call_with_the_right_credential_is_answered() {
    // D3, and the falsification for D2: if the header were never sent, this test
    // would fail with the 401 the previous one asserts. The two are a pair, and
    // neither alone shows that anything was transmitted.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;

    let bucket = node
        .operations()
        .as_user(OWNER.0, OWNER.1)
        .bucket("app", "main", "files");

    bucket
        .put("greeting.txt", b"hello, closed store")
        .await
        .expect("the owner should be able to write");
    assert_eq!(
        bucket.get("greeting.txt").await.expect("and to read back"),
        Some(b"hello, closed store".to_vec()),
        "the bytes must survive the round trip through the credentialed path"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_wrong_password_and_a_scope_that_does_not_reach_are_told_apart() {
    // D4. The protocol says a client branches on the code, and these two are the
    // reason it matters: 401 is worth presenting a credential for, and 403 never
    // is — retrying it with a better password is a loop, not a recovery.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;

    let wrong = node
        .operations()
        .as_user(OWNER.0, "not the password")
        .bucket("app", "main", "files")
        .get("greeting.txt")
        .await
        .expect_err("a wrong password must not be answered");
    assert_eq!(
        refusal_status(&wrong),
        401,
        "a wrong password is the same status as no password: the node does not \
         say which of the two it was, and a client must not invent the difference"
    );

    // The same user, the same right password, reaching a database their grant
    // does not cover. Declared `ROLE owner` so the scope is the only thing left
    // that can refuse.
    let outside = node
        .operations()
        .as_user(SCOPED.0, SCOPED.1)
        .bucket("other", "main", "files")
        .get("greeting.txt")
        .await
        .expect_err("a user must not reach outside their own namespace");
    assert_eq!(
        refusal_status(&outside),
        403,
        "reaching outside a grant is 403 — a credential was accepted and is \
         still not enough, which is a different thing from not having one"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn the_refusal_status_follows_the_stores_posture_and_not_the_call() {
    // The measurement that makes the three tests above mandatory rather than
    // merely thorough. One call, named identically both times, answered with two
    // different statuses depending on something the caller cannot see from the
    // call site.
    //
    // On an **open** store the node looks the namespace up and reports that it
    // is not there. On a **closed** one it never gets that far: the credential
    // is checked first, so the same request is refused before the namespace is
    // ever a question. A suite written against an open store therefore pins the
    // wrong number and keeps passing after authentication stops working.
    let open = HttpNode::start().await;
    let on_open = open
        .operations()
        .bucket("nowhere", "main", "files")
        .get("greeting.txt")
        .await
        .expect_err("a namespace that does not exist must not be answered");

    let closed = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    let on_closed = closed
        .operations()
        .bucket("nowhere", "main", "files")
        .get("greeting.txt")
        .await
        .expect_err("a closed store must not answer an uncredentialed read");

    assert_ne!(
        refusal_status(&on_open),
        refusal_status(&on_closed),
        "the identical call must be refused differently on the two postures; \
         open answered {} and closed answered {}",
        refusal_status(&on_open),
        refusal_status(&on_closed)
    );
    assert_eq!(
        refusal_status(&on_closed),
        401,
        "the closed store refuses on the credential, before the lookup"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_files_size_comes_back_without_the_file() {
    // E1 and E3. Three lengths, chosen for where a length answer goes wrong: a
    // file of no bytes, a file of one, and one large enough that a client
    // reading the body instead of the header would be doing visibly different
    // work.
    //
    // E3 rides along and is the property that actually breaks. A `HEAD` answers
    // the `Content-Length` a `GET` would carry and then sends **nothing**, so
    // every assertion below is reached only because the reader takes
    // `expects_body` from the method rather than from the header.
    //
    // Falsified 2026-09-01 by forcing that argument true: both tests here fail
    // in milliseconds with `Error::Truncated` — *not* with the hang the design
    // note predicted. `Connection: close` is why; the node shuts the socket
    // after the headers, so the read ends rather than blocks. The prediction was
    // wrong in the client's favour and is corrected at `http/mod.rs::send`.
    let node = HttpNode::start().await;
    let bucket = node.bucket().await;

    let large = vec![b'x'; 8192];
    for (name, bytes) in [
        ("empty.bin", [].as_slice()),
        ("one.bin", b"!".as_slice()),
        ("large.bin", large.as_slice()),
    ] {
        bucket
            .put(name, bytes)
            .await
            .unwrap_or_else(|e| panic!("{name} should be written: {e}"));
        assert_eq!(
            bucket
                .size(name)
                .await
                .unwrap_or_else(|e| panic!("{name} should have a size: {e}")),
            Some(bytes.len() as u64),
            "the declared length must be the file's own, for {name}"
        );
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_file_with_no_bytes_and_no_file_at_all_have_different_sizes() {
    // E2. `get` already draws this distinction and a length-based answer is
    // exactly where it would be lost: zero is a plausible-looking stand-in for
    // "not there", and a caller who took it would delete or skip a file that
    // exists.
    let node = HttpNode::start().await;
    let bucket = node.bucket().await;

    assert_eq!(
        bucket
            .size("never-written.bin")
            .await
            .expect("asking is fine"),
        None,
        "a file that was never written has no size, and says so"
    );

    bucket
        .put("written-empty.bin", b"")
        .await
        .expect("an empty file should be writable");
    assert_eq!(
        bucket
            .size("written-empty.bin")
            .await
            .expect("and should have a size"),
        Some(0),
        "a file of no bytes is there and is zero long — not absent"
    );
}

/// Can this credential reach the fixture bucket on a closed store?
///
/// A read of a file that was never written, which is `Ok(None)` when the
/// credential is accepted and a refusal when it is not. Deliberately a *read*
/// and not a write: the question is whether the store took the credential, and a
/// write that succeeded would leave the next assertion looking at a store the
/// previous one changed.
async fn reaches(node: &HttpNode, name: &str, password: &str) -> Result<(), u16> {
    node.operations()
        .as_user(name, password)
        .bucket("app", "main", "files")
        .get("never-written.txt")
        .await
        .map(|_| ())
        .map_err(|error| refusal_status(&error))
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn changing_a_password_retires_the_old_one_and_keeps_the_handle_working() {
    // C1+C2+C3 against the shipped binary, and the three are asserted together
    // because no one of them carries the proof. That the call returns `Ok` says
    // nothing — a method that sent the request and updated nothing returns `Ok`
    // too, and so does one that updated the handle while the store refused. The
    // store's own before-and-after answer is the evidence.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;

    let mut handle = node.operations().as_user(OWNER.0, OWNER.1);
    assert!(
        reaches(&node, OWNER.0, OWNER.1).await.is_ok(),
        "the starting credential has to work, or the rest of this proves nothing"
    );

    handle
        .change_password("a longer one entirely")
        .await
        .expect("the owner should be able to change their own password");

    // C2 — the handle that made the call is still the handle that works. This
    // is the one that fails silently without `&mut self`: the change lands in
    // the store and the caller's handle keeps presenting a password the store
    // no longer holds.
    //
    // Asserted through a **credentialed** route on purpose. `metrics()` was the
    // obvious probe and is the wrong one: the node's `/metrics` handler takes no
    // credential at all, so it answers a stale handle exactly as happily as a
    // current one and this assertion would have held with the feature deleted.
    handle
        .bucket("app", "main", "files")
        .get("never-written.txt")
        .await
        .expect("the handle must still be usable by the caller who changed it");

    // C3 — and the store really moved, which `Ok` alone never said.
    assert_eq!(
        reaches(&node, OWNER.0, OWNER.1).await.unwrap_err(),
        401,
        "the old password must stop working, or nothing was changed"
    );
    assert!(
        reaches(&node, OWNER.0, "a longer one entirely")
            .await
            .is_ok(),
        "and the new one must work, or something was changed to nothing useful"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_wrong_current_password_changes_nothing_at_all() {
    // C5. The node re-verifies the current password before it changes anything,
    // and this pins that it does — a route that took the new password on the
    // strength of a name alone would pass every other test in this file.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;

    let refused = node
        .operations()
        .as_user(OWNER.0, "not the password")
        .change_password("nor is this")
        .await
        .expect_err("a wrong current password must not authorise a change");
    assert_eq!(
        refusal_status(&refused),
        401,
        "401 — present a credential — rather than 403, which would mean the \
         credential was right and the grants were not"
    );

    assert!(
        reaches(&node, OWNER.0, OWNER.1).await.is_ok(),
        "the real password must be untouched by a refused change"
    );
    assert_eq!(
        reaches(&node, OWNER.0, "nor is this").await.unwrap_err(),
        401,
        "and the password the refused call proposed must never have been set"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_backup_carries_the_store_and_the_incremental_carries_less_of_it() {
    // U05 and U06, and the pair is the point. Either test alone proves only that
    // a route answered: a full backup asserted non-empty would pass on a method
    // that always fetched everything, and an incremental asserted non-empty
    // would pass on one that ignored its query entirely. What no single call can
    // fake is the two answers **differing in the direction the sequence
    // predicts**, which is the semantics rather than the existence.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;
    let operations = node.operations().as_user(OWNER.0, OWNER.1);

    let mut whole = Vec::new();
    let written = operations
        .backup(&mut whole)
        .await
        .expect("the owner of a closed store may take a backup");

    assert!(
        !whole.is_empty(),
        "a store with a fixture in it is not empty"
    );
    assert_eq!(
        usize::try_from(written).expect("a fixture backup fits"),
        whole.len(),
        "the returned count is the bytes the caller actually received, not the \
         length the node declared"
    );

    // Far past anything this fixture committed, so the node has nothing after it
    // to send — but **not** `u64::MAX`, which is measured to come back `400`
    // *"18446744073709551615 … is not a number this store can hold"*. The route
    // parses the query as a `u64` and the language then refuses the statement it
    // was folded into, so the two disagree about the domain and the client sits
    // on the wider side of it.
    let mut since = Vec::new();
    operations
        .backup_from(1_000_000, &mut since)
        .await
        .expect("an incremental backup from beyond the end is still an answer");

    assert!(
        since.len() < whole.len(),
        "`from=` has to reach the node and change what it sends; got {} bytes \
         against {} for the whole store",
        since.len(),
        whole.len()
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_refused_backup_leaves_the_callers_sink_untouched() {
    // The criterion that fails silently rather than loudly. A refusal returns
    // `Err` whether or not its body was copied, so the error alone proves
    // nothing; the sink is the only observation that separates a clean refusal
    // from one that wrote `{"error":…}` into what the caller believes is a
    // backup file.
    //
    // On a **closed** store deliberately. On an open one this call succeeds
    // without a credential and hands back the whole store, which is the node's
    // decision and not this client's to override.
    let node = HttpNode::start_closed(OWNER.0, OWNER.1).await;
    node.closed_fixture().await;

    let mut sink = Vec::new();
    let refused = node
        .operations()
        .backup(&mut sink)
        .await
        .expect_err("a closed store must not hand a backup to an anonymous caller");

    assert_eq!(
        refusal_status(&refused),
        401,
        "an absent credential is 401 — the status that means *present one*"
    );
    assert!(
        sink.is_empty(),
        "the refusal body must not reach the sink; got {} bytes",
        sink.len()
    );
}

// --- the query corpus, against the parser it was written for ------------------
//
// `query_corpus.rs` proves this client renders what the contract says. It cannot
// prove the node accepts it, and nothing offline can: a rendering the parser
// stopped taking would keep that suite green all the way to a user.
//
// So the corpus runs here as well, and this is the half that reaches the parser.

/// Every table a non-refused corpus case reads or writes.
///
/// Derived from the corpus rather than hard-coded, so a case naming a new table
/// fails on a missing collection instead of being quietly skipped.
fn corpus_tables(cases: &[serde_json::Value]) -> Vec<String> {
    let mut named: Vec<String> = Vec::new();
    for case in cases {
        if case.get("refused").is_some() {
            continue;
        }
        let build = case["build"].as_object().expect("a build is an object");
        let (_, held) = build.iter().next().expect("one entry");
        let table = held
            .get("from")
            .or_else(|| held.get("table"))
            .and_then(serde_json::Value::as_str)
            .expect("every statement names a table");
        if !named.iter().any(|held| held == table) {
            named.push(table.to_owned());
        }
    }
    named
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn every_corpus_query_is_accepted_and_run_by_a_real_node() {
    let node = Node::start().await;
    let mut client = node.client().await;

    let document = read_corpus("queries-v1.json");
    let cases = document["cases"].as_array().expect("the corpus has cases");
    let tables = corpus_tables(cases);

    client
        .run("DEFINE NAMESPACE corpus; USE NAMESPACE corpus;", None)
        .await
        .expect("the namespace should be accepted");

    let mut ran = 0_usize;
    for (index, case) in cases.iter().enumerate() {
        if case.get("refused").is_some() {
            // A refusal never becomes text, so there is nothing to send. The
            // offline suite is where those are checked.
            continue;
        }
        let name = case["name"].as_str().expect("every case is named");

        // A fresh database per case: several cases write the same identity, and
        // ordering them so that they happen not to collide would make this suite
        // depend on the order the corpus lists them in.
        let collections = tables
            .iter()
            .map(|table| format!("DEFINE COLLECTION {table};"))
            .collect::<Vec<_>>()
            .join(" ");
        client
            .run(
                &format!("DEFINE DATABASE d{index}; USE DATABASE d{index}; {collections}"),
                None,
            )
            .await
            .unwrap_or_else(|why| panic!("case {name}: the setup was refused: {why}"));

        // Prerequisites first: an UPDATE needs the record it changes, and a
        // corpus that left that implicit would work only in the order it happens
        // to list its cases.
        for prerequisite in case
            .get("needs")
            .and_then(serde_json::Value::as_array)
            .unwrap_or(&Vec::new())
        {
            let first = build(&prerequisite["build"])
                .unwrap_or_else(|why| panic!("case {name}: a prerequisite would not build: {why}"));
            client
                .run_with(&first.script, None, first.parameters)
                .await
                .unwrap_or_else(|why| {
                    panic!(
                        "case {name}: a prerequisite was refused: {}\n{why}",
                        first.script
                    )
                });
        }

        // Built here rather than taken from the corpus's `script` field: what
        // needs proving is that *this client's* rendering is accepted, and
        // sending the corpus's own text would prove it about the corpus.
        let query = build(&case["build"])
            .unwrap_or_else(|why| panic!("case {name}: this builder refused a stated case: {why}"));
        assert_eq!(
            query.parameters,
            expected_parameters(&case["parameters"]),
            "case {name}: parameters differ from the corpus"
        );

        client
            .run_with(&query.script, None, query.parameters)
            .await
            .unwrap_or_else(|why| {
                panic!(
                    "case {name}: the node refused a statement this builder \
                     rendered.\n  {}\n{why}",
                    query.script
                )
            });

        ran = ran.saturating_add(1);
    }

    assert!(
        ran >= 15,
        "only {ran} cases reached the node — a corpus that shrank to nothing \
         would pass this suite silently"
    );
    println!("corpus against a node: {ran} statements accepted and run");
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn the_comparisons_a_parser_cannot_check_are_checked_by_running_them() {
    // Acceptance is not meaning. A builder that spelled `lt` as `>` would render
    // text the parser takes, the node would run it, and every record would come
    // back on the wrong side of the comparison with nothing in an error state.
    //
    // Six operators, one seeded collection, and the answer asserted per operator
    // — this is the only assertion in the crate that would notice.
    let node = Node::start().await;
    let mut client = node.client().await;

    client
        .run(
            "DEFINE NAMESPACE meaning; USE NAMESPACE meaning; \
             DEFINE DATABASE main; USE DATABASE main; DEFINE COLLECTION weights; \
             CREATE weights:1 = { weight: 1 }; CREATE weights:2 = { weight: 2 }; \
             CREATE weights:3 = { weight: 3 }; CREATE weights:4 = { weight: 4 }; \
             CREATE weights:5 = { weight: 5 };",
            None,
        )
        .await
        .expect("the seed should be accepted");

    // Each row: the builder call, and the identities that must come back.
    let expectations: Vec<(&str, Vec<&str>)> = vec![
        ("eq", vec!["3"]),
        ("ne", vec!["1", "2", "4", "5"]),
        ("lt", vec!["1", "2"]),
        ("le", vec!["1", "2", "3"]),
        ("gt", vec!["4", "5"]),
        ("ge", vec!["3", "4", "5"]),
    ];

    for (operator, expected) in expectations {
        let condition = on_field("weight");
        let condition = match operator {
            "eq" => condition.eq(3_i64),
            "ne" => condition.ne(3_i64),
            "lt" => condition.lt(3_i64),
            "le" => condition.le(3_i64),
            "gt" => condition.gt(3_i64),
            "ge" => condition.ge(3_i64),
            other => panic!("no such operator: {other}"),
        };
        let query = Select::from("weights")
            .filter(condition)
            .order_by("weight", Order::Ascending)
            .build()
            .expect("a well-formed query");

        let answers = client
            .run_with(&query.script, None, query.parameters)
            .await
            .unwrap_or_else(|why| panic!("{operator}: {} was refused: {why}", query.script));

        let mut found: Vec<&str> = records(&answers[0])
            .iter()
            .map(|(id, _)| id.as_str())
            .collect();
        found.sort_unstable();

        assert_eq!(
            found, expected,
            "{operator}: `{}` selected the wrong records — the text parsed and \
             meant something else",
            query.script
        );
    }

    println!("comparison semantics: six operators, each answer asserted");
}
