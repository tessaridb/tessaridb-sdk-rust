//! Following a redirect (protocol §3.12), against scripted nodes on loopback.
//!
//! A socket rather than a duplex, because following means dialling a second
//! node: each fake below listens on its own port, greets, keeps its own
//! session's `USE`, answers `session::context()` as a node does, and hands
//! every other script to the test's closure.

#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tessaridb_client::codec::encode;
use tessaridb_client::wire::frame::{self, Kind};
use tessaridb_client::{Answer, Client, Error, Number, Redirect, Settlement, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What a fake answers a script with.
enum Reply {
    /// One value outcome.
    Value(Value),
    /// Go there instead.
    Elsewhere {
        node: [u8; 16],
        epoch: u64,
        settled: bool,
        endpoint: String,
    },
}

type Behaviour = Arc<dyn Fn(&str) -> Reply + Send + Sync>;

/// What a fake was sent, shared with the test reading it.
type Log = Arc<Mutex<Vec<String>>>;

/// A node that answers on loopback, and the scripts it was sent.
struct Fake {
    address: String,
    node: [u8; 16],
    seen: Log,
    signed: Log,
}

impl Fake {
    /// Listen as `node`, answering every script other than `USE` and
    /// `session::context()` with `behave`. `claims` overrides the node the
    /// fake says it is — the wrong-node case.
    async fn start(node: [u8; 16], claims: Option<[u8; 16]>, behave: Behaviour) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let signed = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let names = Arc::clone(&signed);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let behave = Arc::clone(&behave);
                let log = (Arc::clone(&log), Arc::clone(&names));
                tokio::spawn(serve(stream, claims.unwrap_or(node), behave, log));
            }
        });
        Self {
            address,
            node,
            seen,
            signed,
        }
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

async fn serve(mut stream: TcpStream, node: [u8; 16], behave: Behaviour, (log, names): (Log, Log)) {
    let mut greeting = [0_u8; 6];
    stream.read_exact(&mut greeting).await.unwrap();
    stream.write_all(b"TESS\x01\x02").await.unwrap();
    let mut namespace = Value::Null;
    let mut database = Value::Null;
    while let Ok(Some((kind, body))) = frame::read(&mut stream).await {
        assert_eq!(kind, Kind::Request);
        let script = script_of(&body);
        log.lock().unwrap().push(script.clone());
        if let Some(name) = signed_as(&body) {
            names.lock().unwrap().push(name);
        }
        let reply = if script == "RETURN session::context();" {
            Reply::Value(Value::Object(BTreeMap::from([
                ("node".to_owned(), Value::Uuid(node)),
                ("namespace".to_owned(), namespace.clone()),
                ("database".to_owned(), database.clone()),
            ])))
        } else if script.starts_with("USE ") {
            for statement in script.split(';') {
                let words: Vec<&str> = statement.split_whitespace().collect();
                match words.as_slice() {
                    ["USE", "NAMESPACE", name] => namespace = Value::String((*name).to_owned()),
                    ["USE", "DATABASE", name] => database = Value::String((*name).to_owned()),
                    _ => {}
                }
            }
            Reply::Value(Value::Null)
        } else {
            behave(&script)
        };
        let (kind, body) = match reply {
            Reply::Value(value) => (Kind::Answer, value_answer(&value)),
            Reply::Elsewhere {
                node,
                epoch,
                settled,
                endpoint,
            } => (
                Kind::Elsewhere,
                elsewhere_body(node, epoch, settled, &endpoint),
            ),
        };
        frame::write(&mut stream, kind, &body).await.unwrap();
    }
}

/// The script, which a request body opens with.
fn script_of(body: &[u8]) -> String {
    let length = u32::from_be_bytes(body[..4].try_into().unwrap());
    let end = usize::try_from(length).unwrap().checked_add(4).unwrap();
    String::from_utf8(body[4..end].to_vec()).unwrap()
}

/// The user a request signed in as, which follows the script.
fn signed_as(body: &[u8]) -> Option<String> {
    let length = usize::try_from(u32::from_be_bytes(body[..4].try_into().unwrap())).unwrap();
    let at = length.checked_add(4).unwrap();
    (body[at] == 1).then(|| script_of(&body[at.checked_add(1).unwrap()..]))
}

/// An answer frame holding one value outcome (protocol §3.5).
fn value_answer(value: &Value) -> Vec<u8> {
    let encoded = encode(value);
    let mut outcome = vec![2_u8];
    outcome.extend_from_slice(&0_u32.to_be_bytes());
    outcome.extend_from_slice(&u32::try_from(encoded.len()).unwrap().to_be_bytes());
    outcome.extend_from_slice(&encoded);
    let mut body = 1_u32.to_be_bytes().to_vec();
    body.extend_from_slice(&u32::try_from(outcome.len()).unwrap().to_be_bytes());
    body.extend_from_slice(&outcome);
    body
}

/// An `Elsewhere` body (protocol §3.12).
fn elsewhere_body(node: [u8; 16], epoch: u64, settled: bool, endpoint: &str) -> Vec<u8> {
    let mut body = node.to_vec();
    body.extend_from_slice(&epoch.to_be_bytes());
    body.push(if settled { 1 } else { 2 });
    body.extend_from_slice(&u32::try_from(endpoint.len()).unwrap().to_be_bytes());
    body.extend_from_slice(endpoint.as_bytes());
    body
}

const A: [u8; 16] = [0xa; 16];
const B: [u8; 16] = [0xb; 16];
const C: [u8; 16] = [0xc; 16];
const READ: &str = "SELECT * FROM ledger;";

fn answers(value: i64) -> Behaviour {
    Arc::new(move |_| Reply::Value(Value::Number(Number::Integer(value))))
}

/// [`READ`] goes to `to`; anything else is answered here.
fn sends(to: &Fake, epoch: u64, settled: bool) -> Behaviour {
    let (node, endpoint) = (to.node, to.address.clone());
    Arc::new(move |script| {
        if script == READ {
            Reply::Elsewhere {
                node,
                epoch,
                settled,
                endpoint: endpoint.clone(),
            }
        } else {
            Reply::Value(Value::Number(Number::Integer(1)))
        }
    })
}

fn the_value(answers: &[Answer]) -> &Value {
    match answers.last() {
        Some(Answer::Value { value, .. }) => value,
        other => panic!("expected a value, got {other:?}"),
    }
}

/// A client on `origin` that selected `prod` / `shop` there.
async fn selected(origin: &Fake) -> Client {
    let mut client = Client::connect(&origin.address).await.unwrap();
    client
        .run(
            "USE NAMESPACE prod; USE DATABASE shop;",
            Some(("ada", "secret")),
        )
        .await
        .unwrap();
    client
}

#[tokio::test]
async fn a_transient_redirect_answers_there_and_leaves_the_client_here() {
    let b = Fake::start(B, None, answers(42)).await;
    let a = Fake::start(A, None, sends(&b, 7, false)).await;
    let mut client = selected(&a).await;
    let answered = client.run(READ, None).await.unwrap();
    assert_eq!(the_value(&answered), &Value::Number(Number::Integer(42)));
    // The tenancy was selected on B before the request went there.
    assert_eq!(
        b.seen(),
        [
            "RETURN session::context();",
            "USE NAMESPACE prod; USE DATABASE shop; ",
            READ
        ]
    );
    // The credentials presented on A once were presented on B for it.
    assert_eq!(
        b.signed.lock().unwrap().first().map(String::as_str),
        Some("ada")
    );
    // The next request is A's again.
    client.run("RETURN 1;", None).await.unwrap();
    assert_eq!(a.seen().last().map(String::as_str), Some("RETURN 1;"));
    assert_eq!(b.seen().len(), 3, "B was not asked again");
}

#[tokio::test]
async fn a_settled_redirect_moves_the_client_there() {
    let b = Fake::start(B, None, answers(42)).await;
    let a = Fake::start(A, None, sends(&b, 7, true)).await;
    let mut client = selected(&a).await;
    client.run(READ, None).await.unwrap();
    client.run("RETURN 1;", None).await.unwrap();
    assert_eq!(b.seen().last().map(String::as_str), Some("RETURN 1;"));
    assert!(!a.seen().contains(&"RETURN 1;".to_owned()), "A was left");
}

#[tokio::test]
async fn a_node_other_than_the_one_named_is_not_sent_the_request() {
    let b = Fake::start(B, Some(C), answers(42)).await;
    let a = Fake::start(A, None, sends(&b, 7, false)).await;
    let mut client = selected(&a).await;
    match client.run(READ, None).await {
        Err(Error::WrongNode { expected }) => assert_eq!(expected, B),
        other => panic!("expected WrongNode, got {other:?}"),
    }
    assert!(!b.seen().contains(&READ.to_owned()));
}

#[tokio::test]
async fn a_redirect_dated_before_one_already_followed_is_refused() {
    let c = Fake::start(C, None, answers(42)).await;
    let b = Fake::start(B, None, sends(&c, 3, false)).await;
    let a = Fake::start(A, None, sends(&b, 5, false)).await;
    let mut client = selected(&a).await;
    match client.run(READ, None).await {
        Err(Error::StaleRedirect { epoch, floor }) => assert_eq!((epoch, floor), (3, 5)),
        other => panic!("expected StaleRedirect, got {other:?}"),
    }
    assert!(c.seen().is_empty(), "C was never dialled");
}

#[tokio::test]
async fn three_hops_and_no_answer_is_a_loop() {
    // C sends every request to itself, so following never ends by itself.
    let round = Arc::new(Mutex::new(String::new()));
    let address = Arc::clone(&round);
    let c = Fake::start(
        C,
        None,
        Arc::new(move |_| Reply::Elsewhere {
            node: C,
            epoch: 1,
            settled: false,
            endpoint: address.lock().unwrap().clone(),
        }),
    )
    .await;
    round.lock().unwrap().clone_from(&c.address);
    let a = Fake::start(A, None, sends(&c, 1, false)).await;
    let mut client = selected(&a).await;
    match client.run(READ, None).await {
        Err(Error::RedirectLoop { hops }) => assert_eq!(hops, 3),
        other => panic!("expected RedirectLoop, got {other:?}"),
    }
    let sent = c.seen().iter().filter(|script| *script == READ).count();
    assert_eq!(sent, 3, "the request was sent three times and no more");
}

#[tokio::test]
async fn a_tenancy_that_is_not_a_plain_name_is_not_followed() {
    let b = Fake::start(B, None, answers(42)).await;
    let a = Fake::start(A, None, sends(&b, 7, false)).await;
    let mut client = Client::connect(&a.address).await.unwrap();
    client.run("USE NAMESPACE pr-od;", None).await.unwrap();
    match client.run(READ, None).await {
        Err(Error::NotFollowable { name }) => assert_eq!(name, "pr-od"),
        other => panic!("expected NotFollowable, got {other:?}"),
    }
    assert!(b.seen().is_empty(), "B was never dialled");
}

#[tokio::test]
async fn a_client_handed_its_stream_returns_the_redirect() {
    let b = Fake::start(B, None, answers(42)).await;
    let a = Fake::start(A, None, sends(&b, 7, true)).await;
    let mut stream = TcpStream::connect(&a.address).await.unwrap();
    frame::greet(&mut stream).await.unwrap();
    let mut client = Client::with_stream(stream);
    match client.run(READ, None).await {
        Err(Error::Redirected(Redirect {
            node,
            epoch,
            settlement,
            endpoint,
        })) => {
            assert_eq!((node, epoch, settlement), (B, 7, Settlement::Settled));
            assert_eq!(endpoint, b.address);
        }
        other => panic!("expected Redirected, got {other:?}"),
    }
}

#[test]
fn a_settlement_byte_that_is_neither_one_nor_two_is_malformed() {
    let mut body = elsewhere_body(B, 7, true, "there:1");
    body[24] = 0;
    assert!(matches!(Redirect::decode(&body), Err(Error::Malformed)));
    body[24] = 2;
    assert_eq!(
        Redirect::decode(&body).unwrap(),
        Redirect {
            node: B,
            epoch: 7,
            settlement: Settlement::Transient,
            endpoint: "there:1".to_owned(),
        }
    );
}
