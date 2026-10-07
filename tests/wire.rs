//! The framing, the greeting, and one conversation end to end.
//!
//! Driven over an in-memory duplex rather than a socket: this tier is about the
//! bytes this client writes and the bytes it accepts, and a loopback TCP
//! connection would add scheduling without adding evidence. Exercising a running
//! node is the stronger tier and is owed separately.

// Test assertions are exactly where a panic is the correct outcome; the lints
// these turn off target production paths, where a panic is a defect.
#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]
use tessaridb_client::protocol::{CEILING, GREETING, MAJOR, MINOR};
use tessaridb_client::wire::frame::{self, Kind};
use tessaridb_client::wire::message::decode_answers;
use tessaridb_client::{
    Answer, Client, Correction, Error, Exact, Number, Request, Suggested, Value,
};
use tokio::io::AsyncWriteExt;

/// One outcome, behind the `u32` length the protocol puts in front of it.
///
/// Written as a helper rather than inline in each test because the length is
/// exactly what a hand-built body forgets, and forgetting it would make every
/// test below fail in the same confusing place.
fn outcome(tagged: &[u8]) -> Vec<u8> {
    let mut out = u32::try_from(tagged.len())
        .expect("a test outcome is small")
        .to_be_bytes()
        .to_vec();
    out.extend_from_slice(tagged);
    out
}

/// An answer frame body carrying these outcomes, in order.
fn answer_body(outcomes: &[Vec<u8>]) -> Vec<u8> {
    let count = u32::try_from(outcomes.len()).expect("a test answer is small");
    let mut body = count.to_be_bytes().to_vec();
    for one in outcomes {
        body.extend_from_slice(one);
    }
    body
}

#[tokio::test]
async fn a_frame_round_trips_through_the_wire() {
    let (mut a, mut b) = tokio::io::duplex(4096);
    frame::write(&mut a, Kind::Request, b"hello")
        .await
        .expect("a small frame should write");
    let (kind, body) = frame::read(&mut b)
        .await
        .expect("a written frame should read")
        .expect("and should not be a clean goodbye");
    assert_eq!(kind, Kind::Request);
    assert_eq!(body, b"hello");
}

#[tokio::test]
async fn a_clean_hangup_between_frames_is_not_an_error() {
    let (a, mut b) = tokio::io::duplex(64);
    drop(a);
    let read = frame::read(&mut b)
        .await
        .expect("a clean close is not an error");
    assert!(read.is_none(), "nothing at all is a goodbye, not a failure");
}

#[tokio::test]
async fn a_partial_header_is_truncation_and_not_a_goodbye() {
    let (mut a, mut b) = tokio::io::duplex(64);
    a.write_all(&[Kind::Request.tag(), 0, 0])
        .await
        .expect("write");
    drop(a);
    let error = frame::read(&mut b)
        .await
        .expect_err("a partial header must fail");
    assert!(matches!(error, Error::Truncated), "got {error:?}");
}

#[tokio::test]
async fn a_declared_length_above_the_ceiling_is_refused_before_reading_a_body() {
    let (mut a, mut b) = tokio::io::duplex(64);
    let mut header = vec![Kind::Answer.tag()];
    header.extend_from_slice(&CEILING.saturating_add(1).to_be_bytes());
    a.write_all(&header).await.expect("write");
    // Deliberately no body follows. If the reader allocated on the claim and
    // then waited for bytes, this test would hang rather than fail.
    let error = frame::read(&mut b)
        .await
        .expect_err("an oversized claim must be refused");
    match error {
        Error::TooLarge { length } => assert_eq!(length, CEILING.saturating_add(1)),
        other => panic!("expected TooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unknown_frame_kind_ends_the_connection_rather_than_being_skipped() {
    let (mut a, mut b) = tokio::io::duplex(64);
    a.write_all(&[99, 0, 0, 0, 0]).await.expect("write");
    let error = frame::read(&mut b)
        .await
        .expect_err("an unknown kind must not be skipped");
    match error {
        Error::UnknownFrame { tag } => assert_eq!(tag, 99),
        other => panic!("expected UnknownFrame, got {other:?}"),
    }
}

#[tokio::test]
async fn a_greeting_from_something_that_is_not_a_node_is_refused() {
    let (mut ours, mut theirs) = tokio::io::duplex(64);
    tokio::spawn(async move {
        theirs.write_all(b"HTTP/").await.ok();
    });
    let error = frame::greet(&mut ours)
        .await
        .expect_err("a foreign greeting must be refused");
    assert!(matches!(error, Error::NotThisProtocol), "got {error:?}");
}

#[tokio::test]
async fn a_node_of_another_major_is_refused_at_the_greeting() {
    let (mut ours, mut theirs) = tokio::io::duplex(64);
    tokio::spawn(async move {
        let mut said = GREETING.to_vec();
        said.push(MAJOR.saturating_add(1));
        said.push(MINOR);
        theirs.write_all(&said).await.ok();
    });
    let error = frame::greet(&mut ours)
        .await
        .expect_err("a major mismatch must be refused here, not later");
    match error {
        Error::WrongVersion { found, supported } => {
            assert_eq!(found, MAJOR.saturating_add(1));
            assert_eq!(supported, MAJOR);
        }
        other => panic!("expected WrongVersion, got {other:?}"),
    }
}

#[tokio::test]
async fn a_node_of_a_later_minor_is_accepted_and_its_minor_reported() {
    // The half of the version rule that is easy to get wrong in the safe-looking
    // direction: refusing a differing minor would compile, pass every other test
    // here, and quietly make every upgrade a breaking one.
    let (mut ours, mut theirs) = tokio::io::duplex(64);
    tokio::spawn(async move {
        let mut said = GREETING.to_vec();
        said.push(MAJOR);
        said.push(MINOR.saturating_add(9));
        theirs.write_all(&said).await.ok();
    });
    let heard = frame::greet(&mut ours)
        .await
        .expect("a differing minor is not a refusal");
    assert_eq!(
        heard,
        MINOR.saturating_add(9),
        "the peer's minor is what decides what we may send it"
    );
}

#[tokio::test]
async fn a_script_is_sent_and_its_answers_read_back() {
    let (ours, mut theirs) = tokio::io::duplex(4096);

    let node = tokio::spawn(async move {
        // Read what the client sent, so the test proves the request reached the
        // wire rather than only that a reply could be parsed.
        let (kind, body) = frame::read(&mut theirs)
            .await
            .expect("the client should send a frame")
            .expect("and it should not be a goodbye");
        assert_eq!(kind, Kind::Request);

        // Two outcomes: Done, then Removed(7).
        let mut removed = vec![4_u8];
        removed.extend_from_slice(&7_u64.to_be_bytes());
        let outcomes = [outcome(&[0_u8]), outcome(&removed)];
        frame::write(&mut theirs, Kind::Answer, &answer_body(&outcomes))
            .await
            .expect("the node should answer");
        body
    });

    let mut client = Client::with_stream(ours);
    let answers = client
        .run("DELETE user WHERE age < $limit;", None)
        .await
        .expect("the exchange should succeed");
    assert_eq!(answers, vec![Answer::Done, Answer::Removed(7)]);

    let sent = node.await.expect("the node task should finish");
    let text = String::from_utf8_lossy(&sent);
    assert!(
        text.contains("DELETE user WHERE age < $limit;"),
        "the script should reach the wire verbatim"
    );
}

#[tokio::test]
async fn a_bound_parameter_travels_as_bytes_and_never_as_script_text() {
    // The value is a string that spells a statement. If it were interpolated
    // into the script this test would find it there.
    let hostile = "'; DROP TABLE users; --";
    let request = Request::new("SELECT * FROM users WHERE name = $name;").bind("name", hostile);
    let body = request.encode();
    let text = String::from_utf8_lossy(&body);

    assert!(
        text.contains("SELECT * FROM users WHERE name = $name;"),
        "the script itself should be present"
    );
    // The parameter's bytes are in the frame — that is where a value belongs —
    // but the script portion must not have grown to contain it. The script is
    // length-prefixed and first, so its own bytes end before the parameters.
    let header = body.get(..4).expect("the body opens with a length");
    let script_len =
        u32::from_be_bytes(<[u8; 4]>::try_from(header).expect("four bytes are four bytes"));
    let script_len = usize::try_from(script_len).expect("a length fits a usize here");
    let script = body
        .get(4..4_usize.saturating_add(script_len))
        .map(String::from_utf8_lossy)
        .expect("the script occupies the length it declared");
    assert!(
        !script.contains("DROP TABLE"),
        "a bound value must not reach the script text; script was {script:?}"
    );
}

#[tokio::test]
async fn a_refusal_carries_the_nodes_own_words() {
    let (ours, mut theirs) = tokio::io::duplex(1024);
    tokio::spawn(async move {
        let _ = frame::read(&mut theirs).await;
        frame::write(&mut theirs, Kind::Refusal, b"no such table: widgets")
            .await
            .ok();
    });
    let mut client = Client::with_stream(ours);
    let error = client
        .run("SELECT * FROM widgets;", None)
        .await
        .expect_err("a refusal should surface");
    match error {
        Error::Refused { message, .. } => assert_eq!(message, "no such table: widgets"),
        other => panic!("expected Refused, got {other:?}"),
    }
}

#[test]
fn an_unknown_outcome_is_stepped_over_and_the_next_one_still_reads() {
    // The unknown outcome sits in the MIDDLE, and its payload is deliberately a
    // well-formed `Removed(99)`. A decoder that ignored the length would read
    // that payload as the next outcome's tag and hand back an invented answer;
    // one that stopped at the unknown would silently lose the Done after it.
    let mut strange = vec![0xEE_u8];
    strange.extend_from_slice(&99_u64.to_be_bytes());
    let outcomes = [outcome(&[0_u8]), outcome(&strange), outcome(&[0_u8])];

    let answers = decode_answers(&answer_body(&outcomes)).expect("this should decode");
    assert_eq!(
        answers,
        vec![Answer::Done, Answer::Unknown, Answer::Done],
        "an unknown outcome is skipped by its length, not stopped at"
    );
}

#[test]
fn trailing_bytes_inside_a_known_outcome_are_skipped_rather_than_refused() {
    // What a later minor adding a field to an existing outcome kind looks like
    // to this build. Refusing here would make such an addition breaking, which
    // is the opposite of what a minor bump promises.
    let mut extended = vec![4_u8];
    extended.extend_from_slice(&7_u64.to_be_bytes());
    extended.extend_from_slice(b"a field this build has never heard of");
    let outcomes = [outcome(&extended), outcome(&[0_u8])];

    let answers = decode_answers(&answer_body(&outcomes)).expect("this should decode");
    assert_eq!(answers, vec![Answer::Removed(7), Answer::Done]);
}

#[test]
fn an_outcome_that_claims_more_than_its_length_allows_is_malformed() {
    // The bound works in the other direction too: a Removed needs eight bytes
    // after its tag, and this one is given four. Without the bound it would read
    // into the outcome that follows and answer with a plausible wrong number.
    let mut short = vec![4_u8];
    short.extend_from_slice(&[0, 0, 0, 1]);
    let outcomes = [outcome(&short), outcome(&[0_u8])];

    let error = decode_answers(&answer_body(&outcomes))
        .expect_err("a body larger than its length must not borrow from the next outcome");
    assert!(matches!(error, Error::Malformed), "got {error:?}");
}

#[test]
fn a_value_outcome_carries_the_names_its_references_need() {
    let mut tagged = vec![2_u8];
    // one name: table 3 is "users"
    tagged.extend_from_slice(&1_u32.to_be_bytes());
    tagged.extend_from_slice(&3_u32.to_be_bytes());
    tagged.extend_from_slice(&5_u32.to_be_bytes());
    tagged.extend_from_slice(b"users");
    // the value: integer 1
    let value = tessaridb_client::codec::encode(&Value::Number(Number::Integer(1)));
    let value_len = u32::try_from(value.len()).expect("the test value is small");
    tagged.extend_from_slice(&value_len.to_be_bytes());
    tagged.extend_from_slice(&value);

    let answers = decode_answers(&answer_body(&[outcome(&tagged)])).expect("this should decode");
    match answers.first() {
        Some(Answer::Value { value, names }) => {
            assert_eq!(value, &Value::Number(Number::Integer(1)));
            assert_eq!(names.get(&3).map(String::as_str), Some("users"));
        }
        other => panic!("expected a Value outcome, got {other:?}"),
    }
}

/// A `text` field as the protocol writes it: a `u32` length then the bytes.
fn text(word: &str) -> Vec<u8> {
    let mut out = u32::try_from(word.len())
        .expect("a test string is small")
        .to_be_bytes()
        .to_vec();
    out.extend_from_slice(word.as_bytes());
    out
}

/// A Records outcome carrying no records and no table names, with `tail`
/// appended where the later fields go.
///
/// Empty on purpose: the fields this exercises sit *after* the records, so a
/// record in the way would only add a value payload to hand-build.
fn records_outcome(tail: &[u8]) -> Vec<u8> {
    let mut body = vec![1_u8, 2_u8]; // tag Records, access path `scan`
    body.extend_from_slice(&0_u32.to_be_bytes()); // no table names
    body.extend_from_slice(&0_u32.to_be_bytes()); // no records
    body.extend_from_slice(tail);
    outcome(&body)
}

#[test]
fn a_records_outcome_carries_what_the_read_volunteered() {
    let mut tail = 2_u32.to_be_bytes().to_vec();
    tail.extend_from_slice(&text("fell-back"));
    tail.extend_from_slice(&text("an index could have served this"));
    tail.extend_from_slice(&text("cursor-walked"));
    tail.extend_from_slice(&text("the anchor was reached by walking"));
    tail.push(1); // ONLY was written

    let answers = decode_answers(&answer_body(&[records_outcome(&tail)])).expect("this decodes");
    let [Answer::Records { notes, only, .. }] = answers.as_slice() else {
        panic!("expected one Records outcome, got {answers:?}");
    };
    assert!(*only);
    assert_eq!(
        notes
            .iter()
            .map(|note| note.kind.as_str())
            .collect::<Vec<_>>(),
        ["fell-back", "cursor-walked"],
    );
    assert_eq!(notes[0].message, "an index could have served this");
}

#[test]
fn a_records_outcome_from_a_node_that_predates_these_fields_reads_as_silence() {
    // The frame grows by appending, so an older node's body simply ends after
    // the records. Read as a truncation this would be an error; read correctly
    // it is a node with nothing to add, which is what every read was before
    // these fields existed.
    let answers = decode_answers(&answer_body(&[records_outcome(&[])])).expect("this decodes");
    let [
        Answer::Records {
            notes,
            only,
            exact,
            suggestion,
            ..
        },
    ] = answers.as_slice()
    else {
        panic!("expected one Records outcome, got {answers:?}");
    };
    assert_eq!(notes.as_slice(), []);
    assert!(!*only);
    // And the field where that rule stops. An empty note list and a node that
    // predates notes are the same claim, so they are not told apart. Exactness
    // is not like that: this node did not serve exact answers and forget to
    // mention it, it said nothing, and `Some(Exact::Yes)` here would be this
    // client inventing a promise. §3.5 requires the three states to stay apart.
    assert!(
        exact.is_none(),
        "silence from an older node was read as a claim of exactness",
    );
    // The suggestion is silent for a third reason again. It is not a promise
    // this client would be inventing — a node without the field walked no
    // dictionary, so `NotSought` would even be true of it — but `NothingNearer`
    // would not, and that is the value a decoder reaching for a default lands on
    // when it types this as a list.
    assert!(
        suggestion.is_none(),
        "silence from an older node was read as a suggestion",
    );
}

/// The three states a node can send, told apart.
///
/// The pair this exists for is `NotSought` and `NothingNearer`. They render
/// identically to a caller that only looks at the corrections, and they are
/// different facts: one node consulted a dictionary and found every term held,
/// the other consulted none. A client that collapses them reports a negative the
/// node never checked, on every read of a field with no search index — which is
/// nearly every read.
#[test]
fn the_three_suggestion_states_stay_three() {
    let said = |state: &[u8]| {
        let mut tail = 0_u32.to_be_bytes().to_vec(); // no notes
        tail.push(0); // not an ONLY read
        tail.push(0); // exact
        tail.extend_from_slice(&text("")); // with no reason to give
        tail.extend_from_slice(state);
        let answers =
            decode_answers(&answer_body(&[records_outcome(&tail)])).expect("this decodes");
        let [Answer::Records { suggestion, .. }] = answers.as_slice() else {
            panic!("expected one Records outcome, got {answers:?}");
        };
        suggestion.clone()
    };

    assert_eq!(said(&[0]), Some(Suggested::NotSought));
    assert_eq!(said(&[1]), Some(Suggested::NothingNearer));
    assert_ne!(
        said(&[0]),
        said(&[1]),
        "a dictionary nobody asked read the same as one that found everything held",
    );

    let mut corrections = vec![2_u8];
    corrections.extend_from_slice(&1_u32.to_be_bytes());
    corrections.extend_from_slice(&text("vecter"));
    corrections.extend_from_slice(&text("vector"));
    let Some(Suggested::DidYouMean(read)) = said(&corrections) else {
        panic!("a correction read as no correction");
    };
    assert_eq!(
        read,
        vec![Correction {
            typed: "vecter".to_owned(),
            instead: "vector".to_owned(),
        }],
    );
}

/// A state byte this build has no name for reads as silence, not as an error.
///
/// The same contract §3.5 states for an unrecognised outcome tag and an
/// unrecognised access path, one field further in: a newer node saying something
/// in a vocabulary this build lacks is not a malformed answer, and refusing the
/// whole outcome over it would make every future addition to this field a
/// breaking change for every client already deployed.
#[test]
fn an_unknown_suggestion_state_reads_as_silence_rather_than_a_refusal() {
    let mut tail = 0_u32.to_be_bytes().to_vec();
    tail.push(0);
    tail.push(0);
    tail.extend_from_slice(&text(""));
    tail.push(9); // a state from a node newer than this build
    tail.extend_from_slice(b"whatever followed it");

    let answers = decode_answers(&answer_body(&[records_outcome(&tail)]))
        .expect("an unknown state is not a decode failure");
    let [Answer::Records { suggestion, .. }] = answers.as_slice() else {
        panic!("expected one Records outcome, got {answers:?}");
    };
    assert!(
        suggestion.is_none(),
        "an unfamiliar state was guessed at instead of read as silence",
    );
}

#[test]
fn an_exactness_byte_is_read_and_the_reason_travels_with_it() {
    let said = |approximate: bool, reason: &str| {
        let mut tail = 0_u32.to_be_bytes().to_vec(); // no notes
        tail.push(0); // not an ONLY read
        tail.push(u8::from(approximate));
        tail.extend_from_slice(&text(reason));
        let answers =
            decode_answers(&answer_body(&[records_outcome(&tail)])).expect("this decodes");
        let [Answer::Records { exact, .. }] = answers.as_slice() else {
            panic!("expected one Records outcome, got {answers:?}");
        };
        exact.clone()
    };

    assert_eq!(said(false, ""), Some(Exact::Yes));
    let Some(Exact::No { reason }) = said(true, "a nearer record may exist") else {
        panic!("an approximate answer read as exact");
    };
    // The node's words, not this client's. A reason derived on this side would
    // describe a read this build did not perform.
    assert_eq!(reason, "a nearer record may exist");
}

#[test]
fn notes_without_the_flag_behind_them_still_read_and_the_flag_is_false() {
    // The half-step between the two nodes above: notes shipped one release
    // before the flag did, so this is a real body and not a hypothetical one.
    let mut tail = 1_u32.to_be_bytes().to_vec();
    tail.extend_from_slice(&text("approximate"));
    tail.extend_from_slice(&text("a record that belonged may be missing"));

    let answers = decode_answers(&answer_body(&[records_outcome(&tail)])).expect("this decodes");
    let [Answer::Records { notes, only, .. }] = answers.as_slice() else {
        panic!("expected one Records outcome, got {answers:?}");
    };
    assert_eq!(notes.len(), 1);
    assert!(!*only);
}

/// The bytes of a `bytes` field: a `u32` length, then the bytes.
fn bytes_field(bytes: &[u8]) -> Vec<u8> {
    let mut out = u32::try_from(bytes.len())
        .expect("a test field is small")
        .to_be_bytes()
        .to_vec();
    out.extend_from_slice(bytes);
    out
}

#[test]
fn a_condition_is_written_after_an_empty_cursor_with_its_parameters_as_one_object() {
    use tessaridb_client::Follow;
    use tessaridb_client::codec::encode;

    let asked = Follow::resuming_after(6)
        .to_table("orders")
        .matching("total > $least")
        .binding("least", Value::from(100_i64));

    let mut want = 7_u64.to_be_bytes().to_vec();
    want.push(1);
    want.extend_from_slice(&text("orders"));
    // No cursor was asked for, and a condition needs its place filled (§3.7).
    want.extend_from_slice(&text(""));
    want.extend_from_slice(&text("total > $least"));
    let parameters = Value::Object(
        [("least".to_owned(), Value::from(100_i64))]
            .into_iter()
            .collect(),
    );
    want.extend_from_slice(&bytes_field(&encode(&parameters)));
    assert_eq!(asked.encode(), want);

    // A condition with no parameters still sends the object, empty.
    let bare = Follow::everything().to_table("orders").matching("open");
    let mut want = 0_u64.to_be_bytes().to_vec();
    want.push(1);
    want.extend_from_slice(&text("orders"));
    want.extend_from_slice(&text(""));
    want.extend_from_slice(&text("open"));
    want.extend_from_slice(&bytes_field(&encode(&Value::Object(
        std::collections::BTreeMap::new(),
    ))));
    assert_eq!(bare.encode(), want);

    // A resumed split feed keeps its cursor ahead of the condition.
    let split = Follow::resuming_at("1.1:d=12")
        .to_table("orders")
        .matching("open");
    let mut want = 0_u64.to_be_bytes().to_vec();
    want.push(1);
    want.extend_from_slice(&text("orders"));
    want.extend_from_slice(&text("1.1:d=12"));
    want.extend_from_slice(&text("open"));
    want.extend_from_slice(&bytes_field(&encode(&Value::Object(
        std::collections::BTreeMap::new(),
    ))));
    assert_eq!(split.encode(), want);

    // And a feed with no condition is the frame every earlier client sends.
    let mut plain = 7_u64.to_be_bytes().to_vec();
    plain.push(1);
    plain.extend_from_slice(&text("orders"));
    assert_eq!(Follow::resuming_after(6).to_table("orders").encode(), plain);
}

#[tokio::test]
async fn a_narrowed_feed_hands_over_progress_as_its_own_item_beside_changes() {
    use tessaridb_client::{Arrival, Became, Follow, Progress};

    let (ours, mut theirs) = tokio::io::duplex(4096);
    let node = tokio::spawn(async move {
        let (kind, _) = frame::read(&mut theirs).await.unwrap().unwrap();
        assert_eq!(kind, Kind::Subscribe);
        let mut progress = 41_u64.to_be_bytes().to_vec();
        progress.extend_from_slice(&text("1.1:d=12"));
        frame::write(&mut theirs, Kind::Progress, &progress)
            .await
            .unwrap();
        let mut change = 42_u64.to_be_bytes().to_vec();
        change.extend_from_slice(&text("orders"));
        change.extend_from_slice(&text("7"));
        change.push(1);
        frame::write(&mut theirs, Kind::Change, &change)
            .await
            .unwrap();
    });
    let mut feed = Client::with_stream(ours)
        .follow(&Follow::everything().to_table("orders").matching("open"))
        .await
        .unwrap();
    assert_eq!(
        feed.next().await.unwrap(),
        Some(Arrival::Progress(Progress {
            sequence: 41,
            cursor: Some("1.1:d=12".to_owned()),
        })),
        "a skipped run is told, so the resume point keeps up"
    );
    let Some(Arrival::Change(change)) = feed.next().await.unwrap() else {
        panic!("the change after the progress");
    };
    assert_eq!((change.sequence, change.became), (42, Became::Removed));
    node.await.unwrap();
    assert_eq!(feed.next().await.unwrap(), None);
}

#[tokio::test]
async fn a_condition_is_not_sent_to_a_node_before_minor_four() {
    use tessaridb_client::Follow;
    use tokio::io::AsyncReadExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let older = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut greeting = [0_u8; 6];
        socket.read_exact(&mut greeting).await.unwrap();
        socket.write_all(b"TESS\x01\x03").await.unwrap();
        // An older node reads past a condition and sends every change: the
        // client must not send one at all.
        let mut after = Vec::new();
        socket.read_to_end(&mut after).await.unwrap();
        after
    });
    let bound = std::time::Duration::from_secs(10);
    let client = tokio::time::timeout(bound, Client::connect(address))
        .await
        .expect("connecting took longer than the bound")
        .unwrap();
    let refused = client
        .follow(&Follow::everything().to_table("orders").matching("open"))
        .await
        .unwrap_err();
    assert!(
        matches!(
            refused,
            Error::NodeTooOld {
                found: 3,
                needed: 4
            }
        ),
        "{refused:?}"
    );
    let sent = tokio::time::timeout(bound, older)
        .await
        .expect("the fake node never saw the connection close")
        .unwrap();
    assert!(sent.is_empty(), "the client sent a frame the node misreads");
}
