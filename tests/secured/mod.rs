//! TLS against a running node (protocol §1.1): a node started with a
//! certificate is reached by a client that verified it, on the wire and over
//! HTTP; a client in the clear and a client trusting another authority are not.
//!
//! The certificate authority is minted in memory for each run, so no key is
//! ever written to this repository.

use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::process::{Child, Command, Stdio};

use tessaridb_client::{Answer, Client, Condition, Error, Number, Operations, Tls, Value};

use super::STARTING;

/// A node serving both surfaces over TLS, and the PEM of the authority that
/// issued its certificate.
struct Secured {
    child: Child,
    wire: String,
    http: String,
    authority: String,
    _files: Files,
}

/// The certificate files the node reads, removed with it.
struct Files(std::path::PathBuf);

impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl Drop for Secured {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// An authority, and a leaf for `127.0.0.1` it issued: chain, key, authority.
fn issued() -> (String, String, String) {
    let authority_key = rcgen::KeyPair::generate().unwrap();
    let mut asked = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    asked.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let authority = asked.self_signed(&authority_key).unwrap();
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .unwrap()
        .signed_by(&leaf_key, &authority, &authority_key)
        .unwrap();
    (leaf.pem(), leaf_key.serialize_pem(), authority.pem())
}

fn free_port() -> String {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("127.0.0.1:{}", probe.local_addr().unwrap().port())
}

impl Secured {
    async fn start() -> Self {
        let binary = std::env::var("TESSARIDB_BIN").expect("TESSARIDB_BIN is the node under test");
        let (chain, key, authority) = issued();
        let directory = std::env::temp_dir().join(format!(
            "tessaridb-client-tls-{}-{}",
            std::process::id(),
            free_port().replace([':', '.'], "-")
        ));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join("cert.pem"), chain).unwrap();
        // The node refuses a key file others may read, so it is never written
        // readable in the first place.
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join("key.pem"))
            .and_then(|mut file| file.write_all(key.as_bytes()))
            .unwrap();
        let files = Files(directory.clone());

        let _slot = STARTING.lock().await;
        let (wire, http) = (free_port(), free_port());
        let child = Command::new(&binary)
            .args(["--serve", &wire, "--http", &http])
            .arg("--tls-cert")
            .arg(directory.join("cert.pem"))
            .arg("--tls-key")
            .arg(directory.join("key.pem"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let started = std::time::Instant::now();
        while tokio::net::TcpStream::connect(&wire).await.is_err() {
            assert!(
                started.elapsed() < super::STARTUP_BUDGET,
                "the node never listened"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Self {
            child,
            wire,
            http,
            authority,
            _files: files,
        }
    }

    fn trust(&self) -> Tls {
        Tls::trusting_pem(self.authority.as_bytes()).unwrap()
    }
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_client_that_verified_the_node_is_answered_on_the_wire_and_over_http() {
    let node = Secured::start().await;
    let mut client = Client::connect_tls(&node.wire, &node.trust())
        .await
        .unwrap();
    let answers = client.run("RETURN 40 + 2;", None).await.unwrap();
    assert!(
        matches!(
            &answers[..],
            [Answer::Value {
                value: Value::Number(Number::Integer(42)),
                ..
            }]
        ),
        "{answers:?}"
    );

    let health = Operations::at(&node.http)
        .with_tls(node.trust())
        .health()
        .await
        .unwrap();
    assert!(matches!(health, Condition::Ok { .. }), "{health:?}");
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_client_in_the_clear_is_not_answered() {
    let node = Secured::start().await;
    let refused = Client::connect(&node.wire)
        .await
        .expect_err("a plaintext greeting was answered");
    assert!(
        matches!(
            refused,
            Error::NotThisProtocol | Error::Io(_) | Error::Truncated
        ),
        "{refused}"
    );
    let unanswered = Operations::at(&node.http).health().await;
    assert!(
        unanswered.is_err(),
        "HTTP in the clear was answered: {unanswered:?}"
    );
}

#[tokio::test]
#[ignore = "needs the shipped binary; run with --ignored and TESSARIDB_BIN set"]
async fn a_client_trusting_another_authority_refuses_the_node_and_does_not_retry() {
    let node = Secured::start().await;
    let (_, _, someone_elses) = issued();
    let other = Tls::trusting_pem(someone_elses.as_bytes()).unwrap();
    let refused = Client::connect_tls(&node.wire, &other)
        .await
        .expect_err("a certificate from an authority this client does not trust");
    assert!(matches!(refused, Error::Tls(_)), "{refused}");

    let started = std::time::Instant::now();
    let refused = Operations::at(&node.http)
        .with_tls(other)
        .attempts(5)
        .health()
        .await
        .expect_err("the HTTP surface with the wrong trust");
    assert!(matches!(refused, Error::Tls(_)), "{refused}");
    // Five attempts carry four 50 ms pauses, so a retried handshake cannot
    // finish inside 150 ms.
    assert!(
        started.elapsed() < std::time::Duration::from_millis(150),
        "the handshake was retried"
    );
}
