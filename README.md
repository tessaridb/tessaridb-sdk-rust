<div align="center">

<img src="assets/logo/tessaridb-mark-256.png" alt="" width="88" height="88">

# TessariDB — Rust SDK

**The Rust client for [TessariDB](https://github.com/tessaridb/tessaridb).**

[![status](https://img.shields.io/badge/status-in%20development-D98E33?style=flat-square)](#status)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-6B5FD1?style=flat-square)](LICENSE)
[![protocol](https://img.shields.io/badge/protocol-v1.0-6B5FD1?style=flat-square)](https://github.com/tessaridb/tessaridb-protocol)

</div>

> [!WARNING]
> **Under active development, and not ready for production.** Unpublished to
> crates.io; the HTTP half of the client is partly written, and the API changes
> without notice. See [**Status**](#status).

```toml
[dependencies]
tessaridb-client = { git = "https://github.com/tessaridb/tessaridb-sdk-rust" }
```

The crate is `tessaridb-client`, not `tessaridb`: the plain name belongs to the
**engine**, which is the thing you embed, and this is the thing you call a node
with. The two also carry different licences, so a name that could fetch either
would be a name that decides your licence by accident.

This is the library you use to talk to a TessariDB node: run statements,
subscribe to changes, store and fetch files, and check whether a node is
healthy — without writing HTTP by hand.

Licence: **Apache-2.0**. See [`LICENSE`](LICENSE). Permissive, with an explicit
patent grant — the licence a client library you embed in your own product should
carry.

The server is licensed separately, under the Business Source License 1.1. The
boundary runs between the two on purpose: this client depends on the server's
*protocol*, never on its code, and nothing about the server's licence reaches
the application you build with this crate.

---

## Versions, and what actually has to match

This client's version is **its own** and never tracks the engine's. A fix here
would otherwise force an invented engine release, and an engine release would
force five invented client releases.

What has to match is the **protocol**. This release speaks **protocol 1.1** and
connects to any node of protocol **major 1**, which is checked in the greeting
before anything else is sent — a differing major is refused there rather than
discovered mid-conversation, where it arrives as a decode failure that reads
like corruption. A differing *minor* is not a refusal: the peer's minor is
reported so a caller can decline to send what an older node cannot read.


## Status

**Stage: active development · on crates.io as `tessaridb-client`.**

- ✅ **Runs:** the wire half — connect, run statements with bound parameters,
  decode every value type, subscribe to changes, and build the four common
  statements, including `STALENESS` and `ANSWERED BY`.
- ✅ **Runs:** the HTTP half — health, readiness, metrics, sessions, credentials,
  backup, and writing, sizing, reading and deleting a file in a bucket.
- 🚧 **Next:** the bucket listing, the one `/files` route this client does not
  offer yet.
- ⚠️ **Unstable:** the public API changes without notice while the server it
  talks to is pre-1.0.

It implements **protocol 1.1**: a two-number version where only a differing
major is a refusal, and an outcome kind this build has never seen is stepped over
by its length rather than ending the read.

The boundary was written before any code, on purpose. An SDK's README is where it
is decided what the library owns and what it refuses, and that decision is
expensive to reverse once callers depend on it — so it is made deliberately, in
one place, rather than emerging from whichever module someone writes first.

### Written against the protocol, not against the server

This client implements the published protocol specification and takes **no
dependency on the database's repository** — not by path, not by git, not by a
published crate. It depends on `tokio`, `thiserror` and `serde_json`, and on
nothing else directly.

That is what lets the same protocol have a client in any language, and it is why
a rename inside the server cannot break a client that never used the renamed
thing.

### It is asynchronous

`async fn` throughout, on Tokio. A change subscription holds a connection open
for as long as you care to listen, which is the workload a thread is the wrong
unit for — and it is the workload this client exists for.

```rust
use tessaridb_client::{Client, Follow, Value};

// An address is a host and a port, not a URL: the connection is a TCP socket
// carrying the wire protocol, and a scheme would imply a negotiation that does
// not happen.
let mut client = Client::connect("127.0.0.1:9080").await?;

let answers = client
    .run_with(
        "SELECT * FROM users WHERE age > $min;",
        None,
        [("min", Value::from(21_i64))],
    )
    .await?;

// A subscription consumes the connection: a socket delivering changes is not
// also answering scripts. Two jobs, two connections.
let mut feed = Client::connect("127.0.0.1:9080")
    .await?
    .follow(&Follow::everything().to_table("users"))
    .await?;

while let Some(change) = feed.next().await? {
    println!("{} {} at {}", change.table, change.id, change.sequence);
}
```

## What this SDK is for

One client, everything the node serves. If a TessariDB node answers it, this SDK
reaches it, and no caller should ever hand-roll an HTTP request to use a surface
the node already exposes.

## It speaks two transports, and that is not a design preference

A TessariDB node serves two surfaces, and **neither one carries everything**:

| | wire protocol | HTTP |
|---|---|---|
| statements and parameters | yes | yes |
| change subscription | yes | transport only, for now |
| objects and files | — | yes |
| backup | — | yes |
| health, readiness, metrics | — | yes |
| authentication | once per connection | per request, or per session token |

That table makes an HTTP-only client look like the obvious choice: it reaches
every route. It is the trap. HTTP answers are JSON, which carries **six** types,
while the wire protocol carries the store's full model of **seventeen**. An
HTTP-only SDK would therefore work, reach everything, and quietly narrow every
statement result — invisibly, because a value that has been through JSON is
still a perfectly valid value, and nothing at the call site shows what was lost.

The mirror mistake is just as available: a wire-only SDK keeps every type and
cannot store a file.

So this SDK uses **both**, and which transport carries an operation is fixed:

- **statements and subscriptions → the wire protocol**, for type fidelity;
- **objects, files, backup, health, readiness, metrics → HTTP**, because nothing
  else serves them.

The authentication row is the one asymmetry worth knowing before writing any HTTP
code. A wire connection proves who it is once and keeps that identity for its
lifetime. HTTP has no connection to keep it on, so a credential travels with
every request — and the node verifies a password with Argon2id at the OWASP
floor, deliberately, every time. Call `open_session()` and the handle spends the
password once and presents a token afterwards; skip it and every call is correct
and an order of magnitude slower, with nothing at the call site to say why.

You never choose a transport per call. But the two connections stay separately
configurable and separately reportable, because they are two ports: a firewall
rule, a service-mesh policy, or a partial bind can leave one reachable and the
other not. "Files work but queries don't" should be a state you can diagnose,
not a mystery.

## What it owns

- **Connecting and the session** — addresses, timeouts, reconnection.
- **Authentication** — credentials per call, or held for the session.
- **Retry**, with its boundary stated: transport failures are retried; a
  statement that reached the store and failed *there* is not, because the SDK
  cannot know it was safe to repeat.
- **The change subscription** — what changed, delivered as it happens.
- **Typed rows** — answers mapped into your own structs.
- **Objects and files** — put, get, list, delete. Not byte ranges: this version
  of the protocol has neither ranges nor resumption, and a client cannot own what
  the protocol does not carry.
- **The operational routes** — health, readiness, metrics.
- **The query builder** — re-exported from here, not a second dependency you are
  told to also add.

## What it does not own

**The language.** Statements are TessariQL. This SDK does not invent a second way to
express them. The query builder is not a dialect: it produces the same grammar
the server parses, and that is checked by round-tripping built queries through
the server's own parser rather than by reading them.

**The catalog.** The SDK cannot tell you that a table exists, that a field is
indexed, or that your types match the schema. That knowledge lives on the
server. A client-side validator would be a promise that holds in every test and
fails in production.

## The query builder

Typed and composable, resting on one guarantee: **it never puts a value into the
query text**.

```rust
let q = Select::from("users")
    .filter(field("age").gt(21).and(field("active").eq(true)))
    .order_by("created", Desc)
    .limit(50);
```

**On a cluster, two more clauses say which node may answer** rather than what the
answer holds:

```rust
let q = Select::from("orders")
    .staleness("30s")             // no node further behind than this may answer
    .answered_by(Answerer::Leader); // and it must be where writes are decided

// SELECT * FROM orders STALENESS 30s ANSWERED BY LEADER;
```

They are separate controls rather than one: a follower at zero lag is *level*,
not authoritative. The answerer is an enumeration, so the wrong word does not
compile; the span is a string because its **text** is the contract — `1m30s` and
`90s` are the same length and different statements. A bound tighter than the
cluster can know about itself is refused **by the node**, whose message names the
floor, so this client checks a span's shape and never its value.

**A long text field can come back a window at a time** rather than whole:

```rust
let head = Select::from("documents").field_lines("body", 0, 40);
let next = Select::from("documents").field_lines("body", 40, 40);
```

Lines count from zero, two adjacent windows reconstruct the field, and it
arrives under its own name — so the mapping that reads a `body` does not change
with the window. The two counts are written into the statement rather than
bound, which is the same rule `START` and `LIMIT` follow and for the same
reason: they are the statement's shape rather than data, and they are `u64`, so
there is no syntax to smuggle through them.

Values bind as parameters *after* the statement is parsed, so a string
containing `'; DROP TABLE users; --` is text that reads alarmingly and does
nothing at all. A builder that formatted values into the query would destroy the
one property this store's whole surface is designed around — and would do it
invisibly, because the output still looks correct.

**An incomplete query is caught, and where it is caught differs by what is
missing.** A source is taken by the constructor — `Select::from(table)` is the
only way to have a `Select` at all — so a read with nowhere to read from cannot
be written down. Fields are not: a `Create` or an `Update` with none is refused
by `build`, at run time, with a named error rather than a statement the node
would reject instead.

*(An earlier form of this paragraph claimed a `Select` with no source has no
`build`. That was true of an earlier builder in another repository and has never
been true here; corrected 2026-09-01.)*

### What proves the builder is not a second dialect

The rendering is fixed by a shared corpus rather than by this crate's opinion:
[`spec/query-builder-v1.md`][contract] states what a builder must render, and
`conformance/queries-v1.json` says it case by case. `cargo test` checks this
builder against it, and `cargo test --test node -- --ignored` additionally
**executes** every case against a running node — the only check that reaches the
parser, and the reason a builder in Python or Go will render exactly what this
one does.

[contract]: https://github.com/tessaridb/tessaridb-protocol/blob/main/spec/query-builder-v1.md

## Branches

- **`main`** — stable.
- **`dev`** — integration; work lands here first.

Matching the layout of the `TessariDB` repository.

## Building the surface

Each capability arrives with a test against a **running node**, not a mock. A
mock proves the SDK agrees with its author's belief about the protocol, which is
precisely the belief most likely to be wrong.
