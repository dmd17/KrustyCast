# KrustyCast: A Rust based multicast based message sequencer.

Clients send messages to a central sequencer, which puts them in one total
order and publishes them as a standard **MoldUDP64** multicast stream, with a
MoldUDP64 re-request server for gap recovery.

```
client ──UDP──▶ sequencer ──MoldUDP64 multicast──▶ all clients
   ▲               │  ▲
   └─ NAK/REJECT ──┘  └── re-requests (MoldUDP64 request packets)
```

- **Per-sender reliability:** clients number their messages; the sequencer NAKs gaps and the client resends.
- **Per-target optimistic concurrency:** each message claims the next sequence number of its target. The first claim wins; losers get a REJECT and retry automatically.
- **Idempotent resends:** a client resends anything the sequencer hasn't answered (covering a lost message or a lost REJECT). The sequencer answers a resend of an already-decided message with its original outcome and never sequences it twice. Each client process has a random incarnation ID, so a restarted client isn't mistaken for a duplicate.
- **Crash recovery:** with `--journal FILE`, every decision is journaled (and by default fsynced) before it is published. A sequencer restarted with the same `--session` and journal resumes where it left off. A cleanly ended session can't be resumed.
- **Downstream:** MoldUDP64 (10-byte session, heartbeats, end-of-session, re-requests). Each MoldUDP64 message is `sender_id: u32 | target_id: u32 | target_seq: u64 | payload`. See `src/protocol.rs`.

## Running

```sh
cargo run --bin sequencer -- --session 20261106A --journal 20261106A.journal   # Ctrl-C ends the session
cargo run --bin client1 -- --my-id 3 --target-id 1 --count 0 --linger-secs 30
cargo run --bin client1 -- --my-id 1 --target-id 3
```

Defaults: clients send to `239.0.0.1:5000`, the stream is `239.0.0.2:6000`,
and re-requests go to port `5001`. Pass `--to-sequencer-addr 127.0.0.1:5000`
to both sides to send to the sequencer by unicast. `client1 --replay` replays
the session from its first message.

`cargo test` runs unit tests and end-to-end tests over local multicast,
including interop in both directions with the
[`moldudp`](https://docs.rs/moldudp) crate.

## Design documents

- [Product requirements](docs/PRD.md): scope, guarantees, performance targets
- [Architecture](docs/ARCHITECTURE.md): target design and verification strategy
- [Gap analysis](docs/GAP-ANALYSIS.md): current code against the target, with roadmap
