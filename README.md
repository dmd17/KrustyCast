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
- **Downstream:** MoldUDP64 (10-byte session, heartbeats, end-of-session, re-requests). Each MoldUDP64 message is `sender_id: u32 | target_id: u32 | target_seq: u64 | payload`. See `src/protocol.rs`.

## Running

```sh
cargo run --bin sequencer -- --session 20261106A          # Ctrl-C ends the session
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
