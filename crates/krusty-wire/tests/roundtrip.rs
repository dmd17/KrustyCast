//! Property tests: encode∘decode = id, decode∘encode = id (canonical
//! encodings), and decoding arbitrary bytes never panics.
use krusty_wire::feedback::{self, Feedback, Outcome};
use krusty_wire::ingress::{Append, Submit};
use krusty_wire::stream::{
    self, DataFrameWriter, Frame, MessageMeta, encode_end_of_session, encode_heartbeat,
    encode_recover, encode_unavailable,
};
use krusty_wire::{WireError, msg_type, peek_type};
use proptest::prelude::*;

fn append() -> impl Strategy<Value = Append> {
    prop_oneof![
        Just(Append::Unconditional),
        (any::<u64>(), any::<u64>()).prop_map(|(key, expected_version)| Append::Conditional {
            key,
            expected_version
        }),
    ]
}

fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        any::<u64>().prop_map(|g| Outcome::Accepted { global_seqno: g }),
        (any::<u64>(), any::<u64>()).prop_map(|(key, current_version)| Outcome::Rejected {
            key,
            current_version
        }),
        Just(Outcome::Nak),
        any::<u64>().prop_map(|r| Outcome::Busy { retry_after_us: r }),
        any::<u64>().prop_map(|l| Outcome::NotLeader { leader: l }),
        any::<u64>().prop_map(|m| Outcome::SessionExpired { min_incarnation: m }),
    ]
}

fn meta() -> impl Strategy<Value = MessageMeta> {
    (
        any::<u64>(),
        any::<u32>(),
        proptest::option::of((any::<u64>(), any::<u64>())),
    )
        .prop_map(|(timestamp_ns, client_id, conditional)| MessageMeta {
            timestamp_ns,
            client_id,
            conditional,
        })
}

proptest! {
    #[test]
    fn submit_round_trip(
        client_id in any::<u32>(),
        incarnation in any::<u64>(),
        client_seqno in 1..u64::MAX,
        append in append(),
        payload in proptest::collection::vec(any::<u8>(), 0..300),
    ) {
        let s = Submit { client_id, incarnation, client_seqno, append, payload: &payload };
        let mut buf = vec![0u8; s.encoded_len()];
        let n = s.encode(&mut buf).unwrap();
        prop_assert_eq!(n, buf.len());
        let d = Submit::decode(&buf).unwrap();
        prop_assert_eq!(d, s);
        // Canonical: re-encoding the decoded value gives the same bytes.
        let mut again = vec![0u8; n];
        d.encode(&mut again).unwrap();
        prop_assert_eq!(again, buf);
    }

    #[test]
    fn feedback_round_trip(
        client_id in any::<u32>(),
        incarnation in any::<u64>(),
        client_seqno in any::<u64>(),
        outcome in outcome(),
    ) {
        let f = Feedback { client_id, incarnation, client_seqno, outcome };
        let mut buf = [0u8; feedback::LEN];
        f.encode(&mut buf).unwrap();
        prop_assert_eq!(Feedback::decode(&buf).unwrap(), f);
    }

    #[test]
    fn data_frame_round_trip(
        stream_id in any::<u64>(),
        seqno in 1..u64::MAX / 2,
        retransmit in any::<bool>(),
        msgs in proptest::collection::vec(
            (meta(), proptest::collection::vec(any::<u8>(), 0..40)), 1..20),
    ) {
        let mut buf = vec![0u8; 4096];
        let mut w = DataFrameWriter::new(&mut buf, stream_id, seqno, retransmit).unwrap();
        for (m, p) in &msgs {
            w.push(m, p).unwrap();
        }
        let n = w.finish().unwrap();
        let Frame::Data(f) = Frame::decode(&buf[..n]).unwrap() else { panic!("not data") };
        prop_assert_eq!((f.stream_id, f.seqno, f.count as usize, f.retransmit),
                        (stream_id, seqno, msgs.len(), retransmit));
        prop_assert_eq!(f.next_seqno(), seqno + msgs.len() as u64);
        let got: Vec<_> = f.messages().collect();
        prop_assert_eq!(got.len(), msgs.len());
        for (i, (m, (meta, p))) in got.iter().zip(&msgs).enumerate() {
            prop_assert_eq!(m.seqno, seqno + i as u64);
            prop_assert_eq!(&m.meta, meta);
            prop_assert_eq!(m.payload, &p[..]);
        }
    }

    #[test]
    fn control_frames_round_trip(stream_id in any::<u64>(), a in 1..u64::MAX / 2, b in 1..u64::MAX / 2) {
        let mut buf = [0u8; 64];
        let n = encode_heartbeat(&mut buf, stream_id, a).unwrap();
        prop_assert_eq!(Frame::decode(&buf[..n]).unwrap(), Frame::Heartbeat { stream_id, next_seqno: a });
        let n = encode_end_of_session(&mut buf, stream_id, a).unwrap();
        prop_assert_eq!(Frame::decode(&buf[..n]).unwrap(), Frame::EndOfSession { stream_id, next_seqno: a });
        let n = encode_unavailable(&mut buf, stream_id, a, b).unwrap();
        prop_assert_eq!(Frame::decode(&buf[..n]).unwrap(),
                        Frame::Unavailable { stream_id, from: a, earliest_retained: b });
        let n = encode_recover(&mut buf, stream_id, a, b).unwrap();
        prop_assert_eq!(Frame::decode(&buf[..n]).unwrap(), Frame::Recover { stream_id, from: a, count: b });
    }

    /// Any byte string: decoders return Ok or Err, never panic, and anything
    /// accepted re-encodes to the same bytes.
    #[test]
    fn arbitrary_bytes_never_panic(mut b in proptest::collection::vec(any::<u8>(), 0..200), t in any::<u8>()) {
        if b.len() >= 4 {
            // Give the decoders a fair chance to get past the prefix.
            b[0..2].copy_from_slice(&krusty_wire::MAGIC.to_le_bytes());
            b[2] = krusty_wire::VERSION;
            b[3] = t;
        }
        let _ = peek_type(&b);
        if let Ok(s) = Submit::decode(&b) {
            let mut out = vec![0u8; s.encoded_len()];
            s.encode(&mut out).unwrap();
            prop_assert_eq!(out, b.clone());
        }
        if let Ok(f) = Feedback::decode(&b) {
            let mut out = [0u8; feedback::LEN];
            f.encode(&mut out).unwrap();
            prop_assert_eq!(&out[..], &b[..]);
        }
        if let Ok(Frame::Data(f)) = Frame::decode(&b) {
            for m in f.messages() {
                let _ = m.payload.len();
            }
        }
    }
}

#[test]
fn rejects_malformed_headers() {
    let s = Submit {
        client_id: 1,
        incarnation: 2,
        client_seqno: 3,
        append: Append::Unconditional,
        payload: b"hi",
    };
    let mut buf = [0u8; 50];
    s.encode(&mut buf).unwrap();
    assert_eq!(Submit::decode(&buf[..49]), Err(WireError::Truncated));
    let mut long = buf.to_vec();
    long.push(0);
    assert_eq!(Submit::decode(&long), Err(WireError::TrailingBytes));
    let mut bad = buf;
    bad[0] ^= 1;
    assert_eq!(Submit::decode(&bad), Err(WireError::BadMagic));
    let mut bad = buf;
    bad[2] = 9;
    assert_eq!(Submit::decode(&bad), Err(WireError::UnsupportedVersion(9)));
    let mut bad = buf;
    bad[3] = msg_type::DATA;
    assert_eq!(
        Submit::decode(&bad),
        Err(WireError::UnknownType(msg_type::DATA))
    );
    let mut bad = buf;
    bad[6] = 7;
    assert_eq!(Submit::decode(&bad), Err(WireError::BadField("mode")));
    let mut bad = buf;
    bad[24..32].fill(0);
    assert_eq!(
        Submit::decode(&bad),
        Err(WireError::BadField("client_seqno"))
    );
}

#[test]
fn data_frame_rejects_seqno_overflow_and_bad_padding() {
    let mut buf = vec![0u8; 256];
    let meta = MessageMeta {
        timestamp_ns: 1,
        client_id: 1,
        conditional: None,
    };
    let mut w = DataFrameWriter::new(&mut buf, 7, u64::MAX - 1, false).unwrap();
    w.push(&meta, b"a").unwrap();
    assert_eq!(w.push(&meta, b"b"), Err(WireError::TooLarge)); // would end past u64::MAX
    let n = w.finish().unwrap();
    assert!(Frame::decode(&buf[..n]).is_ok());
    // Corrupt the count so the range overflows.
    buf[6..8].copy_from_slice(&2u16.to_le_bytes());
    assert_eq!(Frame::decode(&buf[..n]), Err(WireError::BadField("seqno")));
    buf[6..8].copy_from_slice(&1u16.to_le_bytes());
    buf[stream::HEADER_LEN + stream::BLOCK_HEADER_LEN + 1] = 9; // padding byte
    assert_eq!(
        Frame::decode(&buf[..n]),
        Err(WireError::BadField("padding"))
    );
}

proptest! {
    /// SUBMITs packed back to back split into the same SUBMITs.
    #[test]
    fn submit_batches_split_into_their_messages(
        msgs in proptest::collection::vec(
            (any::<u32>(), any::<u64>(), 1..u64::MAX, append(),
             proptest::collection::vec(any::<u8>(), 0..200)),
            1..12),
    ) {
        let mut d = Vec::new();
        for (client_id, incarnation, client_seqno, append, payload) in &msgs {
            let s = Submit { client_id: *client_id, incarnation: *incarnation,
                             client_seqno: *client_seqno, append: *append, payload };
            let mut b = vec![0u8; s.encoded_len()];
            s.encode(&mut b).unwrap();
            d.extend_from_slice(&b);
        }
        let got: Vec<Submit> = krusty_wire::batch::submits(&d)
            .map(|p| Submit::decode(p.unwrap()).unwrap())
            .collect();
        prop_assert_eq!(got.len(), msgs.len());
        for (g, (client_id, incarnation, client_seqno, append, payload)) in got.iter().zip(&msgs) {
            prop_assert_eq!(g.client_id, *client_id);
            prop_assert_eq!(g.incarnation, *incarnation);
            prop_assert_eq!(g.client_seqno, *client_seqno);
            prop_assert_eq!(g.append, *append);
            prop_assert_eq!(g.payload, &payload[..]);
        }
        // Cut short anywhere inside the last message: the earlier ones
        // still come out, then an error, then nothing.
        let cut = d.len() - 1;
        let pieces: Vec<_> = krusty_wire::batch::submits(&d[..cut]).collect();
        prop_assert_eq!(pieces.len(), msgs.len());
        prop_assert!(pieces[..msgs.len() - 1].iter().all(|p| p.is_ok()));
        prop_assert_eq!(pieces[msgs.len() - 1], Err(WireError::Truncated));
    }

    /// Outcomes packed back to back split into the same outcomes.
    #[test]
    fn feedback_batches_split_into_their_outcomes(
        fs in proptest::collection::vec(
            (any::<u32>(), any::<u64>(), any::<u64>(), outcome()), 1..30),
    ) {
        let mut d = Vec::new();
        let mut want = Vec::new();
        for (client_id, incarnation, client_seqno, outcome) in fs {
            let f = Feedback { client_id, incarnation, client_seqno, outcome };
            let mut b = [0u8; feedback::LEN];
            f.encode(&mut b).unwrap();
            d.extend_from_slice(&b);
            want.push(f);
        }
        let got: Vec<Feedback> = krusty_wire::batch::feedbacks(&d).map(|f| f.unwrap()).collect();
        prop_assert_eq!(got, want);
    }

    /// Splitting arbitrary bytes never panics, and the pieces cover the
    /// datagram.
    #[test]
    fn splitting_arbitrary_bytes_never_panics(b in proptest::collection::vec(any::<u8>(), 0..600)) {
        let mut total = 0;
        for p in krusty_wire::batch::submits(&b) {
            match p {
                Ok(p) => { total += p.len(); let _ = Submit::decode(p); }
                Err(_) => { total = b.len(); }
            }
        }
        prop_assert_eq!(total, b.len());
        for f in krusty_wire::batch::feedbacks(&b) { let _ = f; }
    }
}
