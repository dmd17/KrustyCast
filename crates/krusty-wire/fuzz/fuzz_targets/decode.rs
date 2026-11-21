//! Every decoder on arbitrary input: must not panic, and whatever decodes
//! must re-encode to the same bytes (one encoding per value).
#![no_main]
use krusty_wire::feedback::{self, Feedback};
use krusty_wire::ingress::Submit;
use krusty_wire::stream::{
    DataFrameWriter, Frame, encode_end_of_session, encode_heartbeat, encode_recover,
    encode_unavailable,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|b: &[u8]| {
    let _ = krusty_wire::peek_type(b);
    for p in krusty_wire::batch::submits(b).flatten() {
        let _ = Submit::decode(p);
    }
    for f in krusty_wire::batch::feedbacks(b) {
        let _ = f;
    }
    if let Ok(s) = Submit::decode(b) {
        let mut out = vec![0u8; s.encoded_len()];
        s.encode(&mut out).unwrap();
        assert_eq!(out, b);
    }
    if let Ok(f) = Feedback::decode(b) {
        let mut out = [0u8; feedback::LEN];
        f.encode(&mut out).unwrap();
        assert_eq!(&out[..], b);
    }
    if let Ok(frame) = Frame::decode(b) {
        let mut out = vec![0u8; b.len()];
        let n = match frame {
            Frame::Data(f) => {
                let mut w = DataFrameWriter::new(&mut out, f.stream_id, f.seqno, f.retransmit).unwrap();
                for m in f.messages() {
                    w.push(&m.meta, m.payload).unwrap();
                }
                w.finish().unwrap()
            }
            Frame::Heartbeat { stream_id, next_seqno } => encode_heartbeat(&mut out, stream_id, next_seqno).unwrap(),
            Frame::EndOfSession { stream_id, next_seqno } => {
                encode_end_of_session(&mut out, stream_id, next_seqno).unwrap()
            }
            Frame::Unavailable { stream_id, from, earliest_retained } => {
                encode_unavailable(&mut out, stream_id, from, earliest_retained).unwrap()
            }
            Frame::Recover { stream_id, from, count } => encode_recover(&mut out, stream_id, from, count).unwrap(),
        };
        assert_eq!(&out[..n], b);
    }
});
