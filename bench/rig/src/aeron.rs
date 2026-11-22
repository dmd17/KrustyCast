//! An Aeron Cluster client over Aeron's C client: the subset of the Java
//! `AeronCluster` the benchmark uses. Connects to one member's ingress (the
//! appointed leader), takes the session from the SessionEvent on egress,
//! then offers session messages (SBE header + SessionMessageHeader +
//! payload) and reads the echo service's replies. SBE schema 111, version
//! 17, protocol 0.3.0, as in aeron-cluster 1.54.0's codecs.
use crate::aeron_c::*;
use crate::{CHECKSUM, Transceiver, now_ns};
use std::ffi::{CString, c_void};
use std::time::Instant;

const SCHEMA_ID: u16 = 111;
const SCHEMA_VERSION: u16 = 17;
const PROTOCOL_VERSION: i32 = 3 << 8; // SemanticVersion.compose(0, 3, 0)
const HEADER: usize = 8; // MessageHeader: blockLength, templateId, schemaId, version
const SESSION_MESSAGE: u16 = 1; // SessionMessageHeader, block 24
const SESSION_EVENT: u16 = 2; // SessionEvent, block 44
const CONNECT: u16 = 3; // SessionConnectRequest, block 16
const KEEP_ALIVE: u16 = 5; // SessionKeepAlive, block 16
const NEW_LEADER: u16 = 6; // NewLeaderEvent, block 20
const INGRESS_STREAM: i32 = 101;
const EGRESS_STREAM: i32 = 102;
const SESSION_HEADER: usize = HEADER + 24;

fn put_header(b: &mut [u8], block: u16, template: u16) {
    b[0..2].copy_from_slice(&block.to_le_bytes());
    b[2..4].copy_from_slice(&template.to_le_bytes());
    b[4..6].copy_from_slice(&SCHEMA_ID.to_le_bytes());
    b[6..8].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
}

fn u16_at(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}
fn i64_at(b: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}
fn i32_at(b: &[u8], at: usize) -> i32 {
    i32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

/// What the fragment handler fills in (passed as its clientd).
struct Egress {
    /// From the SessionEvent answering our connect.
    event: Option<(i64, i64, i32)>, // (cluster session id, leadership term id, code)
    correlation: i64,
    leadership_term: i64,
    got: Option<*mut dyn FnMut(u64)>,
    payload: usize,
}

unsafe extern "C" fn on_fragment(clientd: *mut c_void, buffer: *const u8, length: usize, _h: *mut aeron_header_t) {
    // SAFETY: clientd is the Egress the poll was given; buffer holds length bytes.
    let e = unsafe { &mut *(clientd as *mut Egress) };
    let b = unsafe { std::slice::from_raw_parts(buffer, length) };
    if b.len() < HEADER {
        return;
    }
    match u16_at(b, 2) {
        SESSION_MESSAGE if b.len() >= SESSION_HEADER + e.payload => {
            let p = &b[SESSION_HEADER..];
            let ts = u64::from_le_bytes(p[0..8].try_into().unwrap());
            let sum = u64::from_le_bytes(p[e.payload - 8..e.payload].try_into().unwrap());
            assert_eq!(sum, CHECKSUM, "echo with a wrong checksum");
            if let Some(f) = e.got {
                // SAFETY: set by receive() for the duration of the poll.
                unsafe { (*f)(ts) };
            }
        }
        SESSION_EVENT if b.len() >= HEADER + 32 => {
            let (session, correlation) = (i64_at(b, HEADER), i64_at(b, HEADER + 8));
            let (term, code) = (i64_at(b, HEADER + 16), i32_at(b, HEADER + 28));
            if correlation == e.correlation {
                e.event = Some((session, term, code));
            }
        }
        NEW_LEADER if b.len() >= HEADER + 16 => {
            e.leadership_term = i64_at(b, HEADER);
        }
        _ => {}
    }
}

pub struct AeronCluster {
    client: *mut aeron_t,
    ctx: *mut aeron_context_t,
    ingress: *mut aeron_exclusive_publication_t,
    egress: *mut aeron_subscription_t,
    state: Box<Egress>,
    session: i64,
    msg: Vec<u8>,
    payload: usize,
    epoch: Instant,
    last_sent: u64,
}

impl AeronCluster {
    pub fn connect(dir: &str, ingress: &str, egress: &str, payload: usize, epoch: Instant) -> AeronCluster {
        let check = |r: i32, what: &str| assert!(r >= 0, "{what}: {}", errmsg());
        let mut ctx = std::ptr::null_mut();
        let mut client = std::ptr::null_mut();
        let dir_c = CString::new(dir).unwrap();
        // SAFETY: plain calls into the C client with valid pointers.
        unsafe {
            check(aeron_context_init(&mut ctx), "context");
            check(aeron_context_set_dir(ctx, dir_c.as_ptr()), "aeron dir");
            check(aeron_init(&mut client, ctx), "init");
            check(aeron_start(client), "start");
        }
        let (in_c, eg_c) = (CString::new(ingress).unwrap(), CString::new(egress).unwrap());
        let mut pub_async = std::ptr::null_mut();
        let mut sub_async = std::ptr::null_mut();
        let mut publication: *mut aeron_exclusive_publication_t = std::ptr::null_mut();
        let mut subscription: *mut aeron_subscription_t = std::ptr::null_mut();
        // SAFETY: as above; the async handles are polled until done.
        unsafe {
            check(aeron_async_add_exclusive_publication(&mut pub_async, client, in_c.as_ptr(), INGRESS_STREAM), "ingress");
            check(
                aeron_async_add_subscription(
                    &mut sub_async,
                    client,
                    eg_c.as_ptr(),
                    EGRESS_STREAM,
                    None,
                    std::ptr::null_mut(),
                    None,
                    std::ptr::null_mut(),
                ),
                "egress",
            );
            while publication.is_null() {
                check(aeron_async_add_exclusive_publication_poll(&mut publication, pub_async), "ingress poll");
                std::thread::yield_now();
            }
            while subscription.is_null() {
                check(aeron_async_add_subscription_poll(&mut subscription, sub_async), "egress poll");
                std::thread::yield_now();
            }
        }
        let mut c = AeronCluster {
            client,
            ctx,
            ingress: publication,
            egress: subscription,
            state: Box::new(Egress {
                event: None,
                correlation: 1,
                leadership_term: -1,
                got: None,
                payload,
            }),
            session: -1,
            msg: vec![0u8; SESSION_HEADER + payload],
            payload,
            epoch,
            last_sent: 0,
        };
        // SessionConnectRequest: correlationId, responseStreamId, version,
        // then responseChannel and (empty) encodedCredentials.
        let mut req = vec![0u8; HEADER + 16 + 4 + egress.len() + 4];
        put_header(&mut req, 16, CONNECT);
        req[HEADER..HEADER + 8].copy_from_slice(&c.state.correlation.to_le_bytes());
        req[HEADER + 8..HEADER + 12].copy_from_slice(&EGRESS_STREAM.to_le_bytes());
        req[HEADER + 12..HEADER + 16].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        req[HEADER + 16..HEADER + 20].copy_from_slice(&(egress.len() as u32).to_le_bytes());
        req[HEADER + 20..HEADER + 20 + egress.len()].copy_from_slice(egress.as_bytes());
        let deadline = Instant::now() + std::time::Duration::from_secs(10);
        let mut sent = false;
        while c.state.event.is_none() {
            assert!(Instant::now() < deadline, "no SessionEvent from the cluster");
            // SAFETY: valid publication and buffer.
            if !sent && unsafe { aeron_exclusive_publication_offer(c.ingress, req.as_ptr(), req.len(), None, std::ptr::null_mut()) } > 0 {
                sent = true;
            }
            c.poll();
        }
        let (session, term, code) = c.state.event.unwrap();
        assert_eq!(code, 0, "connect refused (EventCode {code})");
        c.session = session;
        c.state.leadership_term = term;
        eprintln!("aeron: session {session}, leadership term {term}");
        put_header(&mut c.msg, 24, SESSION_MESSAGE);
        c
    }

    fn poll(&mut self) -> i32 {
        let st: *mut Egress = &mut *self.state;
        // SAFETY: the subscription is valid; st outlives the call.
        unsafe { aeron_subscription_poll(self.egress, Some(on_fragment), st as *mut c_void, 10) }
    }

    fn keep_alive(&mut self) {
        let mut b = [0u8; HEADER + 16];
        put_header(&mut b, 16, KEEP_ALIVE);
        b[HEADER..HEADER + 8].copy_from_slice(&self.state.leadership_term.to_le_bytes());
        b[HEADER + 8..HEADER + 16].copy_from_slice(&self.session.to_le_bytes());
        // SAFETY: valid publication and buffer.
        unsafe { aeron_exclusive_publication_offer(self.ingress, b.as_ptr(), b.len(), None, std::ptr::null_mut()) };
    }
}

impl Transceiver for AeronCluster {
    fn send(&mut self, n: usize, ts: u64) -> usize {
        let term = self.state.leadership_term;
        let m = &mut self.msg;
        m[HEADER..HEADER + 8].copy_from_slice(&term.to_le_bytes());
        m[HEADER + 8..HEADER + 16].copy_from_slice(&self.session.to_le_bytes());
        m[SESSION_HEADER..SESSION_HEADER + 8].copy_from_slice(&ts.to_le_bytes());
        let end = SESSION_HEADER + self.payload;
        m[end - 8..end].copy_from_slice(&CHECKSUM.to_le_bytes());
        let mut k = 0;
        while k < n {
            // SAFETY: valid publication and buffer.
            let r = unsafe { aeron_exclusive_publication_offer(self.ingress, m.as_ptr(), m.len(), None, std::ptr::null_mut()) };
            if r < 0 {
                break; // back pressure (or not connected): the rig retries
            }
            k += 1;
        }
        if k > 0 {
            self.last_sent = now_ns(self.epoch);
        }
        k
    }

    fn receive(&mut self, got: &mut dyn FnMut(u64)) {
        // SAFETY (lifetime): the pointer is only used during this poll.
        let p: *mut (dyn FnMut(u64) + '_) = got;
        self.state.got = Some(unsafe { std::mem::transmute::<*mut (dyn FnMut(u64) + '_), *mut dyn FnMut(u64)>(p) });
        self.poll();
        self.state.got = None;
        let now = now_ns(self.epoch);
        if now.saturating_sub(self.last_sent) > 1_000_000_000 {
            self.keep_alive();
            self.last_sent = now;
        }
    }
}

impl Drop for AeronCluster {
    fn drop(&mut self) {
        // SAFETY: closing what connect created (publications and
        // subscriptions are freed with the client).
        unsafe {
            aeron_close(self.client);
            aeron_context_close(self.ctx);
        }
    }
}
