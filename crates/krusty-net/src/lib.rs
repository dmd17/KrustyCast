//! Batched UDP I/O for the node's stages (gap P4): one `recvmmsg` or
//! `sendmmsg` system call moves up to a batch of datagrams. All buffers are
//! allocated up front; the hot path allocates nothing.
//!
//! IPv4 only, Linux only. The `unsafe` is confined to building the
//! `mmsghdr` arrays and the two system calls; pointers are rebuilt on every
//! call, so the structs hold no self-references.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::os::fd::AsRawFd;

fn to_sockaddr(a: SocketAddrV4) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: a.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*a.ip()).to_be(),
        },
        sin_zero: [0; 8],
    }
}

fn from_sockaddr(s: &libc::sockaddr_in) -> SocketAddrV4 {
    SocketAddrV4::new(
        Ipv4Addr::from(u32::from_be(s.sin_addr.s_addr)),
        u16::from_be(s.sin_port),
    )
}

fn zeroed_sockaddr() -> libc::sockaddr_in {
    to_sockaddr(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
}

/// Receives up to `capacity` datagrams of up to `mtu` bytes per call.
pub struct RecvBatch {
    mtu: usize,
    bufs: Vec<u8>,
    lens: Vec<usize>,
    addrs: Vec<libc::sockaddr_in>,
    iovs: Vec<libc::iovec>,
    hdrs: Vec<libc::mmsghdr>,
    n: usize,
}

// SAFETY: the raw pointers inside `iovs`/`hdrs` are only meaningful during a
// `recv` call on the owning thread; they are rebuilt before every call.
unsafe impl Send for RecvBatch {}

impl RecvBatch {
    pub fn new(capacity: usize, mtu: usize) -> RecvBatch {
        assert!((1..=1024).contains(&capacity));
        RecvBatch {
            mtu,
            bufs: vec![0; capacity * mtu],
            lens: vec![0; capacity],
            addrs: vec![zeroed_sockaddr(); capacity],
            iovs: vec![
                libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0
                };
                capacity
            ],
            // SAFETY: mmsghdr is plain old data; all-zero is a valid value.
            hdrs: vec![unsafe { std::mem::zeroed() }; capacity],
            n: 0,
        }
    }

    /// Receives what is queued, without blocking. Returns how many datagrams
    /// arrived (0 if none).
    pub fn recv(&mut self, sock: &UdpSocket) -> io::Result<usize> {
        let cap = self.lens.len();
        let base = self.bufs.as_mut_ptr();
        for i in 0..cap {
            // SAFETY: `i * mtu` is within `bufs`, which has `cap * mtu` bytes.
            self.iovs[i] = libc::iovec {
                iov_base: unsafe { base.add(i * self.mtu) } as *mut _,
                iov_len: self.mtu,
            };
            let h = &mut self.hdrs[i].msg_hdr;
            h.msg_name = &mut self.addrs[i] as *mut _ as *mut _;
            h.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;
            h.msg_iov = &mut self.iovs[i];
            h.msg_iovlen = 1;
            h.msg_control = std::ptr::null_mut();
            h.msg_controllen = 0;
            h.msg_flags = 0;
            self.hdrs[i].msg_len = 0;
        }
        // SAFETY: `hdrs` holds `cap` initialized headers whose pointers refer
        // to live buffers owned by `self`, which outlives the call.
        let r = unsafe {
            libc::recvmmsg(
                sock.as_raw_fd(),
                self.hdrs.as_mut_ptr(),
                cap as u32,
                libc::MSG_DONTWAIT,
                std::ptr::null_mut(),
            )
        };
        if r < 0 {
            let e = io::Error::last_os_error();
            self.n = 0;
            return if e.kind() == io::ErrorKind::WouldBlock {
                Ok(0)
            } else {
                Err(e)
            };
        }
        self.n = r as usize;
        for i in 0..self.n {
            // Datagrams longer than the MTU are truncated by the kernel
            // (MSG_TRUNC in msg_flags); report them as zero-length.
            let truncated = self.hdrs[i].msg_hdr.msg_flags & libc::MSG_TRUNC != 0;
            self.lens[i] = if truncated {
                0
            } else {
                self.hdrs[i].msg_len as usize
            };
        }
        Ok(self.n)
    }

    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Datagram `i` of the last `recv` and its source.
    pub fn get(&self, i: usize) -> (&[u8], SocketAddrV4) {
        assert!(i < self.n);
        (
            &self.bufs[i * self.mtu..i * self.mtu + self.lens[i]],
            from_sockaddr(&self.addrs[i]),
        )
    }
}

/// Queues datagrams and sends them with one `sendmmsg` per flush.
pub struct SendBatch {
    mtu: usize,
    bufs: Vec<u8>,
    lens: Vec<usize>,
    addrs: Vec<libc::sockaddr_in>,
    iovs: Vec<libc::iovec>,
    hdrs: Vec<libc::mmsghdr>,
    n: usize,
    /// `push_packed`: the datagram still open for each destination.
    open: std::collections::HashMap<SocketAddrV4, usize>,
}

/// Where `push_packed` put a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Packed {
    /// Into the datagram already queued for its destination.
    Appended,
    /// Into a new datagram.
    New,
    /// Nowhere: a new datagram was needed and the batch is full.
    Full,
}

// SAFETY: as for RecvBatch.
unsafe impl Send for SendBatch {}

impl SendBatch {
    pub fn new(capacity: usize, mtu: usize) -> SendBatch {
        assert!((1..=1024).contains(&capacity));
        SendBatch {
            mtu,
            bufs: vec![0; capacity * mtu],
            lens: vec![0; capacity],
            addrs: vec![zeroed_sockaddr(); capacity],
            iovs: vec![
                libc::iovec {
                    iov_base: std::ptr::null_mut(),
                    iov_len: 0
                };
                capacity
            ],
            // SAFETY: plain old data.
            hdrs: vec![unsafe { std::mem::zeroed() }; capacity],
            n: 0,
            open: std::collections::HashMap::with_capacity(capacity),
        }
    }

    pub fn is_full(&self) -> bool {
        self.n == self.lens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Queues a datagram built by `fill` (which returns its length). Returns
    /// false if the batch is full.
    pub fn push(&mut self, dest: SocketAddrV4, fill: impl FnOnce(&mut [u8]) -> usize) -> bool {
        if self.is_full() {
            return false;
        }
        let i = self.n;
        let len = fill(&mut self.bufs[i * self.mtu..(i + 1) * self.mtu]);
        assert!(len <= self.mtu);
        self.lens[i] = len;
        self.addrs[i] = to_sockaddr(dest);
        self.n += 1;
        self.open.remove(&dest);
        true
    }

    /// Queues a message of `len` bytes for `dest` (built by `fill`): after
    /// the messages already queued for `dest` in its open datagram if it
    /// fits under the MTU, else in a new datagram, which becomes the open
    /// one. Messages to one destination are thus packed back to back
    /// (krusty_wire::batch), in the order queued, with no waiting: what a
    /// flush sends is what was queued since the last.
    pub fn push_packed(
        &mut self,
        dest: SocketAddrV4,
        len: usize,
        fill: impl FnOnce(&mut [u8]),
    ) -> Packed {
        assert!(len <= self.mtu);
        if let Some(&i) = self.open.get(&dest)
            && self.lens[i] + len <= self.mtu
        {
            let at = i * self.mtu + self.lens[i];
            fill(&mut self.bufs[at..at + len]);
            self.lens[i] += len;
            return Packed::Appended;
        }
        if self.is_full() {
            return Packed::Full;
        }
        let i = self.n;
        fill(&mut self.bufs[i * self.mtu..i * self.mtu + len]);
        self.lens[i] = len;
        self.addrs[i] = to_sockaddr(dest);
        self.n += 1;
        self.open.insert(dest, i);
        Packed::New
    }

    /// Sends everything queued. Datagrams the kernel refuses are dropped
    /// (UDP is lossy anyway; receivers recover); returns how many were sent.
    pub fn flush(&mut self, sock: &UdpSocket) -> io::Result<usize> {
        let mut sent = 0;
        let base = self.bufs.as_mut_ptr();
        for i in 0..self.n {
            // SAFETY: within `bufs`.
            self.iovs[i] = libc::iovec {
                iov_base: unsafe { base.add(i * self.mtu) } as *mut _,
                iov_len: self.lens[i],
            };
            let h = &mut self.hdrs[i].msg_hdr;
            h.msg_name = &mut self.addrs[i] as *mut _ as *mut _;
            h.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;
            h.msg_iov = &mut self.iovs[i];
            h.msg_iovlen = 1;
            h.msg_control = std::ptr::null_mut();
            h.msg_controllen = 0;
            h.msg_flags = 0;
        }
        while sent < self.n {
            // SAFETY: headers `sent..n` are initialized and point into `self`.
            let r = unsafe {
                libc::sendmmsg(
                    sock.as_raw_fd(),
                    self.hdrs.as_mut_ptr().add(sent),
                    (self.n - sent) as u32,
                    0,
                )
            };
            if r < 0 {
                let e = io::Error::last_os_error();
                match e.kind() {
                    io::ErrorKind::Interrupted => continue,
                    // Drop the datagram that failed (e.g. no route) and go on.
                    _ => sent += 1,
                }
            } else {
                sent += r as usize;
            }
        }
        let n = self.n;
        self.n = 0;
        self.open.clear();
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_to_one_destination_are_packed_up_to_the_mtu() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let rx2 = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let v4 = |s: &UdpSocket| match s.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let (a, b) = (v4(&rx), v4(&rx2));
        let mut s = SendBatch::new(8, 100);
        // 40-byte messages: two fit in 100 bytes, the third opens a new one.
        let mut got = Vec::new();
        for (i, to) in [a, b, a, a, b, a].into_iter().enumerate() {
            got.push(s.push_packed(to, 40, |m| m.fill(i as u8)));
        }
        use Packed::*;
        assert_eq!(got, [New, New, Appended, New, Appended, Appended]);
        assert_eq!(s.flush(&tx).unwrap(), 3);
        rx.set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        rx2.set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let mut buf = [0u8; 200];
        let mut dgrams = Vec::new();
        for _ in 0..2 {
            let n = rx.recv(&mut buf).unwrap();
            dgrams.push(buf[..n].to_vec());
        }
        dgrams.sort_by_key(|d| d[0]);
        assert_eq!(dgrams[0], [[0u8; 40], [2; 40]].concat());
        assert_eq!(dgrams[1], [[3u8; 40], [5; 40]].concat());
        let n = rx2.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], &[[1u8; 40], [4; 40]].concat()[..]);
        // After a flush nothing is open.
        assert_eq!(s.push_packed(a, 40, |m| m.fill(9)), New);
    }

    #[test]
    fn batches_round_trip_over_loopback() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let to = match rx.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let from = match tx.local_addr().unwrap() {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!(),
        };
        let mut s = SendBatch::new(32, 1500);
        for i in 0..20u8 {
            assert!(s.push(to, |b| {
                b[..i as usize + 1].fill(i);
                i as usize + 1
            }));
        }
        assert_eq!(s.flush(&tx).unwrap(), 20);
        let mut r = RecvBatch::new(8, 1500);
        let mut got = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while got.len() < 20 && std::time::Instant::now() < deadline {
            let n = r.recv(&rx).unwrap();
            assert!(n <= 8);
            for i in 0..n {
                let (b, src) = r.get(i);
                assert_eq!(src, from);
                got.push(b.to_vec());
            }
        }
        assert_eq!(got.len(), 20);
        for (i, b) in got.iter().enumerate() {
            assert_eq!(b.len(), i + 1);
            assert!(b.iter().all(|&x| x == i as u8));
        }
        assert_eq!(r.recv(&rx).unwrap(), 0); // nothing left; does not block
    }
}
