//! Socket setup shared by the sequencer and the client.

use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

/// Large enough for any UDP datagram.
pub const RECV_BUFFER_SIZE: usize = 65536;

/// Kernel receive buffer requested for every receiving socket. The default
/// (`net.core.rmem_default`, often 208 KiB) holds only a few hundred small
/// datagrams, since each costs about 1 KiB of kernel accounting, so a short
/// scheduling stall drops packets. The kernel caps the request at
/// `net.core.rmem_max`.
pub const SOCKET_RECV_BUFFER: usize = 8 << 20;

/// Opens a socket that receives datagrams sent to `addr`.
///
/// For a multicast address the socket binds `0.0.0.0:<port>` with
/// SO_REUSEADDR (so several processes on one host can listen to the same
/// group) and joins the group on `interface`. For a unicast address it binds
/// `addr` directly.
pub fn bind_receiver(addr: SocketAddrV4, interface: Ipv4Addr) -> io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    // Best effort: a smaller buffer still works, it just drops sooner.
    let _ = socket.set_recv_buffer_size(SOCKET_RECV_BUFFER);
    if addr.ip().is_multicast() {
        socket.set_reuse_address(true)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, addr.port()).into())?;
        socket.join_multicast_v4(addr.ip(), &interface)?;
    } else {
        socket.bind(&addr.into())?;
    }
    Ok(socket.into())
}

/// Opens an ephemeral-port socket for sending to `dest`. When `dest` is
/// multicast, sets TTL 1 and, unless `interface` is unspecified, the outgoing
/// multicast interface.
pub fn bind_sender(dest: SocketAddrV4, interface: Ipv4Addr) -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
    if dest.ip().is_multicast() {
        socket.set_multicast_ttl_v4(1)?;
        if !interface.is_unspecified() {
            let s = Socket::from(socket);
            s.set_multicast_if_v4(&interface)?;
            return Ok(s.into());
        }
    }
    Ok(socket)
}
