use clap::Parser;
use krustycast::protocol::Session;
use krustycast::sequencer::{self, SequencerConfig};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

/// Command-line argument parser
#[derive(Parser, Debug)]
#[command(name = "KrustyCast Sequencer")]
#[command(about = "The KrustyCast multicast sequencer, publishing MoldUDP64.")]
struct Args {
    /// MoldUDP64 session name (1-10 printable ASCII characters), e.g. a date like 20261106A
    #[arg(long)]
    session: Session,

    /// Address clients send to: a multicast group to join, or a local unicast address to bind
    #[arg(long, default_value = "239.0.0.1:5000")]
    to_sequencer_addr: SocketAddrV4,

    /// Multicast group the MoldUDP64 stream is published to
    #[arg(long, default_value = "239.0.0.2:6000")]
    from_sequencer_addr: SocketAddrV4,

    /// Local address for the MoldUDP64 re-request server
    #[arg(long, default_value = "0.0.0.0:5001")]
    rerequest_addr: SocketAddrV4,

    /// Local interface address for multicast (0.0.0.0 = let the OS choose)
    #[arg(long, default_value = "0.0.0.0")]
    interface: Ipv4Addr,

    /// Messages kept for retransmission
    #[arg(long, default_value_t = 1_000_000)]
    history: usize,

    /// Maximum datagram size for batching and retransmissions
    #[arg(long, default_value_t = 1400)]
    max_packet_size: usize,

    /// Seconds to announce end-of-session (and serve re-requests) after Ctrl-C
    #[arg(long, default_value_t = 2)]
    end_of_session_secs: u64,

    /// Log every sequenced and rejected message
    #[arg(long, short)]
    verbose: bool,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let cfg = SequencerConfig {
        input_addr: args.to_sequencer_addr,
        output_addr: args.from_sequencer_addr,
        rerequest_addr: args.rerequest_addr,
        interface: args.interface,
        history_limit: args.history,
        max_packet_size: args.max_packet_size,
        end_of_session_linger: Duration::from_secs(args.end_of_session_secs),
        verbose: args.verbose,
        ..SequencerConfig::new(args.session)
    };

    let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(1);
    ctrlc::set_handler(move || {
        let _ = stop_tx.try_send(());
    })
    .expect("could not install Ctrl-C handler");

    let handle = sequencer::start(cfg)?;
    println!(
        "Listening on {}, publishing to {}, re-requests on {} (Ctrl-C to end the session)",
        handle.input_local_addr(),
        args.from_sequencer_addr,
        handle.rerequest_local_addr()
    );
    let _ = stop_rx.recv();
    handle.shutdown();
    Ok(())
}
