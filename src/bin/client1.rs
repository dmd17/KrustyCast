use clap::Parser;
use krustycast::client::{ClientConfig, KrustyClient};
use krustycast::protocol::Session;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::{Duration, Instant};

/// Command-line argument parser
#[derive(Parser, Debug)]
#[command(name = "KrustyCast Client")]
#[command(about = "A client for the KrustyCast multicast sequencer.")]
struct Args {
    /// Unique sender ID (u32)
    #[arg(long)]
    my_id: u32,

    /// Target ID (u32)
    #[arg(long)]
    target_id: u32,

    /// Sequencer input address (e.g. "239.0.0.1:5000" or "127.0.0.1:5000")
    #[arg(long, default_value = "239.0.0.1:5000")]
    to_sequencer_addr: SocketAddrV4,

    /// Sequencer output multicast address
    #[arg(long, default_value = "239.0.0.2:6000")]
    from_sequencer_addr: SocketAddrV4,

    /// Sequencer re-request server address
    #[arg(long, default_value = "127.0.0.1:5001")]
    rerequest_addr: SocketAddrV4,

    /// Don't recover lost packets; skip gaps
    #[arg(long)]
    no_recovery: bool,

    /// Replay the session from the first message instead of joining live
    #[arg(long)]
    replay: bool,

    /// Only follow this MoldUDP64 session
    #[arg(long)]
    session: Option<Session>,

    /// Number of messages to send
    #[arg(long, default_value_t = 10)]
    count: u32,

    /// Delay between messages, in milliseconds
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,

    /// Seconds to keep listening after the last message is sequenced
    #[arg(long, default_value_t = 2)]
    linger_secs: u64,

    /// Local interface address for multicast (0.0.0.0 = let the OS choose)
    #[arg(long, default_value = "0.0.0.0")]
    interface: Ipv4Addr,

    /// Log retries, NAK resends and re-requests
    #[arg(long, short)]
    verbose: bool,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let my_id = args.my_id;

    let client = KrustyClient::new(ClientConfig {
        to_sequencer_addr: args.to_sequencer_addr,
        from_sequencer_addr: args.from_sequencer_addr,
        rerequest_addr: (!args.no_recovery).then_some(args.rerequest_addr),
        interface: args.interface,
        session: args.session,
        start_seqno: args.replay.then_some(1),
        verbose: args.verbose,
        ..ClientConfig::new(my_id)
    })?;

    client.register_callback(move |msg: Vec<u8>| {
        println!("[client {}] received: {}", my_id, String::from_utf8_lossy(&msg));
    });

    client.start_listener()?;

    for i in 0..args.count {
        let payload = format!("Message {} from {}", i, my_id);
        client.send_message(args.target_id, payload.as_bytes());
        thread::sleep(Duration::from_millis(args.interval_ms));
    }

    // Wait for our messages to show up on the sequenced stream.
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.pending() > 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    match client.pending() {
        0 => println!("[client {}] all {} messages sequenced", my_id, args.count),
        n => eprintln!("[client {}] {} messages still unsequenced after 5s", my_id, n),
    }

    thread::sleep(Duration::from_secs(args.linger_secs));
    println!(
        "[client {}] processed {} stream messages, lost {}, failed to send {}",
        my_id,
        client.delivered(),
        client.lost(),
        client.failed()
    );
    client.stop();
    Ok(())
}
