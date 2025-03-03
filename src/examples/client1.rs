use krustycast::client::KrustyClient;
use std::thread;
use std::time::Duration;
use clap::Parser;

/// Command-line argument parser
#[derive(Parser, Debug)]
#[command(name = "KrustyCast Client")]
#[command(about = "A client for the KrustyCast multicast sequencer.")]
struct Args {
    /// Unique sender ID (u32)
    #[arg(long)]
    my_id: u32,

    /// Sequencer IP address (e.g., "127.0.0.1:5000")
    #[arg(long)]
    to_sequencer_addr: String,

    /// Sequencer Output multicast address (e.g., "239.0.0.2:6000")
    #[arg(long)]
    from_sequencer_addr: String,

    /// Target ID (u32)
    #[arg(long)]
    target_id: u32
}

fn main() -> std::io::Result<()> {
    let args = Args::parse(); // Parse command-line arguments
    let my_id = args.my_id;
    let to_sequencer_addr = args.to_sequencer_addr;
    let from_sequencer_addr: String = args.from_sequencer_addr;

    // Create a new client instance.
    let client = KrustyClient::new(my_id, &to_sequencer_addr, &from_sequencer_addr);

    // Register callback for messages intended for me
    client.register_callback(|msg: Vec<u8>| {
        println!("Received msg for me: {:?}", String::from_utf8_lossy(&msg));
    });

    // Start the listener in a separate thread.
    client.start_listener();

    // Simulate sending messages periodically.
    for i in 0..10 {
        let payload = format!("Message {}", i);
        client.send_message(args.target_id, payload.as_bytes());

        thread::sleep(Duration::from_millis(500));
    }

    //client.stop();

    Ok(())
}
