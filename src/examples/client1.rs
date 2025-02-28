use krustycast::client::{start_listener, ClientSender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // Define addresses.
    let sequencer_addr = "127.0.0.1:5000";       // Address of the sequencer server.
    let multicast_addr = "239.0.0.2:6000";         // Multicast group for global ordering.

    // Create a new sender instance.
    let client_sender = ClientSender::new(sequencer_addr);

    // Clone the global sequence map to share with the listener.
    let global_seq_map = Arc::clone(&client_sender.global_sequence_map);

    // Start the listener in a separate thread.
    start_listener(multicast_addr, global_seq_map)?;

    // Simulate sending messages periodically.
    thread::spawn(move || {
        let target = 42; // Example target ID.
        for i in 0..10 {
            let payload = format!("Message {}", i);
            if let Err(e) = client_sender.send_message(target, payload.as_bytes()) {
                eprintln!("Send error: {}", e);
            }
            thread::sleep(Duration::from_millis(500));
        }
    });

    // Keep the main thread alive.
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

