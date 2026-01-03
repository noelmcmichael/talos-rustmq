// Test producer client for RustMQ

use anyhow::Result;
use rustmq::message::Message;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 RustMQ Producer Test");
    println!("{}", "=".repeat(70));
    
    let addr = "192.168.64.11:31092";
    println!("Connecting to {}...", addr);
    
    let mut stream = TcpStream::connect(addr).await?;
    println!("✅ Connected to RustMQ producer server\n");
    
    // Test 1: Send messages to different partitions
    println!("📤 Test 1: Sending test messages");
    println!("{}", "-".repeat(70));
    
    for i in 0..5 {
        let key = format!("test-key-{}", i).into_bytes();
        let value = format!("{{\"test\": \"message_{}\", \"partition\": {}}}", i, i).into_bytes();
        
        let message = Message::new(key, value);
        let offset = send_message(&mut stream, message).await?;
        
        println!("  ✅ Sent message {}, offset: {}", i, offset);
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    // Test 2: Rapid message sending
    println!("\n📤 Test 2: Rapid message sending (100 messages)");
    println!("{}", "-".repeat(70));
    
    let start = Instant::now();
    for i in 0..100 {
        let key = format!("key-{}", i).into_bytes();
        let value = format!("{{\"message_id\": {}}}", i).into_bytes();
        
        let message = Message::new(key, value);
        send_message(&mut stream, message).await?;
        
        if i % 20 == 0 {
            println!("  Progress: {}/100 messages sent...", i);
        }
    }
    
    let elapsed = start.elapsed();
    let throughput = 100.0 / elapsed.as_secs_f64();
    
    println!("\n  ✅ Sent 100 messages in {:.2?}", elapsed);
    println!("  📊 Throughput: {:.1} msg/s", throughput);
    
    // Test 3: Large message
    println!("\n📤 Test 3: Large message test");
    println!("{}", "-".repeat(70));
    
    let large_value = "x".repeat(10000); // 10KB
    let message = Message::new(b"large-key".to_vec(), large_value.into_bytes());
    let offset = send_message(&mut stream, message).await?;
    
    println!("  ✅ Sent large message (10KB), offset: {}", offset);
    
    // Summary
    println!("\n{}", "=".repeat(70));
    println!("✅ Producer Test Complete!");
    println!("   Total messages sent: 106");
    println!("   Average throughput: {:.1} msg/s", throughput);
    
    Ok(())
}

async fn send_message(stream: &mut TcpStream, message: Message) -> Result<u64> {
    // Serialize message using bincode
    let msg_bytes = bincode::serialize(&message)?;
    let msg_len = msg_bytes.len() as u32;
    
    // Send length (u32, big-endian)
    stream.write_u32(msg_len).await?;
    
    // Send message bytes
    stream.write_all(&msg_bytes).await?;
    
    // Read offset response (u64, big-endian)
    let offset = stream.read_u64().await?;
    
    if offset == u64::MAX {
        anyhow::bail!("Server returned error (u64::MAX)");
    }
    
    Ok(offset)
}
