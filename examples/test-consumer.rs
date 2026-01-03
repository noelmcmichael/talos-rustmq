// Test consumer client for RustMQ

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    println!("📥 RustMQ Consumer Test");
    println!("{}", "=".repeat(70));
    
    let addr = "192.168.64.11:31093";
    println!("Connecting to {}...", addr);
    
    let mut stream = TcpStream::connect(addr).await?;
    println!("✅ Connected to RustMQ consumer server\n");
    
    // Test 1: Consume from partition 0
    println!("📥 Test 1: Consuming from partition 0 (offset 0, count 10)");
    println!("{}", "-".repeat(70));
    
    let messages = consume_messages(&mut stream, 0, 0, 10).await?;
    println!("  📊 Received {} messages", messages.len());
    
    for (i, msg) in messages.iter().enumerate().take(5) {
        let value_str = String::from_utf8_lossy(&msg.value);
        println!("  📬 Offset {}: {}", msg.offset, value_str);
    }
    
    if messages.len() > 5 {
        println!("  ... and {} more messages", messages.len() - 5);
    }
    
    // Test 2: Consume from multiple partitions
    println!("\n📥 Test 2: Consuming from all partitions");
    println!("{}", "-".repeat(70));
    
    let mut total_consumed = 0;
    for partition in 0..12 {
        let messages = consume_messages(&mut stream, partition, 0, 20).await?;
        if !messages.is_empty() {
            total_consumed += messages.len();
            println!("  📊 Partition {}: {} messages", partition, messages.len());
        }
    }
    
    println!("\n  ✅ Total messages consumed: {}", total_consumed);
    
    // Test 3: Consume with offset
    println!("\n📥 Test 3: Consuming from specific offsets");
    println!("{}", "-".repeat(70));
    
    let messages = consume_messages(&mut stream, 0, 5, 5).await?;
    println!("  📊 Messages from partition 0, offset 5+: {}", messages.len());
    
    for msg in &messages {
        let value_str = String::from_utf8_lossy(&msg.value);
        println!("  📬 Offset {}: {}", msg.offset, value_str.chars().take(50).collect::<String>());
    }
    
    // Summary
    println!("\n{}", "=".repeat(70));
    println!("✅ Consumer Test Complete!");
    println!("   Total messages consumed: {}", total_consumed);
    
    Ok(())
}

#[derive(Debug)]
struct ConsumedMessage {
    offset: u64,
    value: Vec<u8>,
}

async fn consume_messages(
    stream: &mut TcpStream,
    partition: u32,
    offset: u64,
    count: u32,
) -> Result<Vec<ConsumedMessage>> {
    // Send request: partition (u32) + offset (u64) + count (u32)
    stream.write_u32(partition).await?;
    stream.write_u64(offset).await?;
    stream.write_u32(count).await?;
    
    // Read message count (u32)
    let msg_count = stream.read_u32().await?;
    
    let mut messages = Vec::new();
    for _ in 0..msg_count {
        // Read offset (u64)
        let msg_offset = stream.read_u64().await?;
        
        // Read length (u32)
        let msg_len = stream.read_u32().await?;
        
        // Read data
        let mut data = vec![0u8; msg_len as usize];
        stream.read_exact(&mut data).await?;
        
        messages.push(ConsumedMessage {
            offset: msg_offset,
            value: data,
        });
    }
    
    Ok(messages)
}
