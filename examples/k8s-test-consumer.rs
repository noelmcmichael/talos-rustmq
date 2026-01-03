// Kubernetes-deployable test consumer for RustMQ
// Reads configuration from environment variables

use anyhow::Result;
use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    println!("📥 RustMQ Consumer Test (Kubernetes)");
    println!("{}", "=".repeat(70));
    
    // Read configuration from environment
    let server_host = env::var("RUSTMQ_HOST").unwrap_or_else(|_| "rustmq-0.rustmq.kafka.svc.cluster.local".to_string());
    let server_port = env::var("RUSTMQ_PORT").unwrap_or_else(|_| "31093".to_string());
    let start_partition = env::var("START_PARTITION")
        .unwrap_or_else(|_| "0".to_string())
        .parse::<u32>()
        .unwrap_or(0);
    let end_partition = env::var("END_PARTITION")
        .unwrap_or_else(|_| "12".to_string())
        .parse::<u32>()
        .unwrap_or(12);
    let messages_per_partition = env::var("MESSAGES_PER_PARTITION")
        .unwrap_or_else(|_| "20".to_string())
        .parse::<u32>()
        .unwrap_or(20);
    
    let addr = format!("{}:{}", server_host, server_port);
    
    println!("Configuration:");
    println!("  Server: {}", addr);
    println!("  Partitions: {} to {}", start_partition, end_partition - 1);
    println!("  Messages per partition: {}", messages_per_partition);
    println!();
    
    println!("Connecting to {}...", addr);
    let mut stream = match TcpStream::connect(&addr).await {
        Ok(s) => {
            println!("✅ Connected to RustMQ consumer server\n");
            s
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to {}: {}", addr, e);
            std::process::exit(1);
        }
    };
    
    // Test 1: Consume from first partition
    println!("📥 Test 1: Consuming from partition {} (offset 0, count {})", 
             start_partition, messages_per_partition);
    println!("{}", "-".repeat(70));
    
    match consume_messages(&mut stream, start_partition, 0, messages_per_partition).await {
        Ok(messages) => {
            println!("  📊 Received {} messages", messages.len());
            
            for (i, msg) in messages.iter().enumerate().take(5) {
                let value_str = String::from_utf8_lossy(&msg.value);
                let preview = if value_str.len() > 100 {
                    format!("{}...", &value_str[..100])
                } else {
                    value_str.to_string()
                };
                println!("  📬 Offset {}: {}", msg.offset, preview);
            }
            
            if messages.len() > 5 {
                println!("  ... and {} more messages", messages.len() - 5);
            }
        }
        Err(e) => {
            eprintln!("  ❌ Failed to consume messages: {}", e);
        }
    }
    
    // Test 2: Consume from all partitions
    println!("\n📥 Test 2: Consuming from all partitions ({} to {})", 
             start_partition, end_partition - 1);
    println!("{}", "-".repeat(70));
    
    let mut total_consumed = 0;
    let mut total_bytes = 0;
    let mut errors = 0;
    
    for partition in start_partition..end_partition {
        match consume_messages(&mut stream, partition, 0, messages_per_partition).await {
            Ok(messages) => {
                if !messages.is_empty() {
                    let partition_bytes: usize = messages.iter().map(|m| m.value.len()).sum();
                    total_consumed += messages.len();
                    total_bytes += partition_bytes;
                    println!("  📊 Partition {}: {} messages ({} bytes)", 
                             partition, messages.len(), partition_bytes);
                }
            }
            Err(e) => {
                errors += 1;
                eprintln!("  ⚠️  Partition {}: error - {}", partition, e);
            }
        }
    }
    
    println!("\n  ✅ Total messages consumed: {}", total_consumed);
    println!("  📊 Total bytes: {} ({:.2} KB)", total_bytes, total_bytes as f64 / 1024.0);
    if errors > 0 {
        println!("  ⚠️  Errors: {} partitions", errors);
    }
    
    // Test 3: Offset-based consumption
    println!("\n📥 Test 3: Consuming from specific offsets");
    println!("{}", "-".repeat(70));
    
    for partition in start_partition..start_partition.min(end_partition).min(start_partition + 3) {
        match consume_messages(&mut stream, partition, 5, 5).await {
            Ok(messages) => {
                println!("  📊 Partition {}, offset 5+: {} messages", partition, messages.len());
                for msg in messages.iter().take(2) {
                    let value_str = String::from_utf8_lossy(&msg.value);
                    let preview = value_str.chars().take(50).collect::<String>();
                    println!("      Offset {}: {}", msg.offset, preview);
                }
            }
            Err(e) => {
                eprintln!("  ⚠️  Partition {}: error - {}", partition, e);
            }
        }
    }
    
    // Summary
    println!("\n{}", "=".repeat(70));
    println!("✅ Consumer Test Complete!");
    println!("   Total messages consumed: {}", total_consumed);
    println!("   Total bytes: {} ({:.2} KB)", total_bytes, total_bytes as f64 / 1024.0);
    println!("   Partitions checked: {}", end_partition - start_partition);
    if errors > 0 {
        println!("   ⚠️  Errors: {} partitions", errors);
    }
    
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
    // Send request: partition (u32) + offset (u64) + max_bytes (u32)
    // Server expects max_bytes, so calculate approximate:
    // Assume ~100 bytes per message on average
    let max_bytes = count * 100;
    
    stream.write_u32(partition).await?;
    stream.write_u64(offset).await?;
    stream.write_u32(max_bytes).await?;
    
    // Read response: length (u32) + bincode(Vec<Message>)
    let response_len = stream.read_u32().await?;
    let mut response_bytes = vec![0u8; response_len as usize];
    stream.read_exact(&mut response_bytes).await?;
    
    // Deserialize messages
    use serde::Deserialize;
    
    #[derive(Deserialize)]
    struct Message {
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: u64,
        headers: Vec<(String, String)>,
    }
    
    let messages: Vec<Message> = bincode::deserialize(&response_bytes)?;
    
    // Convert to ConsumedMessage format
    // Note: server doesn't include offset in response, so we track it
    let mut result = Vec::new();
    for (i, msg) in messages.into_iter().enumerate() {
        result.push(ConsumedMessage {
            offset: offset + i as u64,
            value: msg.value,
        });
    }
    
    Ok(result)
}
