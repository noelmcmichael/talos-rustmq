// Kubernetes-deployable test producer for RustMQ
// Reads configuration from environment variables

use anyhow::Result;
use rustmq::message::Message;
use std::env;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 RustMQ Producer Test (Kubernetes)");
    println!("{}", "=".repeat(70));
    
    // Read configuration from environment
    let server_host = env::var("RUSTMQ_HOST").unwrap_or_else(|_| "rustmq-0.rustmq.kafka.svc.cluster.local".to_string());
    let server_port = env::var("RUSTMQ_PORT").unwrap_or_else(|_| "31092".to_string());
    let num_messages = env::var("NUM_MESSAGES")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .unwrap_or(100);
    let message_size = env::var("MESSAGE_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()
        .unwrap_or(100);
    
    let addr = format!("{}:{}", server_host, server_port);
    
    println!("Configuration:");
    println!("  Server: {}", addr);
    println!("  Messages: {}", num_messages);
    println!("  Message size: {} bytes", message_size);
    println!();
    
    println!("Connecting to {}...", addr);
    let mut stream = match TcpStream::connect(&addr).await {
        Ok(s) => {
            println!("✅ Connected to RustMQ producer server\n");
            s
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to {}: {}", addr, e);
            std::process::exit(1);
        }
    };
    
    // Test 1: Send sample messages
    println!("📤 Test 1: Sending {} test messages", num_messages.min(10));
    println!("{}", "-".repeat(70));
    
    for i in 0..num_messages.min(10) {
        let key = format!("test-key-{}", i).into_bytes();
        let value = format!("{{\"test\": \"message_{}\", \"partition\": {}, \"timestamp\": {}}}", 
                           i, i % 12, chrono::Utc::now().timestamp()).into_bytes();
        
        let message = Message::new(key, value);
        match send_message(&mut stream, message).await {
            Ok(offset) => println!("  ✅ Sent message {}, offset: {}", i, offset),
            Err(e) => {
                eprintln!("  ❌ Failed to send message {}: {}", i, e);
                continue;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    
    // Test 2: Throughput test
    if num_messages > 10 {
        println!("\n📤 Test 2: Throughput test ({} messages)", num_messages);
        println!("{}", "-".repeat(70));
        
        let start = Instant::now();
        let mut success_count = 0;
        let mut error_count = 0;
        
        for i in 0..num_messages {
            let key = format!("key-{}", i).into_bytes();
            let value = "x".repeat(message_size).into_bytes();
            
            let message = Message::new(key, value);
            match send_message(&mut stream, message).await {
                Ok(_) => success_count += 1,
                Err(e) => {
                    error_count += 1;
                    if error_count <= 5 {
                        eprintln!("  ⚠️  Error sending message {}: {}", i, e);
                    }
                }
            }
            
            if i % 100 == 0 && i > 0 {
                println!("  Progress: {}/{} messages sent...", i, num_messages);
            }
        }
        
        let elapsed = start.elapsed();
        let throughput = success_count as f64 / elapsed.as_secs_f64();
        let bandwidth = (success_count * message_size) as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;
        
        println!("\n  ✅ Sent {} messages in {:.2?}", success_count, elapsed);
        println!("  ❌ Errors: {}", error_count);
        println!("  📊 Throughput: {:.1} msg/s", throughput);
        println!("  📊 Bandwidth: {:.2} MB/s", bandwidth);
        
        // Summary
        println!("\n{}", "=".repeat(70));
        println!("✅ Producer Test Complete!");
        println!("   Success: {} messages", success_count);
        println!("   Errors: {} messages", error_count);
        println!("   Success rate: {:.1}%", (success_count as f64 / num_messages as f64) * 100.0);
        println!("   Average throughput: {:.1} msg/s", throughput);
        println!("   Average bandwidth: {:.2} MB/s", bandwidth);
    }
    
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
