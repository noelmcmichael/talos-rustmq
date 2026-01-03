// Kubernetes-deployable batching test producer for RustMQ
// Tests batching performance with configurable batch sizes

use anyhow::Result;
use rustmq::message::Message;
use std::env;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const BATCH_MAGIC: u32 = 0xBEEF0001; // Magic number to indicate batch request

struct BatchProducer {
    stream: TcpStream,
    batch_size: usize,
    enable_batching: bool,
}

impl BatchProducer {
    async fn connect(addr: &str, batch_size: usize, enable_batching: bool) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(BatchProducer {
            stream,
            batch_size,
            enable_batching,
        })
    }

    async fn send_single(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<u64> {
        let message = Message::new(key, value);
        let msg_bytes = bincode::serialize(&message)?;
        let msg_len = msg_bytes.len() as u32;

        // Send length + message
        self.stream.write_u32(msg_len).await?;
        self.stream.write_all(&msg_bytes).await?;
        self.stream.flush().await?; // CRITICAL: flush before reading response

        // Read offset
        let offset = self.stream.read_u64().await?;
        if offset == u64::MAX {
            anyhow::bail!("Server returned error");
        }
        Ok(offset)
    }

    async fn send_batch(&mut self, messages: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        println!("  → Sending batch of {} messages", messages.len());

        // Write magic number
        println!("    Writing magic number: 0x{:08X}", BATCH_MAGIC);
        self.stream.write_u32(BATCH_MAGIC).await?;

        // Write batch size
        println!("    Writing batch size: {}", messages.len());
        self.stream.write_u32(messages.len() as u32).await?;

        // Write each message
        for (i, (key, value)) in messages.iter().enumerate() {
            let message = Message::new(key.clone(), value.clone());
            let msg_bytes = bincode::serialize(&message)?;
            let msg_len = msg_bytes.len() as u32;

            self.stream.write_u32(msg_len).await?;
            self.stream.write_all(&msg_bytes).await?;
            
            if i == 0 || i == messages.len() - 1 {
                println!("    Message {}: {} bytes", i, msg_len);
            }
        }

        // CRITICAL: flush before reading response
        println!("    Flushing TCP buffer...");
        self.stream.flush().await?;

        // Read offsets
        println!("    Waiting for server response...");
        let count = self.stream.read_u32().await? as usize;
        println!("    Server responded with {} offsets", count);
        
        if count != messages.len() {
            anyhow::bail!("Expected {} offsets, got {}", messages.len(), count);
        }

        let mut offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let offset = self.stream.read_u64().await?;
            offsets.push(offset);
        }

        println!("  ✅ Batch complete");
        Ok(offsets)
    }

    async fn send_messages(&mut self, count: usize) -> Result<(usize, usize, f64)> {
        let start = Instant::now();
        let mut success = 0;
        let mut errors = 0;

        if !self.enable_batching {
            // Send one by one
            for i in 0..count {
                let key = format!("key-{}", i).into_bytes();
                let value = format!("value-{}", i).into_bytes();

                match self.send_single(key, value).await {
                    Ok(_) => success += 1,
                    Err(_) => errors += 1,
                }
            }
        } else {
            // Send in batches
            let mut batch = Vec::with_capacity(self.batch_size);
            
            for i in 0..count {
                let key = format!("key-{}", i).into_bytes();
                let value = format!("value-{}", i).into_bytes();
                batch.push((key, value));

                if batch.len() >= self.batch_size || i == count - 1 {
                    match self.send_batch(batch.clone()).await {
                        Ok(_) => success += batch.len(),
                        Err(_) => errors += batch.len(),
                    }
                    batch.clear();
                }
            }
        }

        let elapsed = start.elapsed();
        let throughput = success as f64 / elapsed.as_secs_f64();

        Ok((success, errors, throughput))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 RustMQ Batching Performance Test");
    println!("{}", "=".repeat(70));

    // Read configuration
    let broker = env::var("RUSTMQ_SERVER")
        .or_else(|_| env::var("RUSTMQ_BROKER"))
        .unwrap_or_else(|_| "rustmq.rustmq.svc.cluster.local:9092".to_string());
    
    let message_count = env::var("TEST_MESSAGES")
        .or_else(|_| env::var("MESSAGE_COUNT"))
        .unwrap_or_else(|_| "10000".to_string())
        .parse::<usize>()?;
    
    let batch_size = env::var("RUSTMQ_BATCH_SIZE")
        .unwrap_or_else(|_| "100".to_string())
        .parse::<usize>()?;
    
    let enable_batching = env::var("RUSTMQ_ENABLE_BATCHING")
        .unwrap_or_else(|_| "true".to_string())
        .to_lowercase() == "true";

    println!("Configuration:");
    println!("  Broker: {}", broker);
    println!("  Messages: {}", message_count);
    println!("  Batching: {}", if enable_batching { "enabled" } else { "disabled" });
    println!("  Batch size: {}", if enable_batching { batch_size.to_string() } else { "N/A".to_string() });
    println!();

    // Connect
    println!("Connecting to {}...", broker);
    let mut producer = BatchProducer::connect(&broker, batch_size, enable_batching).await?;
    println!("✅ Connected\n");

    // Run test
    println!("📤 Sending {} messages...", message_count);
    println!("{}", "-".repeat(70));

    let (success, errors, throughput) = producer.send_messages(message_count).await?;

    // Results
    println!();
    println!("{}", "=".repeat(70));
    println!("✅ TEST COMPLETE");
    println!("{}", "=".repeat(70));
    println!("  Successfully produced {} messages in {:.2}s ({:.1} msg/s)", 
             success, success as f64 / throughput, throughput);
    println!("  Errors: {}", errors);
    println!("  Success rate: {:.1}%", (success as f64 / message_count as f64) * 100.0);
    println!();

    if errors == 0 {
        println!("🎉 All messages delivered successfully!");
    } else {
        println!("⚠️  {} messages failed to deliver", errors);
    }

    Ok(())
}
