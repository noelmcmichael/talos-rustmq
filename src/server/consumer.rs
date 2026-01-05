use crate::partition::Partition;
use crate::metrics::{MESSAGES_CONSUMED, BYTES_CONSUMED};
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// ConsumerServer serves messages to consumers via TCP
pub struct ConsumerServer {
    addr: String,
    partitions: Vec<Arc<Partition>>,
}

impl ConsumerServer {
    pub fn new(addr: String, partitions: Vec<Arc<Partition>>) -> Self {
        Self { addr, partitions }
    }
    
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!(addr = %self.addr, "Consumer server listening");
        
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!(client = %addr, "Consumer connected");
                    let partitions = self.partitions.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_consumer_connection(socket, partitions).await {
                            error!(client = %addr, error = %e, "Consumer connection error");
                        }
                        info!(client = %addr, "Consumer disconnected");
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}

async fn handle_consumer_connection(
    mut socket: TcpStream,
    partitions: Vec<Arc<Partition>>,
) -> Result<()> {
    loop {
        // Read request: [partition_id:u32][start_offset:u64][max_bytes:u32]
        let partition_id = match socket.read_u32().await {
            Ok(id) => id,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Client disconnected
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        };
        
        let start_offset = socket.read_u64().await?;
        let max_bytes = socket.read_u32().await?;
        
        // DEBUG: Log every request (for connection reuse investigation)
        debug!(
            partition_id = partition_id,
            start_offset = start_offset,
            max_bytes = max_bytes,
            "Consumer request received"
        );
        
        // Validate partition ID
        if partition_id as usize >= partitions.len() {
            warn!(partition_id = partition_id, "Invalid partition ID");
            socket.write_u32(0).await?; // Empty response
            continue;
        }
        
        // Fetch messages
        let partition = &partitions[partition_id as usize];
        let messages = match partition.read(start_offset, max_bytes as usize).await {
            Ok(msgs) => msgs,
            Err(e) => {
                error!(error = %e, "Failed to read from partition");
                socket.write_u32(0).await?; // Empty response
                continue;
            }
        };
        
        // Serialize response
        let response_bytes = bincode::serialize(&messages)?;
        
        // Update metrics
        let message_count = messages.len();
        let total_bytes: usize = messages.iter().map(|m| m.size_bytes()).sum();
        MESSAGES_CONSUMED.inc_by(message_count as f64);
        BYTES_CONSUMED.inc_by(total_bytes as f64);
        
        // DEBUG: Log response details
        debug!(
            partition_id = partition_id,
            messages_returned = message_count,
            bytes_returned = response_bytes.len(),
            "Consumer response sent"
        );
        
        // Send: [length:u32][messages]
        socket.write_u32(response_bytes.len() as u32).await?;
        socket.write_all(&response_bytes).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Message;
    use crate::partition::PartitionConfig;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_consumer_server() {
        let dir = tempdir().unwrap();
        
        // Create partition with some data
        let config = PartitionConfig {
            partition_id: 0,
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let partition = Arc::new(Partition::open(config).await.unwrap());
        
        // Write messages
        for i in 0..10 {
            let msg = Message::new(
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            );
            partition.append(&msg).await.unwrap();
        }
        partition.flush().await.unwrap();
        
        // Start server
        let server = ConsumerServer::new("127.0.0.1:19093".to_string(), vec![partition]);
        
        tokio::spawn(async move {
            server.run().await.unwrap();
        });
        
        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Connect client
        let mut client = TcpStream::connect("127.0.0.1:19093").await.unwrap();
        
        // Request: partition 0, offset 0, max 1 MB
        client.write_u32(0).await.unwrap(); // partition_id
        client.write_u64(0).await.unwrap(); // start_offset
        client.write_u32(1024 * 1024).await.unwrap(); // max_bytes
        
        // Read response
        let response_len = client.read_u32().await.unwrap();
        let mut response_bytes = vec![0u8; response_len as usize];
        client.read_exact(&mut response_bytes).await.unwrap();
        
        let messages: Vec<Message> = bincode::deserialize(&response_bytes).unwrap();
        assert_eq!(messages.len(), 10);
    }
}
