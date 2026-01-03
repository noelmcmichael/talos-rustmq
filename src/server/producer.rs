use crate::message::Message;
use crate::router::MessageRouter;
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, warn};

/// ProducerServer accepts messages from producers via TCP
pub struct ProducerServer {
    addr: String,
    router: Arc<MessageRouter>,
}

impl ProducerServer {
    pub fn new(addr: String, router: Arc<MessageRouter>) -> Self {
        Self { addr, router }
    }
    
    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!(addr = %self.addr, "Producer server listening");
        
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    info!(client = %addr, "Producer connected");
                    let router = self.router.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = handle_producer_connection(socket, router).await {
                            error!(client = %addr, error = %e, "Producer connection error");
                        }
                        info!(client = %addr, "Producer disconnected");
                    });
                }
                Err(e) => {
                    error!(error = %e, "Failed to accept connection");
                }
            }
        }
    }
}

async fn handle_producer_connection(
    mut socket: TcpStream,
    router: Arc<MessageRouter>,
) -> Result<()> {
    loop {
        // Read first u32 which is either message length (single) or batch size (batch)
        let first_u32 = match socket.read_u32().await {
            Ok(len) => len,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // Client disconnected
                return Ok(());
            }
            Err(e) => {
                return Err(e.into());
            }
        };
        
        // Check if this is a batch request (magic number: 0xBEEF0001 = 3203096577)
        // If not magic, treat as single message length
        if first_u32 == 0xBEEF0001 {
            handle_batch_request(&mut socket, router.clone()).await?;
        } else {
            handle_single_request(&mut socket, router.clone(), first_u32).await?;
        }
    }
}

async fn handle_single_request(
    socket: &mut TcpStream,
    router: Arc<MessageRouter>,
    msg_len: u32,
) -> Result<()> {
    // Sanity check
    if msg_len > 10 * 1024 * 1024 {
        // 10 MB max message size
        warn!(msg_len = msg_len, "Message too large, rejecting");
        socket.write_u64(u64::MAX).await?; // Error code
        return Ok(());
    }
    
    // Read message bytes
    let mut msg_buf = vec![0u8; msg_len as usize];
    socket.read_exact(&mut msg_buf).await?;
    
    // Deserialize message
    let msg: Message = match bincode::deserialize(&msg_buf) {
        Ok(msg) => msg,
        Err(e) => {
            error!(error = %e, "Failed to deserialize message");
            socket.write_u64(u64::MAX).await?; // Error code
            return Ok(());
        }
    };
    
    // Route to partition
    let offset = match router.route(msg).await {
        Ok(offset) => offset,
        Err(e) => {
            error!(error = %e, "Failed to route message");
            socket.write_u64(u64::MAX).await?; // Error code
            return Ok(());
        }
    };
    
    // Send ack (offset)
    socket.write_u64(offset).await?;
    
    Ok(())
}

async fn handle_batch_request(
    socket: &mut TcpStream,
    router: Arc<MessageRouter>,
) -> Result<()> {
    // Read batch size
    let batch_size = socket.read_u32().await?;
    
    // Sanity check
    if batch_size == 0 || batch_size > 10000 {
        // Max 10K messages per batch
        warn!(batch_size = batch_size, "Invalid batch size");
        socket.write_u32(0).await?; // Error: 0 offsets returned
        return Ok(());
    }
    
    // Read all messages in batch
    let mut messages = Vec::with_capacity(batch_size as usize);
    
    for _ in 0..batch_size {
        let msg_len = socket.read_u32().await?;
        
        if msg_len > 10 * 1024 * 1024 {
            warn!(msg_len = msg_len, "Message in batch too large");
            socket.write_u32(0).await?; // Error
            return Ok(());
        }
        
        let mut msg_buf = vec![0u8; msg_len as usize];
        socket.read_exact(&mut msg_buf).await?;
        
        let msg: Message = match bincode::deserialize(&msg_buf) {
            Ok(msg) => msg,
            Err(e) => {
                error!(error = %e, "Failed to deserialize message in batch");
                socket.write_u32(0).await?; // Error
                return Ok(());
            }
        };
        
        messages.push(msg);
    }
    
    // Route entire batch
    let offsets = match router.route_batch(messages).await {
        Ok(offsets) => offsets,
        Err(e) => {
            error!(error = %e, "Failed to route batch");
            socket.write_u32(0).await?; // Error
            return Ok(());
        }
    };
    
    // Send batch size and all offsets
    socket.write_u32(offsets.len() as u32).await?;
    for offset in offsets {
        socket.write_u64(offset).await?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{Partition, PartitionConfig};
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_producer_server() {
        let dir = tempdir().unwrap();
        
        // Create partition
        let config = PartitionConfig {
            partition_id: 0,
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let partition = Arc::new(Partition::open(config).await.unwrap());
        
        // Create router
        let router = Arc::new(MessageRouter::new(vec![partition]));
        
        // Start server
        let server = ProducerServer::new("127.0.0.1:19092".to_string(), router);
        
        tokio::spawn(async move {
            server.run().await.unwrap();
        });
        
        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Connect client
        let mut client = TcpStream::connect("127.0.0.1:19092").await.unwrap();
        
        // Send message
        let msg = Message::new(b"test-key".to_vec(), b"test-value".to_vec());
        let msg_bytes = bincode::serialize(&msg).unwrap();
        
        client.write_u32(msg_bytes.len() as u32).await.unwrap();
        client.write_all(&msg_bytes).await.unwrap();
        
        // Read ack
        let offset = client.read_u64().await.unwrap();
        assert_eq!(offset, 0);
    }
}
