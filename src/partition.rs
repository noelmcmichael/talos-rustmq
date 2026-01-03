use crate::message::{IndexEntry, LogRecord, Message};
use crate::metrics::{MESSAGES_PRODUCED, BYTES_PRODUCED};
use anyhow::{Context, Result};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Configuration for a partition
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    pub partition_id: u32,
    pub data_dir: PathBuf,
    pub segment_size: usize,
    pub index_interval: u64,
    pub flush_interval_ms: u64,
    pub flush_messages: u64,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            partition_id: 0,
            data_dir: PathBuf::from("/var/lib/rustmq"),
            segment_size: 1024 * 1024 * 1024, // 1 GB
            index_interval: 4096,              // Every 4096 messages
            flush_interval_ms: 100,            // 100ms
            flush_messages: 10000,             // Or 10K messages
        }
    }
}

/// Partition represents an append-only log
pub struct Partition {
    config: PartitionConfig,
    log_file: Arc<RwLock<File>>,
    index_file: Arc<RwLock<File>>,
    current_offset: Arc<AtomicU64>,
    messages_since_flush: Arc<AtomicU64>,
    write_buffer: Arc<RwLock<Vec<u8>>>,
}

impl Partition {
    pub async fn open(config: PartitionConfig) -> Result<Self> {
        // Create data directory
        std::fs::create_dir_all(&config.data_dir)?;
        
        // Open log file
        let log_path = config.data_dir.join(format!("partition-{}.log", config.partition_id));
        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)
            .context("Failed to open log file")?;
        
        // Open index file
        let index_path = config.data_dir.join(format!("partition-{}.index", config.partition_id));
        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&index_path)
            .context("Failed to open index file")?;
        
        // Determine starting offset by reading last entry
        let current_offset = Self::recover_offset(&log_file)?;
        
        info!(
            partition_id = config.partition_id,
            current_offset = current_offset,
            "Partition opened"
        );
        
        Ok(Self {
            config,
            log_file: Arc::new(RwLock::new(log_file)),
            index_file: Arc::new(RwLock::new(index_file)),
            current_offset: Arc::new(AtomicU64::new(current_offset)),
            messages_since_flush: Arc::new(AtomicU64::new(0)),
            write_buffer: Arc::new(RwLock::new(Vec::with_capacity(1024 * 1024))), // 1 MB buffer
        })
    }
    
    /// Append a message to the partition
    pub async fn append(&self, msg: &Message) -> Result<u64> {
        // Get next offset
        let offset = self.current_offset.fetch_add(1, Ordering::SeqCst);
        
        // Create log record
        let record = LogRecord::from_message(offset, msg);
        let bytes = bincode::serialize(&record)?;
        
        // Write to buffer
        {
            let mut buffer = self.write_buffer.write().await;
            buffer.extend_from_slice(&bytes);
        }
        
        // Track messages since flush
        let msgs_since_flush = self.messages_since_flush.fetch_add(1, Ordering::SeqCst) + 1;
        
        // Maybe flush
        if msgs_since_flush >= self.config.flush_messages {
            self.flush().await?;
        }
        
        // Update sparse index
        if offset % self.config.index_interval == 0 {
            self.write_index_entry(offset).await?;
        }
        
        // Update metrics
        MESSAGES_PRODUCED.inc();
        BYTES_PRODUCED.inc_by(msg.size_bytes() as f64);
        
        debug!(
            partition_id = self.config.partition_id,
            offset = offset,
            size = bytes.len(),
            "Message appended"
        );
        
        Ok(offset)
    }
    
    /// Append multiple messages in a batch (optimized for throughput)
    pub async fn append_batch(&self, messages: &[Message]) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut offsets = Vec::with_capacity(messages.len());
        let mut total_bytes = 0u64;
        
        // Get starting offset for batch (single atomic operation)
        let start_offset = self.current_offset.fetch_add(messages.len() as u64, Ordering::SeqCst);
        
        // Pre-serialize all messages (outside of lock)
        let mut serialized_records = Vec::with_capacity(messages.len());
        for (i, msg) in messages.iter().enumerate() {
            let offset = start_offset + i as u64;
            offsets.push(offset);
            
            let record = LogRecord::from_message(offset, msg);
            let bytes = bincode::serialize(&record)?;
            total_bytes += bytes.len() as u64;
            serialized_records.push(bytes);
        }
        
        // Write all serialized data to buffer in one lock acquisition
        {
            let mut buffer = self.write_buffer.write().await;
            for bytes in &serialized_records {
                buffer.extend_from_slice(bytes);
            }
        }
        
        // Track messages since flush (single atomic operation)
        let msgs_since_flush = self.messages_since_flush.fetch_add(messages.len() as u64, Ordering::SeqCst) + messages.len() as u64;
        
        // Maybe flush
        if msgs_since_flush >= self.config.flush_messages {
            self.flush().await?;
        }
        
        // Update sparse index for messages at index interval
        for &offset in &offsets {
            if offset % self.config.index_interval == 0 {
                self.write_index_entry(offset).await?;
            }
        }
        
        // Update metrics (batch operation)
        MESSAGES_PRODUCED.inc_by(messages.len() as f64);
        BYTES_PRODUCED.inc_by(total_bytes as f64);
        
        debug!(
            partition_id = self.config.partition_id,
            batch_size = messages.len(),
            start_offset = start_offset,
            total_bytes = total_bytes,
            "Batch appended"
        );
        
        Ok(offsets)
    }
    
    /// Flush write buffer to disk
    pub async fn flush(&self) -> Result<()> {
        let mut buffer = self.write_buffer.write().await;
        
        if buffer.is_empty() {
            return Ok(());
        }
        
        // Write buffer to file
        {
            let mut file = self.log_file.write().await;
            file.write_all(&buffer)?;
            file.sync_data()?; // fsync
        }
        
        // Clear buffer
        buffer.clear();
        
        // Reset counter
        self.messages_since_flush.store(0, Ordering::SeqCst);
        
        debug!(
            partition_id = self.config.partition_id,
            "Flushed to disk"
        );
        
        Ok(())
    }
    
    /// Read messages starting from an offset
    pub async fn read(&self, start_offset: u64, max_bytes: usize) -> Result<Vec<Message>> {
        // First, flush any pending writes
        self.flush().await?;
        
        // Find starting position using index
        let position = self.find_position(start_offset).await?;
        
        // Read from file
        let file = self.log_file.read().await;
        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;
        
        if position >= file_size {
            return Ok(Vec::new());
        }
        
        // Memory-map the file for fast reading
        let mmap = unsafe { memmap2::Mmap::map(&*file)? };
        let slice = &mmap[position..];
        
        // Deserialize messages
        let mut messages = Vec::new();
        let mut bytes_read = 0;
        let mut cursor = 0;
        
        while bytes_read < max_bytes && cursor < slice.len() {
            match bincode::deserialize::<LogRecord>(&slice[cursor..]) {
                Ok(record) => {
                    if record.offset >= start_offset {
                        messages.push(record.to_message());
                    }
                    cursor += record.length as usize;
                    bytes_read += record.length as usize;
                }
                Err(_) => break,
            }
        }
        
        debug!(
            partition_id = self.config.partition_id,
            start_offset = start_offset,
            messages_read = messages.len(),
            "Messages read"
        );
        
        Ok(messages)
    }
    
    /// Write an index entry
    async fn write_index_entry(&self, offset: u64) -> Result<()> {
        let position = {
            let file = self.log_file.read().await;
            file.metadata()?.len()
        };
        
        let entry = IndexEntry { offset, position };
        let bytes = bincode::serialize(&entry)?;
        
        let mut index_file = self.index_file.write().await;
        index_file.write_all(&bytes)?;
        
        Ok(())
    }
    
    /// Find byte position for an offset using binary search on index
    async fn find_position(&self, offset: u64) -> Result<usize> {
        let index_file = self.index_file.read().await;
        let metadata = index_file.metadata()?;
        let index_size = metadata.len() as usize;
        
        if index_size == 0 {
            return Ok(0);
        }
        
        // Memory-map index file
        let mmap = unsafe { memmap2::Mmap::map(&*index_file)? };
        
        // Binary search through index entries
        let entry_size = std::mem::size_of::<u64>() * 2; // offset + position
        let num_entries = index_size / entry_size;
        
        let mut left = 0;
        let mut right = num_entries;
        let mut best_position = 0;
        
        while left < right {
            let mid = (left + right) / 2;
            let entry_offset = mid * entry_size;
            
            if entry_offset + entry_size > mmap.len() {
                break;
            }
            
            let entry: IndexEntry = bincode::deserialize(&mmap[entry_offset..entry_offset + entry_size])?;
            
            if entry.offset <= offset {
                best_position = entry.position as usize;
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        
        Ok(best_position)
    }
    
    /// Recover the last offset from the log file
    fn recover_offset(file: &File) -> Result<u64> {
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            return Ok(0);
        }
        
        // Read last 1 KB of file
        let read_size = 1024.min(metadata.len() as usize);
        let mmap = unsafe { memmap2::Mmap::map(file)? };
        let total_len = mmap.len();
        
        if total_len == 0 {
            return Ok(0);
        }
        
        let start = total_len.saturating_sub(read_size);
        let slice = &mmap[start..];
        
        // Try to deserialize last record
        let mut last_offset = 0;
        let mut cursor = 0;
        
        while cursor < slice.len() {
            match bincode::deserialize::<LogRecord>(&slice[cursor..]) {
                Ok(record) => {
                    last_offset = record.offset;
                    cursor += record.length as usize;
                }
                Err(_) => break,
            }
        }
        
        Ok(last_offset + 1) // Next offset to use
    }
    
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_partition_append_and_read() {
        let dir = tempdir().unwrap();
        let config = PartitionConfig {
            partition_id: 0,
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        
        let partition = Partition::open(config).await.unwrap();
        
        // Append messages
        let msg1 = Message::new(b"key1".to_vec(), b"value1".to_vec());
        let msg2 = Message::new(b"key2".to_vec(), b"value2".to_vec());
        
        let offset1 = partition.append(&msg1).await.unwrap();
        let offset2 = partition.append(&msg2).await.unwrap();
        
        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        
        // Read messages
        let messages = partition.read(0, 1024 * 1024).await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].value, b"value1");
        assert_eq!(messages[1].value, b"value2");
    }
    
    #[tokio::test]
    async fn test_partition_batch_append() {
        let dir = tempdir().unwrap();
        let config = PartitionConfig {
            partition_id: 0,
            data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        
        let partition = Partition::open(config).await.unwrap();
        
        // Create batch
        let messages: Vec<Message> = (0..100)
            .map(|i| Message::new(
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            ))
            .collect();
        
        // Append batch
        let offsets = partition.append_batch(&messages).await.unwrap();
        assert_eq!(offsets.len(), 100);
        
        // Read back
        let read_messages = partition.read(0, 1024 * 1024).await.unwrap();
        assert_eq!(read_messages.len(), 100);
    }
}
