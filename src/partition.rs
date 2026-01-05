use crate::message::{IndexEntry, LogRecord, Message};
use crate::metrics::{MESSAGES_PRODUCED, BYTES_PRODUCED};
use anyhow::{anyhow, Context, Result};

use std::fs::{File, OpenOptions};
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

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
        
        // EXPERT FIX: Open files with read+write (not append-only)
        let log_path = config.data_dir.join(format!("partition-{}.log", config.partition_id));
        let mut log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)  // Changed from append to write
            .open(&log_path)
            .context("Failed to open log file")?;
        
        let index_path = config.data_dir.join(format!("partition-{}.index", config.partition_id));
        let mut index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)  // Changed from append to write
            .open(&index_path)
            .context("Failed to open index file")?;
        
        // EXPERT DIAGNOSTIC: Undeniable log BEFORE recovery
        let log_len = log_file.metadata()?.len();
        let index_len = index_file.metadata()?.len();
        info!(
            partition_id = config.partition_id,
            log_path = %log_path.display(),
            index_path = %index_path.display(),
            log_len,
            index_len,
            "ABOUT TO RUN recover_by_scanning()"
        );
        
        // EXPERT FIX: Proper recovery by scanning from byte 0
        let recovered = Self::recover_by_scanning(
            &mut log_file,
            &mut index_file,
            config.index_interval,
            config.partition_id,
        )?;
        let current_offset = recovered.current_offset;
        
        // Seek to end for appending
        log_file.seek(SeekFrom::End(0))?;
        
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
        
        // EXPERT DIAGNOSTIC: Log position calculation
        info!(
            partition_id = self.config.partition_id,
            start_offset,
            max_bytes,
            position,
            file_size,
            "read(): computed start position"
        );
        
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
                        bytes_read += record.length as usize;  // BUG FIX: Only count included messages
                    }
                    cursor += record.length as usize;  // Always advance cursor
                }
                Err(e) => {
                    // EXPERT DIAGNOSTIC: Show WHERE and WHY deserialization fails
                    error!(
                        partition_id = self.config.partition_id,
                        start_offset,
                        cursor,
                        position,
                        file_size = slice.len(),
                        error = ?e,
                        "read(): LogRecord deserialize failed - THIS IS THE WALL"
                    );
                    break;
                }
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
    
    /// EXPERT FIX: Proper recovery by scanning from byte 0
    /// Rebuilds index, enforces monotonic offsets, truncates corrupted tail
    fn recover_by_scanning(
        log_file: &mut File,
        index_file: &mut File,
        index_interval: u64,
        partition_id: u32,
    ) -> Result<RecoveredState> {
        // EXPERT DIAGNOSTIC: Undeniable log at function entry
        let log_len = log_file.metadata()?.len();
        let index_len = index_file.metadata()?.len();
        info!(
            partition_id,
            log_len,
            index_len,
            "recover_by_scanning() ENTERED"
        );
        
        // Ensure we're at start of file
        log_file.seek(SeekFrom::Start(0))?;
        
        if log_len == 0 {
            info!(partition_id, "Empty log file, returning current_offset=0");
            return Ok(RecoveredState {
                current_offset: 0,
                index_entries: Vec::new(),
                last_good_pos: 0,
            });
        }
        
        let mmap = unsafe { memmap2::Mmap::map(&*log_file)? };
        let mut cur = Cursor::new(&mmap[..]);
        
        let mut last_good_pos: u64 = 0;
        let mut last_offset: Option<u64> = None;
        let mut index_entries: Vec<IndexEntry> = Vec::new();
        let mut record_count: u64 = 0;
        
        // Scan from byte 0, tracking exact boundaries
        while (cur.position() as usize) < mmap.len() {
            let pos_before = cur.position();
            
            match bincode::deserialize_from::<_, LogRecord>(&mut cur) {
                Ok(rec) => {
                    let pos_after = cur.position();
                    
                    // CRITICAL: Enforce monotonic offsets
                    if let Some(prev) = last_offset {
                        if rec.offset <= prev {
                            error!(
                                prev_offset = prev,
                                bad_offset = rec.offset,
                                position = pos_before,
                                "Non-monotonic offset detected - truncating here"
                            );
                            break;
                        }
                    }
                    
                    last_good_pos = pos_after;
                    last_offset = Some(rec.offset);
                    record_count += 1;
                    
                    // Rebuild index during scan
                    if record_count % index_interval == 0 {
                        index_entries.push(IndexEntry {
                            offset: rec.offset,
                            position: pos_before,
                        });
                    }
                }
                Err(e) => {
                    // EXPERT DIAGNOSTIC: Show first 32 bytes if this is first failure
                    if pos_before == 0 {
                        let hex_dump: String = mmap.iter()
                            .take(32.min(mmap.len()))
                            .map(|b| format!("{:02x}", b))
                            .collect::<Vec<_>>()
                            .join(" ");
                        error!(
                            partition_id,
                            position = pos_before,
                            file_len = mmap.len(),
                            error = ?e,
                            first_32_bytes = %hex_dump,
                            "Decode failed at pos=0 - wrong file or wrong type?"
                        );
                    } else {
                        error!(
                            partition_id,
                            position = pos_before,
                            file_len = mmap.len(),
                            error = ?e,
                            "Decode failed during recovery - truncating tail"
                        );
                    }
                    break;
                }
            }
        }
        
        // EXPERT FIX: Drop mmap before truncating
        let need_truncate = last_good_pos as usize != mmap.len();
        let file_len = mmap.len();
        drop(mmap);  // Drop BEFORE any file operations
        
        // Truncate if needed
        if need_truncate {
            warn!(
                partition_id,
                from = file_len,
                to = last_good_pos,
                truncated_bytes = file_len as u64 - last_good_pos,
                "Truncating log tail to last valid record"
            );
            log_file.set_len(last_good_pos)?;
            log_file.sync_data()?;
        }
        
        // Rebuild index file
        index_file.set_len(0)?;  // Truncate existing index
        index_file.seek(SeekFrom::Start(0))?;
        
        for entry in &index_entries {
            let bytes = bincode::serialize(entry)?;
            index_file.write_all(&bytes)?;
        }
        index_file.sync_data()?;
        
        let current_offset = last_offset.map(|o| o + 1).unwrap_or(0);
        
        // EXPERT DIAGNOSTIC: Must-appear completion log
        info!(
            partition_id,
            records_scanned = record_count,
            last_offset = ?last_offset,
            current_offset,
            last_good_pos,
            index_entries = index_entries.len(),
            log_len = file_len,
            "recover_by_scanning() COMPLETE"
        );
        
        Ok(RecoveredState {
            current_offset,
            index_entries,
            last_good_pos,
        })
    }
    
    pub fn current_offset(&self) -> u64 {
        self.current_offset.load(Ordering::SeqCst)
    }
}

/// Recovery state from scanning log file
struct RecoveredState {
    current_offset: u64,
    index_entries: Vec<IndexEntry>,
    last_good_pos: u64,
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
