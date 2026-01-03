use crate::message::Message;
use crate::partition::Partition;
use ahash::AHasher;
use anyhow::Result;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tracing::debug;

/// MessageRouter routes messages to partitions based on key hash
pub struct MessageRouter {
    partitions: Vec<Arc<Partition>>,
}

impl MessageRouter {
    pub fn new(partitions: Vec<Arc<Partition>>) -> Self {
        Self { partitions }
    }
    
    /// Route a message to the appropriate partition
    pub async fn route(&self, msg: Message) -> Result<u64> {
        let partition_id = self.select_partition(&msg.key);
        let partition = &self.partitions[partition_id];
        
        let offset = partition.append(&msg).await?;
        
        debug!(
            partition_id = partition_id,
            offset = offset,
            key_len = msg.key.len(),
            value_len = msg.value.len(),
            "Message routed"
        );
        
        Ok(offset)
    }
    
    /// Route multiple messages (preserves order of offsets)
    pub async fn route_batch(&self, messages: Vec<Message>) -> Result<Vec<u64>> {
        // Group messages by partition, tracking original indices
        let mut partitioned: Vec<Vec<(usize, Message)>> = vec![Vec::new(); self.partitions.len()];
        
        for (idx, msg) in messages.into_iter().enumerate() {
            let partition_id = self.select_partition(&msg.key);
            partitioned[partition_id].push((idx, msg));
        }
        
        // Append to each partition and collect offsets with their original indices
        let mut indexed_offsets = Vec::new();
        
        for (partition_id, msgs_with_idx) in partitioned.into_iter().enumerate() {
            if !msgs_with_idx.is_empty() {
                let partition = &self.partitions[partition_id];
                
                // Extract just the messages for append
                let msgs: Vec<Message> = msgs_with_idx.iter().map(|(_, msg)| msg.clone()).collect();
                let offsets = partition.append_batch(&msgs).await?;
                
                // Pair each offset with its original index
                for ((idx, _), offset) in msgs_with_idx.into_iter().zip(offsets.into_iter()) {
                    indexed_offsets.push((idx, offset));
                }
            }
        }
        
        // Sort by original index to restore order
        indexed_offsets.sort_by_key(|(idx, _)| *idx);
        
        // Extract just the offsets in original order
        Ok(indexed_offsets.into_iter().map(|(_, offset)| offset).collect())
    }
    
    /// Select partition using consistent hashing
    fn select_partition(&self, key: &[u8]) -> usize {
        let hash = Self::hash_key(key);
        (hash as usize) % self.partitions.len()
    }
    
    /// Fast hash function (AHash)
    fn hash_key(key: &[u8]) -> u32 {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        hasher.finish() as u32
    }
    
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
    
    pub fn get_partition(&self, partition_id: usize) -> Option<&Arc<Partition>> {
        self.partitions.get(partition_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::PartitionConfig;
    use tempfile::tempdir;
    
    #[tokio::test]
    async fn test_router_distribution() {
        let dir = tempdir().unwrap();
        
        // Create 4 partitions
        let mut partitions = Vec::new();
        for i in 0..4 {
            let config = PartitionConfig {
                partition_id: i,
                data_dir: dir.path().join(format!("partition-{}", i)),
                ..Default::default()
            };
            let partition = Arc::new(Partition::open(config).await.unwrap());
            partitions.push(partition);
        }
        
        let router = MessageRouter::new(partitions.clone());
        
        // Send 100 messages
        for i in 0..100 {
            let msg = Message::new(
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            );
            router.route(msg).await.unwrap();
        }
        
        // Flush all partitions
        for partition in &partitions {
            partition.flush().await.unwrap();
        }
        
        // Check distribution (should be roughly even)
        let mut counts = Vec::new();
        for partition in &partitions {
            let offset = partition.current_offset();
            counts.push(offset);
        }
        
        println!("Distribution: {:?}", counts);
        
        // All partitions should have some messages
        assert!(counts.iter().all(|&c| c > 0));
        
        // Total should be 100
        assert_eq!(counts.iter().sum::<u64>(), 100);
    }
    
    #[tokio::test]
    async fn test_router_batch() {
        let dir = tempdir().unwrap();
        
        // Create 4 partitions
        let mut partitions = Vec::new();
        for i in 0..4 {
            let config = PartitionConfig {
                partition_id: i,
                data_dir: dir.path().join(format!("partition-{}", i)),
                ..Default::default()
            };
            let partition = Arc::new(Partition::open(config).await.unwrap());
            partitions.push(partition);
        }
        
        let router = MessageRouter::new(partitions);
        
        // Create batch
        let messages: Vec<Message> = (0..1000)
            .map(|i| Message::new(
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            ))
            .collect();
        
        // Route batch
        let offsets = router.route_batch(messages).await.unwrap();
        assert_eq!(offsets.len(), 1000);
    }
}
