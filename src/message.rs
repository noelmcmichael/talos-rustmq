use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Message is the core data structure sent by producers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    /// Routing key (used for partition selection)
    pub key: Vec<u8>,
    
    /// Message payload
    pub value: Vec<u8>,
    
    /// Timestamp in microseconds since epoch
    pub timestamp: u64,
    
    /// Optional metadata headers
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

impl Message {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value,
            timestamp: current_timestamp_micros(),
            headers: Vec::new(),
        }
    }
    
    pub fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }
    
    pub fn size_bytes(&self) -> usize {
        self.key.len() + self.value.len() + 8 + // timestamp
        self.headers.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
    }
}

/// LogRecord is the on-disk format for messages
#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
    /// Total record length (for seeking)
    pub length: u32,
    
    /// Offset within partition
    pub offset: u64,
    
    /// Message timestamp
    pub timestamp: u64,
    
    /// Key length
    pub key_len: u16,
    
    /// Key bytes
    pub key: Vec<u8>,
    
    /// Value bytes
    pub value: Vec<u8>,
    
    /// Headers (optional)
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

impl LogRecord {
    pub fn from_message(offset: u64, msg: &Message) -> Self {
        let key_len = msg.key.len() as u16;
        let record = Self {
            length: 0, // Will be set after serialization
            offset,
            timestamp: msg.timestamp,
            key_len,
            key: msg.key.clone(),
            value: msg.value.clone(),
            headers: msg.headers.clone(),
        };
        
        // Calculate length
        let serialized = bincode::serialize(&record).unwrap();
        let mut record_with_length = record;
        record_with_length.length = serialized.len() as u32;
        record_with_length
    }
    
    pub fn to_message(&self) -> Message {
        Message {
            key: self.key.clone(),
            value: self.value.clone(),
            timestamp: self.timestamp,
            headers: self.headers.clone(),
        }
    }
}

/// IndexEntry points to a message offset in the log file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Message offset
    pub offset: u64,
    
    /// Byte position in log file
    pub position: u64,
}

pub fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_message_serialization() {
        let msg = Message::new(
            b"test-key".to_vec(),
            b"test-value".to_vec(),
        );
        
        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: Message = bincode::deserialize(&serialized).unwrap();
        
        assert_eq!(msg.key, deserialized.key);
        assert_eq!(msg.value, deserialized.value);
    }
    
    #[test]
    fn test_log_record_conversion() {
        let msg = Message::new(
            b"key".to_vec(),
            b"value".to_vec(),
        );
        
        let record = LogRecord::from_message(42, &msg);
        assert_eq!(record.offset, 42);
        assert_eq!(record.key, msg.key);
        assert_eq!(record.value, msg.value);
        
        let converted = record.to_message();
        assert_eq!(converted.key, msg.key);
        assert_eq!(converted.value, msg.value);
    }
}
