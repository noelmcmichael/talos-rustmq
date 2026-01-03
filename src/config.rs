use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub partitions: PartitionsConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub producer_addr: String,
    pub consumer_addr: String,
    pub metrics_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_dir: PathBuf,
    pub log_segment_size: usize,
    pub index_interval: u64,
    pub flush_interval_ms: u64,
    pub flush_messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionsConfig {
    pub count: u32,
    pub replication_factor: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub mmap_size: usize,
    pub batch_size: usize,
    pub io_threads: usize,
}

impl Default for Config {
    fn default() -> Self {
        // Read data directory from environment variable, fall back to default
        let data_dir = std::env::var("RUSTMQ_DATA_DIR")
            .unwrap_or_else(|_| "/var/lib/rustmq".to_string());
        
        Self {
            server: ServerConfig {
                producer_addr: "0.0.0.0:9092".to_string(),
                consumer_addr: "0.0.0.0:9093".to_string(),
                metrics_addr: "0.0.0.0:9094".to_string(),
            },
            storage: StorageConfig {
                data_dir: PathBuf::from(data_dir),
                log_segment_size: 1024 * 1024 * 1024, // 1 GB
                index_interval: 4096,
                flush_interval_ms: 100,
                flush_messages: 10000,
            },
            partitions: PartitionsConfig {
                count: 12,
                replication_factor: 1,
            },
            performance: PerformanceConfig {
                mmap_size: 1024 * 1024 * 1024, // 1 GB
                batch_size: 1000,
                io_threads: 4,
            },
        }
    }
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
    
    pub fn to_file(&self, path: &str) -> anyhow::Result<()> {
        let contents = toml::to_string_pretty(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }
}
