use rustmq::{Config, Partition, PartitionConfig, MessageRouter};
use rustmq::server::{ProducerServer, ConsumerServer};
use rustmq::metrics;
use anyhow::Result;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .init();
    
    info!("Starting RustMQ server...");
    
    // Load configuration
    let config = match std::env::var("RUSTMQ_CONFIG") {
        Ok(path) => {
            info!(config_path = %path, "Loading config from file");
            Config::from_file(&path)?
        }
        Err(_) => {
            info!("Using default configuration");
            Config::default()
        }
    };
    
    // Initialize metrics
    metrics::init_metrics();
    
    // Create partitions
    info!(count = config.partitions.count, "Creating partitions");
    let mut partitions = Vec::new();
    
    for i in 0..config.partitions.count {
        let partition_config = PartitionConfig {
            partition_id: i,
            data_dir: config.storage.data_dir.join(format!("partition-{}", i)),
            segment_size: config.storage.log_segment_size,
            index_interval: config.storage.index_interval,
            flush_interval_ms: config.storage.flush_interval_ms,
            flush_messages: config.storage.flush_messages,
        };
        
        let partition = Arc::new(Partition::open(partition_config).await?);
        partitions.push(partition);
    }
    
    info!(partitions = partitions.len(), "Partitions created");
    
    // Create router
    let router = Arc::new(MessageRouter::new(partitions.clone()));
    
    // Start producer server
    let producer_server = ProducerServer::new(
        config.server.producer_addr.clone(),
        router.clone(),
    );
    
    let producer_handle = tokio::spawn(async move {
        if let Err(e) = producer_server.run().await {
            tracing::error!(error = %e, "Producer server error");
        }
    });
    
    // Start consumer server
    let consumer_server = ConsumerServer::new(
        config.server.consumer_addr.clone(),
        partitions.iter().map(|p| p.clone()).collect(),
    );
    
    let consumer_handle = tokio::spawn(async move {
        if let Err(e) = consumer_server.run().await {
            tracing::error!(error = %e, "Consumer server error");
        }
    });
    
    // Start metrics server
    let metrics_addr = config.server.metrics_addr.clone();
    let metrics_handle = tokio::spawn(async move {
        use warp::Filter;
        
        let metrics_route = warp::path!("metrics").map(|| {
            use prometheus::Encoder;
            let encoder = prometheus::TextEncoder::new();
            let metric_families = metrics::REGISTRY.gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            String::from_utf8(buffer).unwrap()
        });
        
        info!(addr = %metrics_addr, "Metrics server listening");
        warp::serve(metrics_route)
            .run(metrics_addr.parse::<std::net::SocketAddr>().unwrap())
            .await;
    });
    
    info!("RustMQ server started successfully");
    
    // Wait for servers
    tokio::select! {
        _ = producer_handle => info!("Producer server stopped"),
        _ = consumer_handle => info!("Consumer server stopped"),
        _ = metrics_handle => info!("Metrics server stopped"),
    }
    
    Ok(())
}
