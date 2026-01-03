# RustMQ - Custom Message Queue for Talos Cluster

High-performance message queue written in Rust, deployed to Talos Kubernetes cluster.

## Overview

RustMQ is a custom message queue implementation designed for low-latency, high-throughput data streaming:

- **Custom Protocol:** TCP-based with bincode serialization (NOT Kafka-compatible)
- **Partitioned Storage:** 12 partitions for parallel processing
- **Memory-Mapped I/O:** For fast persistence
- **Prometheus Metrics:** Built-in observability
- **Kubernetes-Native:** Designed for StatefulSet deployment

## Architecture

```
Producer (TCP 9092) → RustMQ → Consumer (TCP 9093)
                         ↓
                   Metrics (HTTP 9094)
```

## Deployment

### Current Configuration (Minimal)
- **Replicas:** 1 (testing configuration)
- **Storage:** 10Gi per replica
- **Resources:**
  - CPU: 500m request, 1000m limit
  - Memory: 1Gi request, 2Gi limit

### Services
- `rustmq-headless`: StatefulSet DNS (ClusterIP: None)
- `rustmq`: Internal service (ClusterIP)
- `rustmq-metrics`: Prometheus scraping (ClusterIP)
- `rustmq-external`: External access (LoadBalancer with BGP)

### External Access
LoadBalancer service with BGP label `bgp: blue`:
- Producer port: 9092
- Consumer port: 9093
- Metrics port: 9094

VIP assigned by Cilium, advertised via BGP to FRR ToR.

## Client Usage

RustMQ uses a custom protocol. Do NOT use Kafka clients (rdkafka, etc.).

### Producer Example

```rust
use rustmq::message::Message;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn send_message(stream: &mut TcpStream, message: Message) -> Result<u64> {
    // Serialize message
    let msg_bytes = bincode::serialize(&message)?;
    let msg_len = msg_bytes.len() as u32;
    
    // Send length (u32 big-endian)
    stream.write_u32(msg_len).await?;
    
    // Send message bytes
    stream.write_all(&msg_bytes).await?;
    
    // Read offset response (u64 big-endian)
    let offset = stream.read_u64().await?;
    
    Ok(offset)
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut stream = TcpStream::connect("rustmq-0.rustmq-headless.data-pipeline.svc.cluster.local:9092").await?;
    
    let message = Message::new(
        b"my-key".to_vec(),
        b"my-value".to_vec()
    );
    
    let offset = send_message(&mut stream, message).await?;
    println!("Message written at offset: {}", offset);
    
    Ok(())
}
```

### Consumer Example

See `examples/k8s-test-consumer.rs` for full implementation.

## CI/CD Pipeline

**GitOps Workflow:**
1. Push to `main` branch
2. GitHub Actions builds Docker image (AMD64)
3. Pushes to Harbor registry
4. Updates `k8s/01-statefulset.yaml` with new image tag
5. ArgoCD detects change and auto-syncs

## Monitoring

Prometheus metrics available at port 9094:
- Message throughput
- Partition statistics
- Write latency
- Consumer lag

Access metrics via LoadBalancer VIP or internal service.

## Development

### Local Build
```bash
cargo build --release
```

### Docker Build
```bash
docker build -t rustmq:local .
```

### Test
```bash
cargo test
```

## Configuration

Environment variables:
- `RUSTMQ_DATA_DIR`: Data directory path (default: `/data/rustmq`)
- `RUSTMQ_PARTITIONS`: Number of partitions (default: `12`)
- `RUSTMQ_PRODUCER_PORT`: Producer port (default: `9092`)
- `RUSTMQ_CONSUMER_PORT`: Consumer port (default: `9093`)
- `RUSTMQ_METRICS_PORT`: Metrics port (default: `9094`)

## Scaling

To scale to 3 replicas:
```bash
kubectl -n data-pipeline scale statefulset rustmq --replicas=3
```

**Note:** Requires additional storage capacity or EBS-backed PVCs.

## Status

- ✅ Source migrated from POC environment
- ✅ Updated for AMD64 (AWS EC2)
- ✅ Kubernetes manifests ready
- ✅ GitHub Actions CI/CD configured
- ⏳ Awaiting initial deployment

## Links

- **Cluster:** Talos AWS (10.0.1.10)
- **Harbor Registry:** https://harbor.int-talos-poc.pocketcove.net/
- **Namespace:** data-pipeline

---

**Status:** Ready to deploy  
**Platform:** Talos Kubernetes v1.31.2  
**Last Updated:** January 2, 2026
# RustMQ Build
