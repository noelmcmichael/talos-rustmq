use prometheus::{Counter, Histogram, Registry};
use lazy_static::lazy_static;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    
    pub static ref MESSAGES_PRODUCED: Counter = Counter::new(
        "rustmq_messages_produced_total",
        "Total messages produced"
    ).unwrap();
    
    pub static ref BYTES_PRODUCED: Counter = Counter::new(
        "rustmq_bytes_produced_total",
        "Total bytes produced"
    ).unwrap();
    
    pub static ref PRODUCE_LATENCY: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "rustmq_produce_latency_seconds",
            "Produce latency in seconds"
        )
        .buckets(vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0])
    ).unwrap();
    
    pub static ref MESSAGES_CONSUMED: Counter = Counter::new(
        "rustmq_messages_consumed_total",
        "Total messages consumed"
    ).unwrap();
    
    pub static ref BYTES_CONSUMED: Counter = Counter::new(
        "rustmq_bytes_consumed_total",
        "Total bytes consumed"
    ).unwrap();
}

pub fn init_metrics() {
    REGISTRY.register(Box::new(MESSAGES_PRODUCED.clone())).unwrap();
    REGISTRY.register(Box::new(BYTES_PRODUCED.clone())).unwrap();
    REGISTRY.register(Box::new(PRODUCE_LATENCY.clone())).unwrap();
    REGISTRY.register(Box::new(MESSAGES_CONSUMED.clone())).unwrap();
    REGISTRY.register(Box::new(BYTES_CONSUMED.clone())).unwrap();
}
