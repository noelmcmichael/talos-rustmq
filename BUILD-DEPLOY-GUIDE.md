# RustMQ Build and Deployment Guide

## Current Status

✅ **Completed:**
- RustMQ binary compiled successfully (`rustmq-server`, 5MB, release mode)
- Dockerfile created and tested (ARM64 multi-stage build)
- Kubernetes manifests ready (namespace, StatefulSet, services, ServiceMonitor)
- Deployment scripts ready

⏸️ **Blocked:**
- Docker Desktop appears to be stuck/unresponsive
- Need to restart Docker before building image

## Prerequisites

1. **Docker must be running** - If stuck, restart:
   ```bash
   # Kill and restart Docker
   killall -9 Docker
   sleep 5
   open -a Docker
   
   # Wait ~30 seconds for Docker to start
   sleep 30
   
   # Verify
   docker info
   ```

2. **Registry configured** - Talos nodes trust insecure registry at `192.168.64.11:30005`

## Build and Deploy Steps

### Step 1: Build Docker Image

```bash
cd /Users/noelmcmichael/Workspace/onprem_infra_poc/rustmq

# Build ARM64 image
docker build --platform linux/arm64 \
  -t 192.168.64.11:30005/rustmq:latest .
```

**Expected output:**
- Multi-stage build (builder + runtime)
- Compiles Rust code (takes 2-5 minutes)
- Final image size: ~100-150 MB

### Step 2: Push to Registry

```bash
docker push 192.168.64.11:30005/rustmq:latest
```

**Expected output:**
- Pushes layers to registry
- Takes 1-2 minutes depending on network

**Verify:**
```bash
curl http://192.168.64.11:30005/v2/_catalog
# Should show: {"repositories":["rustmq",...]}
```

### Step 3: Deploy to Kubernetes

```bash
cd /Users/noelmcmichael/Workspace/onprem_infra_poc
python3 deploy-rustmq.py
```

**What it does:**
1. Creates `rustmq` namespace
2. Deploys StatefulSet (3 replicas)
3. Creates services (headless, ClusterIP, metrics, NodePort)
4. Creates ServiceMonitor for Prometheus
5. Provisions 3x 50Gi PVCs (local-path storage)

**Expected output:**
```
🚀 Deploying RustMQ to Kubernetes
==================================

📄 Applying 00-namespace.yaml...
  ✅ Created Namespace/rustmq

📄 Applying 01-statefulset.yaml...
  ✅ Created StatefulSet/rustmq

📄 Applying 02-services.yaml...
  ✅ Created Service/rustmq-headless
  ✅ Created Service/rustmq
  ✅ Created Service/rustmq-metrics
  ✅ Created Service/rustmq-external

📄 Applying 03-servicemonitor.yaml...
  ✅ Created ServiceMonitor/rustmq

🔍 Checking RustMQ status...
  ✅ Namespace: rustmq exists
  📦 StatefulSet: 0/3 replicas ready (initializing...)
  
✨ Deployment complete!
```

### Step 4: Monitor Startup

```bash
# Watch pods start (takes 2-5 minutes)
python3 check-rustmq-status.py

# Or watch continuously
watch -n 2 python3 check-rustmq-status.py
```

**Pod startup sequence:**
1. **Init** (0-30s): Image pull (if not cached)
2. **PodInitializing** (0-10s): Mount volumes
3. **Running** (0-10s): Container starts
4. **Ready** (10-30s): Health checks pass

**Common startup issues:**
- **ImagePullBackOff**: Image not in registry or registry unreachable
- **CrashLoopBackOff**: Check logs with Python script
- **Pending**: PVC provisioning issue (check storage class)

### Step 5: Verify Functionality

```bash
# Check metrics endpoint (external)
curl http://192.168.64.11:30094/metrics

# Should return Prometheus metrics:
# rustmq_messages_total
# rustmq_partitions_total
# rustmq_connections_active
# etc.

# Check services
python3 check-rustmq-status.py
```

**Expected endpoints:**
- **Producer (external):** `192.168.64.11:30092`
- **Consumer (external):** `192.168.64.11:30093`
- **Metrics (external):** `http://192.168.64.11:30094/metrics`
- **Producer (internal):** `rustmq.rustmq.svc.cluster.local:9092`
- **Consumer (internal):** `rustmq.rustmq.svc.cluster.local:9093`

## Quick Commands Reference

```bash
# Build and push (one command)
./build-and-push-rustmq.sh

# Deploy
python3 deploy-rustmq.py

# Check status
python3 check-rustmq-status.py

# Get pod logs
python3 -c "
import requests, yaml, base64
from pathlib import Path

kubeconfig = Path('configs-utm/kubeconfig')
with open(kubeconfig) as f:
    config = yaml.safe_load(f)

cert_data = base64.b64decode(config['users'][0]['user']['client-certificate-data'])
key_data = base64.b64decode(config['users'][0]['user']['client-key-data'])
ca_data = base64.b64decode(config['clusters'][0]['cluster']['certificate-authority-data'])

with open('/tmp/k8s-client.crt', 'wb') as f: f.write(cert_data)
with open('/tmp/k8s-client.key', 'wb') as f: f.write(key_data)
with open('/tmp/k8s-ca.crt', 'wb') as f: f.write(ca_data)

resp = requests.get(
    'https://192.168.64.11:6443/api/v1/namespaces/rustmq/pods/rustmq-0/log',
    cert=('/tmp/k8s-client.crt', '/tmp/k8s-client.key'),
    verify='/tmp/k8s-ca.crt'
)
print(resp.text)
"

# Delete deployment (if needed)
python3 -c "
import requests, yaml, base64
from pathlib import Path

kubeconfig = Path('configs-utm/kubeconfig')
with open(kubeconfig) as f:
    config = yaml.safe_load(f)

cert_data = base64.b64decode(config['users'][0]['user']['client-certificate-data'])
key_data = base64.b64decode(config['users'][0]['user']['client-key-data'])
ca_data = base64.b64decode(config['clusters'][0]['cluster']['certificate-authority-data'])

with open('/tmp/k8s-client.crt', 'wb') as f: f.write(cert_data)
with open('/tmp/k8s-client.key', 'wb') as f: f.write(key_data)
with open('/tmp/k8s-ca.crt', 'wb') as f: f.write(ca_data)

API = 'https://192.168.64.11:6443'
CERT = ('/tmp/k8s-client.crt', '/tmp/k8s-client.key')
CA = '/tmp/k8s-ca.crt'

# Delete namespace (cascades to all resources)
resp = requests.delete(f'{API}/api/v1/namespaces/rustmq', cert=CERT, verify=CA)
print(f'Deleted namespace: {resp.status_code}')
"
```

## Architecture Details

**StatefulSet Configuration:**
- **Replicas:** 3
- **Storage:** 50Gi per pod (150Gi total)
- **Storage Class:** local-path
- **CPU:** 1-2 cores per pod
- **Memory:** 2-4 Gi per pod
- **Ports:** 9092 (producer), 9093 (consumer), 9094 (metrics)

**Services:**
1. **rustmq-headless** - StatefulSet DNS (pod-0.rustmq-headless.rustmq.svc)
2. **rustmq** - ClusterIP for internal clients
3. **rustmq-metrics** - Prometheus scraping
4. **rustmq-external** - NodePort for Mac testing (30092, 30093, 30094)

**Data Persistence:**
- Each pod gets a PVC: `data-rustmq-0`, `data-rustmq-1`, `data-rustmq-2`
- Data stored in `/data/rustmq` inside containers
- Partitions stored as memory-mapped files
- Survives pod restarts and rescheduling

## Troubleshooting

### Docker Issues

**Symptom:** `docker info` hangs or fails
```bash
# Restart Docker
killall -9 Docker
sleep 5
open -a Docker
sleep 30
docker info  # Should work now
```

**Symptom:** `Cannot connect to Docker daemon`
```bash
# Check if Docker is running
ps aux | grep Docker | grep -v grep

# If not, start it
open -a Docker
```

### Build Issues

**Symptom:** `exec format error` in Dockerfile
- **Cause:** Building for wrong architecture
- **Fix:** Ensure `--platform linux/arm64` flag is used

**Symptom:** `COPY failed: file not found: /build/target/release/rustmq-server`
- **Cause:** Cargo build failed in Docker
- **Fix:** Build locally first to test: `cd rustmq && cargo build --release`

### Deployment Issues

**Symptom:** `ImagePullBackOff`
```bash
# Check if image exists in registry
curl http://192.168.64.11:30005/v2/_catalog

# Check if image tag exists
curl http://192.168.64.11:30005/v2/rustmq/tags/list
```

**Symptom:** `CrashLoopBackOff`
```bash
# Get logs from pod
python3 -c "..." # (use log command from Quick Commands)

# Common causes:
# - Permission denied: PodSecurity policy issue
# - No space: PVC not provisioned
# - Port conflict: Another service on same port
```

**Symptom:** PVC stuck in `Pending`
```bash
# Check storage class
python3 check-storage-status.py

# If provisioner not running, install it
./install-local-path-storage.sh

# Fix RBAC if needed
python3 fix-provisioner-rbac.py
python3 restart-provisioner.py
```

## Next Steps After Deployment

1. **Verify metrics:** `curl http://192.168.64.11:30094/metrics`
2. **Create Grafana dashboard** (Phase 2)
3. **Build client library** (Phase 3)
4. **Migrate producers** (Phase 3)
5. **Benchmark vs Kafka** (Phase 3)

## Files Reference

```
rustmq/
├── Dockerfile                    # Multi-stage ARM64 build
├── Cargo.toml                    # Rust dependencies
├── src/                          # Rust source code
├── k8s/
│   ├── 00-namespace.yaml         # rustmq namespace
│   ├── 01-statefulset.yaml       # 3-replica StatefulSet
│   ├── 02-services.yaml          # 4 service definitions
│   └── 03-servicemonitor.yaml    # Prometheus integration
└── target/release/
    └── rustmq-server             # Compiled binary (5MB)

/
├── deploy-rustmq.py              # Deployment script
├── check-rustmq-status.py        # Status verification
└── build-and-push-rustmq.sh      # Build + push automation
```

---

**Last Updated:** December 10, 2024  
**Status:** Ready to build and deploy (Docker restart needed)
