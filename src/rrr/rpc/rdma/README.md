# Native RDMA Support for RRR Framework

This document describes the native RDMA support added to the mako RRR framework, enabling high-performance intra-datacenter communication while maintaining TCP for cross-datacenter replication.

## Table of Contents

1. [RDMA Cloud Solutions Across WAN](#1-rdma-cloud-solutions-across-wan)
2. [Rationale Behind Hybrid Architecture](#2-rationale-behind-hybrid-architecture)
3. [Why Alibaba eRDMA](#3-why-alibaba-erdma)
4. [Why Two-Sided RDMA](#4-why-two-sided-rdma)
5. [Architecture Changes](#5-architecture-changes)
6. [Performance Numbers](#6-performance-numbers)
7. [Future Work](#7-future-work)

---

## 1. RDMA Cloud Solutions Across WAN

Here's the current state of cloud vendor support:

| Provider | Technology | WAN Support | Notes |
|----------|------------|-------------|-------|
| **Azure** | RoCEv2 | ❌ Limited | Requires lossless fabric with PFC; doesn't scale in WAN. No documentation for cross-region support and cross VNET support |
| **AWS** | EFA | ⚠️ Partial | Recently added [cross-subnet support](https://aws.amazon.com/about-aws/whats-new/2024/07/elastic-fabric-adapter-cross-subnet-communication/) (July 2024). Unclear if it works across VPCs. |
| **Google** | Falcon | ❌ Not ready | No working reference implementations available. |
| **Alibaba** | eRDMA | ⚠️ Partial | Claims cross-VPC support across availability zones. Performance for cross-VPC not well documented. |

**Conclusion**: RDMA over WAN is a fundamentally hard problem.  No cloud vendor provides complete, reliable RDMA over WAN support that would give us confidence to fully migrate the replication layer away from TCP.

---

## 2. Rationale Behind Hybrid Architecture

Since there are no reliable solutions for RDMA over WAN, we implemented a **hybrid architecture**:

- **Intra-DC (Same Datacenter)**: Use RDMA for low-latency, high-throughput communication
- **Cross-DC (Different Datacenters)**: Use TCP for reliable WAN communication

This approach achieves the best of both worlds:
- **Performance**: RDMA provides ~76% higher throughput within a datacenter
- **Reliability**: TCP handles the inherent challenges of WAN communication
- **Flexibility**: The architecture can easily switch to full RDMA when cloud vendors mature

The recent trends from AWS and Alibaba show a clear push towards RDMA over WAN, so we designed the architecture to be flexible and Connections can easily switch between RDMA and TCP based on the environment variables.

---

## 3. Why Alibaba eRDMA

We chose Alibaba Cloud's eRDMA for the following reasons:

| Feature | Alibaba eRDMA | AWS EFA | Azure RoCEv2 |
|---------|---------------|---------|--------------|
| Cross-VPC Support | ✅ Claimed | ✅ Claimed ([AWS EFA Cross-Subnet Announcement](https://aws.amazon.com/about-aws/whats-new/2024/07/elastic-fabric-adapter-cross-subnet-communication/)) | ❌ No |
| Native Verbs API | ✅ Yes | ❌ Custom libfabric | ✅ Yes |
| PFC-free Congestion Control | ✅ HPCC | ✅ Custom protocol | ❌ Requires PFC |
| Reference Implementations that use RDMA | ✅ bRPC | ⚠️ Limited | ✅ eRPC |
| Cost | ✅ Cheapest | $$$ | $$$ |

### Key Technical Advantages

**High Precision Congestion Control (HPCC)**: Unlike traditional RoCEv2 which requires Priority Flow Control (PFC) for lossless networks, eRDMA uses In-Network Telemetry (INT) and HPCC for congestion control. This makes cloud deployment significantly easier. It also uses DDP (Direct Data Placement) which utilizes full bandwidth of the network

**References**:
- [HPCC Paper](https://liyuliang001.github.io/publications/hpcc.pdf)
- [eRDMA Paper](https://www.semanticscholar.org/paper/An-efficient-cloud-based-elastic-RDMA-protocol-for-Cao-Xu/f859617475dd0493609ae005e4a43b7db2868591)
- [bRPC](https://github.com/apache/brpc/tree/release-1.15)

---

## 4. Why Two-Sided RDMA

We chose **two-sided RDMA** (Send/Receive) over one-sided RDMA (Read/Write) for the following reasons:

| Aspect | Two-Sided RDMA | One-Sided RDMA |
|--------|----------------|----------------|
| **Complexity** | Reasonable | Very high. Requires algorithm redesign |
| **Paxos Changes** | None required | Would need significant changes |
| **Integration Effort** | Days | Months |
| **CPU Involvement** | Receiver CPU notified | Receiver CPU bypassed |

One-sided RDMA would require fundamental changes to the Paxos algorithm involvement. Two-sided RDMA behaves like TCP (send/receive semantics), making it a drop-in replacement at the transport layer.

**Key Principle**: Upstream components (Paxos, Raft, rpcbench, etc.) need not care about the underlying transport with Two-sided RDMA

---

## 5. Architecture Changes

### 5.1 Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `MAKO_REPLICATION_TRANSPORT` | Transport type: `tcp` or `rdma` | `rdma` |
| `MAKO_LOCAL_DC_IPS` | Comma-separated IPs of same-DC peers | `10.1.0.14,10.1.0.16` |

**Transport Selection Logic**:
```
if MAKO_REPLICATION_TRANSPORT == "rdma" AND target_ip IN MAKO_LOCAL_DC_IPS:
    use RDMA
else:
    use TCP
```

For Paxos deployments, `MAKO_LOCAL_DC_IPS` is automatically set based on the `datacenter` section in the YAML config file.

### 5.2 YAML Configuration (Optional)

```yaml
# config/1c1s3r3p-dc.yml
datacenter:
  Virginia: [localhost, p2]    # Leader and first follower
  California: [p3]             # Second follower (cross-DC, uses TCP)
```

### 5.3 Directory Structure

```
src/rrr/rpc/rdma/
├── rdma_helper.h/cpp           # Global RDMA initialization, device management
├── block_pool.h/cpp            # Pre-registered memory pool
├── rdma_endpoint.h/cpp         # Core RDMA endpoint (QP, CQ, flow control)
├── rdma_server_connection.h/cpp # Server-side RDMA connection wrapper
└── README.md                   # This file
```

### 5.4 Key Components

#### rdma_helper
- `GlobalRdmaInitializeOrDie()` - Initialize RDMA subsystem at startup
- `RegisterMemoryForRdma()` - Register memory regions with NIC
- `GetGlobalPd()` - Get protection domain for QP creation

#### block_pool
- Pre-allocates registered memory for RDMA operations
- Thread-safe allocation/deallocation
- Avoids per-message memory registration overhead

#### rdma_endpoint
- **Queue Pair (QP)**: One per connection, handles send/receive
- **Completion Queue (CQ)**: Notifies when operations complete
- **Flow Control**: Sliding window algorithm (derived from bRPC)
- **Handshake**: TCP-based capability exchange before RDMA data transfer

### 5.5 Connection Flow

```
Client                                Server
   |                                     |
   |-------- TCP Connect --------------->|
   |                                     |
   |<------- TCP Accept -----------------|
   |                                     |
   |-------- HelloMessage (RDMA caps) -->|  (over TCP)
   |                                     |
   |<------- HelloMessage (RDMA caps) ---|  (over TCP)
   |                                     |
   |======== RDMA Send =================>|  (QP now active)
   |<======= RDMA Receive ===============| 
```

### 5.6 Key Design Decisions

1. **CQ integrated with epoll**: Completion channel FD is added to the existing epoll loop
2. **Async Send/Receive**: RdmaEndpoint class handles RDMA operations directly, not the epoll thread to send and recv work requests
3. **Flow Control**: Sliding window with piggybacked ACKs in immediate data
4. **Memory Management**: Single copy from Marshal to registered RDMA buffers
5. **Completion Notifications**: Used to track and release registered memory, send Acks and call RPC callbacks

### 5.7 Modified Files

| File | Changes |
|------|---------|
| `src/rrr/rpc/transport_config.h` | Added `MAKO_LOCAL_DC_IPS` support, `ShouldUseRdma()` |
| `src/rrr/rpc/client.cpp` | Transport selection based on `ShouldUseRdma()` |
| `src/rrr/rpc/server.cpp` | Accept creates `RdmaServerConnection` or `ServerConnection` |
| `src/deptran/config.cc` | Parse `datacenter` YAML section |
| `src/mako/benchmarks/benchmark_config.cpp` | `initSameDcIPs()` sets env var from config |

---

## 6. Performance Numbers

Benchmarks using `rpcbench` with 1KB messages.

### Test Topology

| Node | IP | Location |
|------|-----|----------|
| Server | 10.1.0.14 | Virginia DC |
| Client 1 | 10.1.0.16 | Virginia DC (same-VPC) |
| Client 2 | 10.2.0.124 | California DC (cross-DC, different VPC) |

### Same-DC: RDMA vs TCP

| Transport | Improvement |
|-----------|-------------|
| **RDMA** |  **+50-75%** |
| TCP | baseline |

**RDMA (Same-DC)**:
```bash
# Server
ecs-user@mako-eRDMA002:~/mako$ MAKO_LOCAL_DC_IPS=10.1.0.16 MAKO_REPLICATION_TRANSPORT=rdma ./build/rpcbench -s 0.0.0.0:8848

# Client
ecs-user@iZ0xi1p2dkpx2m5pmkxurhZ:~/mako$ MAKO_LOCAL_DC_IPS=10.1.0.14 MAKO_REPLICATION_TRANSPORT=rdma ./build/rpcbench -c 10.1.0.14:8848 -w 1 -t 1 | grep qps
qps: 165887
qps: 161284
qps: 177089
qps: 176058
qps: 176796
qps: 161142
qps: 175449
qps: 161250
avg qps: 169369.38
```

**TCP (Same-DC)**:
```bash
# Server
ecs-user@mako-eRDMA002:~/mako$ MAKO_REPLICATION_TRANSPORT=tcp ./build/rpcbench -s 0.0.0.0:8848

# Client
ecs-user@iZ0xi1p2dkpx2m5pmkxurhZ:~/mako$ MAKO_REPLICATION_TRANSPORT=tcp ./build/rpcbench -c 10.1.0.14:8848 -w 1 -t 1 | grep qps
qps: 95998
qps: 96370
qps: 96088
qps: 96700
qps: 96626
qps: 95942
qps: 95945
qps: 97115
avg qps: 96348.00
```

### Hybrid Mode: Simultaneous RDMA + TCP Clients

Demonstrates that RRR Server can handle both transports concurrently:

```bash
# Server (Virginia)
ecs-user@mako-eRDMA002:~/mako$ MAKO_LOCAL_DC_IPS=10.1.0.16 MAKO_REPLICATION_TRANSPORT=rdma ./build/rpcbench -s 0.0.0.0:8848
D [/home/ecs-user/mako/src/rrr/rpc/server.cpp:522] 2025-12-07 16:19:49.312 | server@0.0.0.0:8848 got new client from 10.1.0.16, fd=7
I [/home/ecs-user/mako/src/rrr/rpc/transport_config.h:181] 2025-12-07 16:19:49.312 | [RRR] Same-DC IPs (MAKO_LOCAL_DC_IPS):
I [/home/ecs-user/mako/src/rrr/rpc/transport_config.h:183] 2025-12-07 16:19:49.312 | [RRR]   - 10.1.0.16
I [/home/ecs-user/mako/src/rrr/rpc/server.cpp:531] 2025-12-07 16:19:49.312 | Creating RdmaServerConnection for same-DC client 10.1.0.16 (fd=7)
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_server_connection.cpp:15] 2025-12-07 16:19:49.312 | RdmaServerConnection: created with ctrl_socket=7 (handshake pending)
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:91] 2025-12-07 16:19:49.312 | RdmaEndpoint: initialized (SERVER, resources deferred)
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_server_connection.cpp:40] 2025-12-07 16:19:49.312 | RdmaServerConnection: starting handshake thread for fd=7
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_server_connection.cpp:76] 2025-12-07 16:19:49.312 | RdmaServerConnection: Handshake thread started for fd=7
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:720] 2025-12-07 16:19:49.312 | RdmaEndpoint::AcceptFrom: accepting RDMA connection on fd=7
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:579] 2025-12-07 16:19:49.312 | Starting server handshake (60s timeout)
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:587] 2025-12-07 16:19:49.312 | Waiting for client HelloMessage...
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:606] 2025-12-07 16:19:49.312 | Received HelloMessage: remote_qp=1005, sq=256, rq=256
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:611] 2025-12-07 16:19:49.312 | Server: allocating RDMA resources after HelloMessage
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:189] 2025-12-07 16:19:49.312 | RdmaEndpoint: cached lkey=64000
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:191] 2025-12-07 16:19:49.312 | RdmaEndpoint: allocated QP num=1872, SQ=256, RQ=256
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:193] 2025-12-07 16:19:49.312 | RdmaEndpoint: CQ=0x7f29bde01000, QP=0x7f29bde16000, comp_channel fd=8
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:656] 2025-12-07 16:19:49.312 | Sending HelloMessage: qp_num=1872, sq=256, rq=256
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_endpoint.cpp:680] 2025-12-07 16:19:49.413 | Server handshake complete (took 100 ms)
I [/home/ecs-user/mako/src/rrr/rpc/rdma/rdma_server_connection.cpp:95] 2025-12-07 16:19:49.413 | RdmaServerConnection: Adding to poll with RDMA FD

D [/home/ecs-user/mako/src/rrr/rpc/server.cpp:522] 2025-12-07 16:19:51.317 | server@0.0.0.0:8848 got new client from 10.2.0.124, fd=9
I [/home/ecs-user/mako/src/rrr/rpc/server.cpp:537] 2025-12-07 16:19:51.317 | Creating TCP ServerConnection for cross-DC client 10.2.0.124 (fd=9)

# Client 1 (Virginia - uses RDMA)
ecs-user@iZ0xi1p2dkpx2m5pmkxurhZ:~/mako$ MAKO_LOCAL_DC_IPS=10.1.0.14 MAKO_REPLICATION_TRANSPORT=rdma ./build/rpcbench -c 10.1.0.14:8848 -w 1 -t 1 | grep qps
qps: 177697
qps: 143078
qps: 122113
qps: 157061
qps: 174086
qps: 151447
qps: 164030
qps: 168605
avg qps: 157264.62

# Client 2 (California - uses TCP, no MAKO_LOCAL_DC_IPS set)
ecs-user@mako-eRDMA-California:~/mako$ ./build/rpcbench -c 10.1.0.14:8848 -w 1 -t 1 | grep qps
qps: 12000
qps: 12210
qps: 12053
qps: 13000
qps: 12131
qps: 12420
qps: 12580
qps: 12000
avg qps: 12299.25  # Limited by WAN latency (~80ms RTT)
```

---

## TPC-C Replication Metrics (Mako `dbtest`)

Benchmarks conducted with **1 shard** and **4 replicas**  
Deployment: Leader + P2 on VM1, P1 + Learner on VM2

---

### TCP Baseline (Same Datacenter)

**Topology**
- **VM1**: Leader, P2  
- **VM2**: P1, Learner  

#### Replication Metrics

| Metric | Value |
|------|------|
| Memory Delta | 3631.47 MB |
| Commits | 804,082 |
| Memory Delta Rate | 56.86 MB/s |
| Logical Memory Delta | 50.94 MB |
| Logical Delta Rate | 0.80 MB/s |
| Throughput | 12,589 ops/s |
| Avg Throughput / Core | 12,589 ops/s |
| Persist Throughput | 12,589 ops/s |
| Avg Latency | 0.077 ms |
| Abort Rate | 0 aborts/s |

#### Transaction Latencies (Local)

| Transaction | Commit Latency |
|------------|----------------|
| NewOrder | 0.090 ms |
| Payment | 0.052 ms |
| Delivery | 0.245 ms |
| OrderStatus | 0.036 ms |
| StockLevel | 0.100 ms |

---

### RDMA (Same Datacenter)

**Topology**
- **VM1**: Leader, P2  
- **VM2**: P1, Learner  

#### Replication Metrics

| Metric | Value |
|------|------|
| Runtime | 41.42 sec |
| Memory Delta | 3474.61 MB |
| Commits | 1,639,160 |
| Memory Delta Rate | 83.88 MB/s |
| Logical Memory Delta | 103.85 MB |
| Logical Delta Rate | 2.51 MB/s |
| Throughput | 39,571 ops/s |
| Avg Throughput / Core | 39,571 ops/s |
| Persist Throughput | 39,571 ops/s |
| Avg Latency | 0.022 ms |
| Abort Rate | 0 aborts/s |

#### Transaction Latencies (Local)

| Transaction | Commit Latency |
|------------|----------------|
| NewOrder | 0.020 ms |
| Payment | 0.019 ms |
| Delivery | 0.084 ms |
| OrderStatus | 0.005 ms |
| StockLevel | 0.063 ms |

---

### Summary Comparison

| Metric | TCP | RDMA | Improvement |
|------|-----|------|-------------|
| Throughput | 12,589 ops/s | 39,571 ops/s | **3.14×** |
| Avg Latency | 0.077 ms | 0.022 ms | **3.5× lower** |
| Memory Delta Rate | 56.8 MB/s | 83.9 MB/s | **1.47×** |
| Logical Delta Rate | 0.80 MB/s | 2.51 MB/s | **3.1×** |

---

## 7. Future Work

### a) Memory Optimization
Currently, we copy data from `Marshal` to registered RDMA buffers. This decouples the write path to allow `Marshal` reuse. Future optimization to use Marshall directly with RDMA buffers.

### b) Flow Control Tuning
Optimize send/receive queue sizes and flow control parameters based on real-world Paxos workloads.

### c) Full WAN RDMA
When cloud vendors provide reliable RDMA over WAN, the architecture can easily switch by simply adding cross-DC IPs to `MAKO_LOCAL_DC_IPS`.

### d) Error Handling
Implement automatic fallback to TCP on RDMA connection failures.

---

## Quick Start

```bash
# Build with RDMA support
cmake -DRDMA_REPLICATION=ON ..
make -j32

# Run server (RDMA enabled for same-DC clients)
MAKO_LOCAL_DC_IPS=<client_ip> MAKO_REPLICATION_TRANSPORT=rdma ./build/rpcbench -s 0.0.0.0:8848

# Run client (RDMA to same-DC server)
MAKO_LOCAL_DC_IPS=<server_ip> MAKO_REPLICATION_TRANSPORT=rdma ./build/rpcbench -c <server_ip>:8848 -w 1 -t 1
```
