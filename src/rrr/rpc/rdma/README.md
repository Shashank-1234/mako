# RRR RDMA Support

Native RDMA support for RRR RPC layer.

## Directory Structure

```
src/rrr/rpc/rdma/
├── rdma_helper.h/cpp      # Global RDMA initialization, device management
├── block_pool.h/cpp       # Memory registration pool (pre-registered buffers)
├── rdma_endpoint.h/cpp    # Core RDMA endpoint (QP, CQ, flow control)
└── README.md              # This file
```

## Components

### 1. rdma_helper (✅ Implemented)

**Purpose**: Global RDMA initialization and device management

**Key Functions**:
- `GlobalRdmaInitializeOrDie()` - Initialize RDMA subsystem (call at startup)
- `RegisterMemoryForRdma()` - Register user memory for RDMA
- `GetMemoryLKey()` - Get lkey for registered memory
- `GetRdmaDeviceInfo()` - Query device capabilities (LID, GID, etc.)

**Usage**:
```cpp
#include "rrr/rpc/rdma/rdma_helper.h"

// At startup
rrr::rdma::GlobalRdmaInitializeOrDie();

// Check availability
if (rrr::rdma::IsRdmaAvailable()) {
    // Use RDMA
}
```

### 2. block_pool (✅ Implemented)

**Purpose**: Pre-registered memory pool for zero-copy RDMA

**Key Features**:
- Pre-allocates 1GB registered memory at initialization
- Extends automatically if pool exhausted (up to 16 regions)
- Block sizes: 4KB, 16KB, 128KB (configurable)
- Thread-safe allocation/deallocation

**Usage**:
```cpp
#include "rrr/rpc/rdma/block_pool.h"

// Initialize with registration callback
rrr::rdma::InitBlockPool(rrr::rdma::RegisterMemoryForRdma);

// Allocate registered memory
void* buf = rrr::rdma::AllocBlock(4096);  // 4KB block
uint32_t lkey = rrr::rdma::GetRegionId(buf);  // Get lkey

// Use for RDMA...

// Deallocate
rrr::rdma::DeallocBlock(buf);
```

### 3. rdma_endpoint (⚠️ ~85% Implemented)

**Purpose**: RDMA connection endpoint with QP, CQ, and flow control

**Implemented**:
- ✅ Resource allocation (QP, CQ, buffers)
- ✅ QP state transitions (RESET → INIT → RTR → RTS)
- ✅ **TCP handshake (HelloMessage protocol)**
- ✅ `ConnectTo` (client-side handshake)
- ✅ `AcceptFrom` (server-side handshake)
- ✅ Send with flow control (`Write`)
- ✅ Completion handling (`HandleCompletionEvents`)
- ✅ Flow control (sliding window + ACK piggybacking)
- ✅ Epoll integration (completion channel FD)

**TODO**:
- ❌ Receive queue management (`Read`)
- ❌ Message deserialization
- ❌ Error handling and reconnection

**Design**:
```cpp
// Client side
RdmaEndpoint ep;
ep.Initialize();
ep.ConnectTo("10.0.0.1", 9000);  // TODO: Implement

// Send
std::shared_ptr<Marshallable> msg = ...;
ssize_t sent = ep.Write(msg);

// Receive (in event loop)
int fd = ep.GetCompletionFd();
// epoll_wait on fd...
ep.HandleCompletionEvents();
```

## Memory Management with RustyCpp

All memory management follows RRR's RustyCpp patterns:

```cpp
// ✅ CORRECT: Use smart pointers
class RdmaEndpoint {
    std::vector<void*> send_buffers_;                    // Raw RDMA buffers (OK)
    std::vector<std::shared_ptr<Marshallable>> sbuf_;    // Smart pointers for objects
};

ssize_t Write(std::shared_ptr<Marshallable> data) {
    sbuf_[sq_current_] = data;  // Keep alive until send completion
    // ...
    ibv_post_send(...);
}

void HandleSendCompletion(ibv_wc& wc) {
    sbuf_[sq_idx].reset();  // Release → auto-deallocates if last reference
}
```

## Build Configuration

```cmake
# CMakeLists.txt
option(RDMA_REPLICATION "Enable RDMA support in RRR" OFF)

if(RDMA_REPLICATION)
    add_definitions(-DRDMA_REPLICATION)
    find_package(Verbs REQUIRED)
    
    # Add RDMA sources
    set(RRR_RDMA_SOURCES
        src/rrr/rpc/rdma/rdma_helper.cpp
        src/rrr/rpc/rdma/block_pool.cpp
        src/rrr/rpc/rdma/rdma_endpoint.cpp
    )
    
    target_sources(rrr PRIVATE ${RRR_RDMA_SOURCES})
    target_link_libraries(rrr ibverbs)
endif()
```

## Testing

```bash
# Build with RDMA support
cmake -DRDMA_REPLICATION=ON ..
make -j32

# Test basic initialization
./test_rdma_init

# Test connection (TODO)
./test_rdma_connection
```

## Next Steps

### Phase 1: ~~Complete Handshake~~ ✅ **DONE**
1. ~~Implement `DoClientHandshake`~~ ✅ Implemented
2. ~~Implement `DoServerHandshake`~~ ✅ Implemented  
3. ~~Handshake protocol~~ ✅ MAGIC "RDMA" + HelloMessage

### Phase 2: Receive Path (Priority: HIGH - NEXT)
1. Implement receive queue management
2. Deserialize from recv buffers to Marshallable
3. Implement `Read()` method

### Phase 3: Integration with Client/Server (Priority: MEDIUM)
1. Create `Endpoint` base class (abstraction)
2. Implement `TcpEndpoint` (existing socket code)
3. Implement `RdmaEndpoint` deriving from `Endpoint`
4. Update `Client`/`Server` to use `Endpoint*` polymorphically

### Phase 4: Testing & Hardening (Priority: MEDIUM)
1. Unit tests for handshake, send/recv, flow control
2. Integration tests with RRR RPC
3. Error handling, reconnection, fallback to TCP
4. Performance benchmarks

## References

- Design doc: `/home/ecs-user/mako-rdma/docs/README.migration.md`
- RRR docs: `/home/ecs-user/mako/doc/`

## Key Design Principles

1. **Memory must be pre-registered** - Use block_pool for all RDMA buffers
2. **Flow control is explicit** - Track window_size, send ACKs
3. **Repost receives immediately** - Or receive queue exhausts
4. **Handshake over TCP** - RDMA for data, TCP for connection setup
5. **One lkey per region** - Not per allocation (efficient!)
6. **Use memcpy for now** - Can optimize to zero-copy scatter-gather later
