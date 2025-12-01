// RDMA Helper - Global RDMA initialization and utilities
// Ported from bRPC rdma_helper.h, adapted for RRR

#pragma once

#include <infiniband/verbs.h>
#include <cstdint>
#include <cstddef>

namespace rrr {
namespace rdma {

// Global RDMA initialization (call once at startup)
void GlobalRdmaInitializeOrDie();

// Check if RDMA is available
bool IsRdmaAvailable();

// Get global RDMA resources
ibv_context* GetRdmaContext();
ibv_pd* GetRdmaProtectionDomain();

// Memory registration for user-provided buffers
// Returns lkey (local key) for the registered memory region
// Returns 0 on failure
uint32_t RegisterMemoryForRdma(void* buf, size_t len);
void DeregisterMemoryForRdma(void* buf);

// Get lkey for a buffer (must be from registered pool or RegisterMemoryForRdma)
uint32_t GetMemoryLKey(const void* buf);

// RDMA device information
struct RdmaDeviceInfo {
    uint16_t lid;              // Local ID
    ibv_gid gid;               // Global ID
    uint32_t max_qp_wr;        // Max WRs per QP
    uint32_t max_sge;          // Max SGEs per WR
    uint32_t max_cqe;          // Max CQEs per CQ
};

bool GetRdmaDeviceInfo(RdmaDeviceInfo* info);

} // namespace rdma
} // namespace rrr
