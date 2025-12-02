// RDMA Helper Implementation
// Ported from bRPC rdma_helper.cpp, adapted for RRR

#include "rdma_helper.h"
#include "block_pool.h"
#include "../../base/logging.hpp"
#include <pthread.h>
#include <errno.h>
#include <vector>
#include <mutex>
#include <arpa/inet.h>

namespace rrr {
namespace rdma {

// Global RDMA state
static ibv_context* g_context = nullptr;
static ibv_pd* g_pd = nullptr;
static ibv_device** g_device_list = nullptr;
static int g_num_devices = 0;
static bool g_rdma_available = false;
static pthread_once_t g_init_once = PTHREAD_ONCE_INIT;

// User-registered memory regions
struct MemoryRegion {
    void* addr;
    size_t length;
    ibv_mr* mr;
};
static std::vector<MemoryRegion> g_user_mrs;
static std::mutex g_user_mrs_lock;

// Internal: Find and open RDMA device
static bool OpenRdmaDevice() {
    g_device_list = ibv_get_device_list(&g_num_devices);
    if (!g_device_list || g_num_devices == 0) {
        Log_warn("No RDMA devices found");
        return false;
    }
    
    // Use first device
    ibv_device* device = g_device_list[0];
    Log_info("Using RDMA device: %s", ibv_get_device_name(device));
    
    g_context = ibv_open_device(device);
    if (!g_context) {
        Log_error("Failed to open RDMA device: %s", strerror(errno));
        return false;
    }
    
    return true;
}

// Internal: Create protection domain
static bool CreateProtectionDomain() {
    g_pd = ibv_alloc_pd(g_context);
    if (!g_pd) {
        Log_error("Failed to allocate protection domain: %s", strerror(errno));
        return false;
    }
    return true;
}

// Internal: Query device attributes
static bool QueryDeviceAttributes() {
    ibv_device_attr attr;
    if (ibv_query_device(g_context, &attr) != 0) {
        Log_error("Failed to query device attributes: %s", strerror(errno));
        return false;
    }
    
    Log_info("RDMA device attributes:");
    Log_info("  max_qp_wr: %u", attr.max_qp_wr);
    Log_info("  max_sge: %u", attr.max_sge);
    Log_info("  max_cqe: %u", attr.max_cqe);
    Log_info("  max_mr_size: %llu", (unsigned long long)attr.max_mr_size);
    
    return true;
}

// Internal: Cleanup on exit
static void GlobalRdmaCleanup() {
    if (g_pd) {
        ibv_dealloc_pd(g_pd);
        g_pd = nullptr;
    }
    
    if (g_context) {
        ibv_close_device(g_context);
        g_context = nullptr;
    }
    
    if (g_device_list) {
        ibv_free_device_list(g_device_list);
        g_device_list = nullptr;
    }
    
    Log_info("RDMA cleanup complete");
}

// Internal: Initialization function
static void GlobalRdmaInitializeImpl() {
    Log_info("Initializing RDMA support...");
    
    // Open device
    if (!OpenRdmaDevice()) {
        Log_error("Failed to open RDMA device");
        return;
    }
    
    // Create protection domain
    if (!CreateProtectionDomain()) {
        Log_error("Failed to create protection domain");
        return;
    }
    
    // Query attributes
    if (!QueryDeviceAttributes()) {
        Log_error("Failed to query device attributes");
        return;
    }
    
    // Initialize block pool with memory registration callback
    if (!InitBlockPool(RegisterMemoryForRdma)) {
        Log_error("Failed to initialize RDMA block pool");
        return;
    }
    Log_info("RDMA block pool initialized");
    
    // Register cleanup
    atexit(GlobalRdmaCleanup);
    
    g_rdma_available = true;
    Log_info("RDMA initialization successful");
}

void GlobalRdmaInitializeOrDie() {
    if (pthread_once(&g_init_once, GlobalRdmaInitializeImpl) != 0) {
        Log_fatal("Failed to initialize RDMA (pthread_once failed)");
        exit(1);
    }
    
    if (!g_rdma_available) {
        Log_fatal("RDMA initialization failed, but RDMA_REPLICATION is enabled");
        exit(1);
    }
}

bool IsRdmaAvailable() {
    return g_rdma_available;
}

ibv_context* GetRdmaContext() {
    return g_context;
}

ibv_pd* GetRdmaProtectionDomain() {
    return g_pd;
}

uint32_t RegisterMemoryForRdma(void* buf, size_t len) {
    if (!buf || len == 0) {
        Log_error("Invalid buffer for memory registration");
        return 0;
    }
    
    if (!g_pd) {
        Log_error("Protection domain not initialized");
        return 0;
    }
    
    int access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    ibv_mr* mr = ibv_reg_mr(g_pd, buf, len, access_flags);
    if (!mr) {
        Log_error("Failed to register memory: addr=%p, len=%zu, errno=%d (%s)", 
                  buf, len, errno, strerror(errno));
        return 0;
    }
    
    std::lock_guard<std::mutex> lock(g_user_mrs_lock);
    g_user_mrs.push_back({buf, len, mr});
    
    Log_debug("Registered memory: addr=%p, len=%zu, lkey=%u", buf, len, mr->lkey);
    return mr->lkey;
}

void DeregisterMemoryForRdma(void* buf) {
    std::lock_guard<std::mutex> lock(g_user_mrs_lock);
    
    for (auto it = g_user_mrs.begin(); it != g_user_mrs.end(); ++it) {
        if (it->addr == buf) {
            if (ibv_dereg_mr(it->mr) != 0) {
                Log_error("Failed to deregister memory: %s", strerror(errno));
            }
            g_user_mrs.erase(it);
            Log_debug("Deregistered memory: addr=%p", buf);
            return;
        }
    }
    
    Log_warn("Attempted to deregister unregistered memory: %p", buf);
}

uint32_t GetMemoryLKey(const void* buf) {
    std::lock_guard<std::mutex> lock(g_user_mrs_lock);
    
    // Search user-registered regions
    for (const auto& mr : g_user_mrs) {
        uintptr_t addr = (uintptr_t)buf;
        uintptr_t start = (uintptr_t)mr.addr;
        uintptr_t end = start + mr.length;
        
        if (addr >= start && addr < end) {
            return mr.mr->lkey;
        }
    }
    
    Log_error("No lkey found for buffer: %p", buf);
    return 0;
}

bool GetRdmaDeviceInfo(RdmaDeviceInfo* info) {
    if (!info || !g_context) {
        return false;
    }
    
    // Query port attributes for LID and GID
    ibv_port_attr port_attr;
    if (ibv_query_port(g_context, 1, &port_attr) != 0) {
        Log_error("Failed to query port: %s", strerror(errno));
        return false;
    }
    
    info->lid = port_attr.lid;
    
    // Get GID - use index 1 for IPv4-mapped GID (::ffff:x.x.x.x)
    // GID index 0 is link-local (fe80::...), index 1 is IPv4-mapped
    if (ibv_query_gid(g_context, 1, 1, &info->gid) != 0) {
        Log_error("Failed to query GID index 1: %s", strerror(errno));
        return false;
    }
    
    // Debug: log the GID we're using
    char gid_str[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &info->gid, gid_str, sizeof(gid_str));
    Log_info("Using GID index 1: %s", gid_str);
    
    // Query device attributes
    ibv_device_attr dev_attr;
    if (ibv_query_device(g_context, &dev_attr) != 0) {
        Log_error("Failed to query device: %s", strerror(errno));
        return false;
    }
    
    info->max_qp_wr = dev_attr.max_qp_wr;
    info->max_sge = dev_attr.max_sge;
    info->max_cqe = dev_attr.max_cqe;
    
    return true;
}

} // namespace rdma
} // namespace rrr
