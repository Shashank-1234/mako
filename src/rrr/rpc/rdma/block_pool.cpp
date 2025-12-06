// RDMA Memory Pool Implementation
// Simplified version adapted for RRR

#include "block_pool.h"
#include "rdma_helper.h"
#include "../../base/logging.hpp"
#include <stdlib.h>
#include <string.h>
#include <mutex>
#include <vector>

namespace rrr {
namespace rdma {

// Configuration (can be made flags later)
static const size_t DEFAULT_INITIAL_SIZE_MB = 128;  // 128MB
static const size_t BYTES_IN_MB = 1048576;
static const size_t MAX_REGIONS = 16;

// Memory region tracking
struct Region {
    void* start;
    size_t size;
    uint32_t lkey;
};

// Free block node (simple linked list)
struct FreeBlock {
    void* addr;
    size_t size;
    FreeBlock* next;
};

// Global pool state
static RegisterCallback g_register_cb = nullptr;
static std::vector<Region> g_regions;
static FreeBlock* g_free_list = nullptr;
static std::mutex g_pool_mutex;
static bool g_initialized = false;

// Statistics
static size_t g_allocated_blocks = 0;
static size_t g_free_blocks = 0;

// Internal: Find which region a pointer belongs to
static Region* FindRegion(const void* ptr) {
    uintptr_t addr = (uintptr_t)ptr;
    for (auto& region : g_regions) {
        uintptr_t start = (uintptr_t)region.start;
        uintptr_t end = start + region.size;
        if (addr >= start && addr < end) {
            return &region;
        }
    }
    return nullptr;
}

// Internal: Add a region to the pool
static bool AddRegion(size_t size_bytes) {
    Log_debug("Block pool: attempting to add region of %zu MB", size_bytes / BYTES_IN_MB);
    
    if (g_regions.size() >= MAX_REGIONS) {
        Log_error("Block pool: max regions (%zu) reached", MAX_REGIONS);
        return false;
    }
    
    // Allocate memory
    void* base = malloc(size_bytes);
    if (!base) {
        Log_error("Block pool: failed to allocate %zu bytes", size_bytes);
        return false;
    }
    
    // Register with RDMA
    uint32_t lkey = g_register_cb(base, size_bytes);
    if (lkey == 0) {
        Log_error("Block pool: failed to register memory");
        free(base);
        return false;
    }
    
    // Add to regions
    Region region;
    region.start = base;
    region.size = size_bytes;
    region.lkey = lkey;
    g_regions.push_back(region);
    
    // Add entire region as one free block
    FreeBlock* block = new FreeBlock;
    block->addr = base;
    block->size = size_bytes;
    block->next = g_free_list;
    g_free_list = block;
    g_free_blocks++;
    
    Log_info("Block pool: added region %p, size %zu MB, lkey %u", 
             base, size_bytes / BYTES_IN_MB, lkey);
    
    return true;
}

bool InitBlockPool(RegisterCallback cb) {
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    
    if (g_initialized) {
        Log_warn("Block pool: already initialized");
        return false;
    }
    
    if (!cb) {
        Log_error("Block pool: null registration callback");
        return false;
    }
    
    g_register_cb = cb;
    
    // Pre-allocate initial region
    Log_info("Block pool: allocating initial region of %zu MB", DEFAULT_INITIAL_SIZE_MB);
    if (!AddRegion(DEFAULT_INITIAL_SIZE_MB * BYTES_IN_MB)) {
        Log_error("Block pool: failed to add initial region");
        return false;
    }
    
    g_initialized = true;
    Log_info("Block pool: initialized with %zu MB", DEFAULT_INITIAL_SIZE_MB);
    
    return true;
}

void* AllocBlock(size_t size) {
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    
    if (!g_initialized) {
        Log_error("Block pool: not initialized");
        return nullptr;
    }
    
    // Simple first-fit allocation
    FreeBlock* prev = nullptr;
    FreeBlock* curr = g_free_list;
    
    while (curr) {
        if (curr->size >= size) {
            // Found suitable block
            void* addr = curr->addr;
            
            if (curr->size > size) {
                // Split block
                curr->addr = (char*)curr->addr + size;
                curr->size -= size;
            } else {
                // Use entire block
                if (prev) {
                    prev->next = curr->next;
                } else {
                    g_free_list = curr->next;
                }
                delete curr;
                g_free_blocks--;
            }
            
            g_allocated_blocks++;
            return addr;
        }
        
        prev = curr;
        curr = curr->next;
    }
    
    // No suitable block found - try to extend pool
    Log_warn("Block pool: no free block for size %zu, extending pool", size);
    
    size_t extend_size = (DEFAULT_INITIAL_SIZE_MB * BYTES_IN_MB);
    if (size > extend_size) {
        extend_size = size * 2;  // Extend with at least 2x requested size
    }
    
    if (!AddRegion(extend_size)) {
        Log_error("Block pool: failed to extend");
        return nullptr;
    }
    
    // Retry allocation
    return AllocBlock(size);
}

void DeallocBlock(void* block) {
    if (!block) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    
    if (!g_initialized) {
        Log_error("Block pool: not initialized");
        return;
    }
    
    // Find which region this block belongs to
    Region* region = FindRegion(block);
    if (!region) {
        Log_error("Block pool: block %p not from pool", block);
        return;
    }
    
    // For simplicity, we don't track individual block sizes on dealloc
    // Just mark as free with a conservative estimate
    // TODO: Proper size tracking or use fixed block sizes
    size_t block_size = BLOCK_MEDIUM;  // Conservative estimate
    
    // Add back to free list
    FreeBlock* fb = new FreeBlock;
    fb->addr = block;
    fb->size = block_size;
    fb->next = g_free_list;
    g_free_list = fb;
    
    g_allocated_blocks--;
    g_free_blocks++;
}

uint32_t GetRegionId(const void* block) {
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    
    Region* region = FindRegion(block);
    if (region) {
        return region->lkey;
    }
    
    return 0;
}

void* ExtendBlockPool(size_t region_size_mb) {
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    
    if (!g_initialized) {
        Log_error("Block pool: not initialized");
        return nullptr;
    }
    
    size_t size_bytes = region_size_mb * BYTES_IN_MB;
    if (AddRegion(size_bytes)) {
        return g_regions.back().start;
    }
    
    return nullptr;
}

void GetBlockPoolStats(BlockPoolStats* stats) {
    if (!stats) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(g_pool_mutex);
    
    stats->total_regions = g_regions.size();
    stats->allocated_blocks = g_allocated_blocks;
    stats->free_blocks = g_free_blocks;
    
    stats->total_bytes = 0;
    for (const auto& region : g_regions) {
        stats->total_bytes += region.size;
    }
}

} // namespace rdma
} // namespace rrr
