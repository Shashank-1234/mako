// RDMA Memory Pool - Pre-registered memory blocks for zero-copy RDMA
// Ported from bRPC block_pool.h, simplified for RRR

#pragma once

#include <cstdint>
#include <cstddef>

namespace rrr {
namespace rdma {

// Callback type for memory registration
// Called when a new memory region is added to the pool
// Returns lkey (local key) for the registered region, 0 on failure
typedef uint32_t (*RegisterCallback)(void* buf, size_t size);

// Initialize the block pool with registration callback
// Must be called before any AllocBlock/DeallocBlock
// Returns true on success
bool InitBlockPool(RegisterCallback cb);

// Allocate a block from the pool
// Block will be from registered memory (has lkey)
// Returns nullptr if pool exhausted (should extend or fail gracefully)
void* AllocBlock(size_t size);

// Deallocate a block back to the pool
// Block must have been allocated from AllocBlock
void DeallocBlock(void* block);

// Get lkey for a block allocated from the pool
// Returns 0 if block not from pool
uint32_t GetRegionId(const void* block);

// Extend the pool with more memory (optional - pool extends automatically)
// region_size: Size in MB
// Returns pointer to new region, nullptr on failure
void* ExtendBlockPool(size_t region_size_mb);

// Pool statistics (for debugging/monitoring)
struct BlockPoolStats {
    size_t total_regions;
    size_t total_bytes;
    size_t allocated_blocks;
    size_t free_blocks;
};

void GetBlockPoolStats(BlockPoolStats* stats);

// Block sizes (can be tuned for RRR workload)
enum BlockSize {
    BLOCK_SMALL = 4096,      // 4KB - small RPC messages
    BLOCK_MEDIUM = 16384,    // 16KB - transaction commands
    BLOCK_LARGE = 131072     // 128KB - large batches
};

} // namespace rdma
} // namespace rrr
