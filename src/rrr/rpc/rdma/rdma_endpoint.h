// RDMA Endpoint - Core RDMA connection management
// Ported from bRPC RdmaEndpoint, adapted for RRR

#pragma once

#include <infiniband/verbs.h>
#include <memory>
#include <vector>
#include <atomic>
#include "../../misc/marshal.hpp"
#include "../../reactor/epoll_wrapper.h"

namespace rrr {
namespace rdma {

// RDMA connection endpoint
// Manages QP, CQ, send/recv buffers, and flow control
// Implements Pollable for integration with PollThread
class RdmaEndpoint : public Pollable {
public:
    RdmaEndpoint();
    ~RdmaEndpoint();
    
    // Initialize resources (QP, CQ, buffers)
    // Must be called before Connect
    bool Initialize();
    
    // Connection establishment (client-side)
    // Uses TCP socket for handshake
    bool ConnectTo(const char* addr, int port);
    
    // Connection acceptance (server-side)
    // tcp_fd: Accepted TCP connection for handshake
    bool AcceptFrom(int tcp_fd);
    
    // === Pollable Interface ===
    // Returns completion channel FD (monitored by PollThread)
    int fd() const override;
    
    // Returns READ (only monitor completions, no EPOLLOUT for RDMA)
    int poll_mode() const override;
    
    // Called when CQ has completions - processes both send and recv
    // Appends received data to in_buffer (passed from Client/ServerConnection)
    void handle_read() override;
    
    // RDMA doesn't use EPOLLOUT (sends are non-blocking)
    int handle_write() override;
    
    // Handle connection errors
    void handle_error() override;
    
    // === RDMA-specific API ===
    // Send message directly (called from user thread in end_request)
    // data: Marshal buffer containing serialized request/response
    // Returns bytes posted, -1 on error (errno = EAGAIN if no credits)
    // NOTE: Non-blocking! Always returns immediately
    ssize_t SendMessage(Marshal& data);
    
    // Set Marshal buffer for appending received data
    // Called by Client/ServerConnection to provide their in_ buffer
    void SetReceiveBuffer(Marshal* in_buffer);
    
    // Get window size for flow control check
    uint16_t GetWindowSize() const { return window_size_.load(); }
    
    // Check if endpoint is ready for writes
    bool IsWritable() const;
    
    // Close connection
    void Close();
    
    // Connection state
    bool IsConnected() const { return state_ == State::ESTABLISHED; }
    
private:
    enum class State {
        UNINIT,           // Not initialized
        INITIALIZED,      // Resources allocated
        HANDSHAKING,      // TCP handshake in progress
        ESTABLISHED,      // RDMA connection ready
        FAILED            // Connection failed
    };
    
    // Handshake message (exchanged over TCP)
    struct HelloMessage {
        uint16_t msg_len;        // 40 bytes
        uint16_t hello_ver;      // Protocol version
        uint16_t impl_ver;       // Implementation version
        uint32_t block_size;     // Receive buffer size (8KB default)
        uint16_t sq_size;        // Send queue size (128 default)
        uint16_t rq_size;        // Receive queue size (128 default)
        uint16_t lid;            // Local ID
        ibv_gid gid;             // Global ID (16 bytes)
        uint32_t qp_num;         // QP number
        
        void Serialize(void* data) const;
        void Deserialize(void* data);
    };
    
    // Resource allocation/deallocation
    bool AllocateResources();
    void DeallocateResources();
    
    // Handshake helpers
    bool DoClientHandshake(int tcp_fd);
    bool DoServerHandshake(int tcp_fd);
    bool BringUpQp(uint16_t remote_lid, ibv_gid remote_gid, uint32_t remote_qp_num);
    
    // TCP I/O helpers (for handshake)
    int ReadFromFd(int fd, void* data, size_t len);
    int WriteToFd(int fd, const void* data, size_t len);
    
    // Send/Recv helpers
    bool PostRecv(uint32_t count);
    
    // Completion handling
    void HandleSendCompletion(ibv_wc& wc);
    void HandleRecvCompletion(ibv_wc& wc);
    void HandleCompletions();  // Process all pending completions
    
    // State
    State state_;
    
    // Receive buffer (provided by Client/ServerConnection)
    Marshal* in_buffer_;  // Not owned, just a reference
    
    // RDMA resources
    ibv_qp* qp_;                         // Queue Pair
    ibv_cq* cq_;                         // Completion Queue
    ibv_comp_channel* comp_channel_;     // Completion channel (for epoll)
    
    // Queue sizes
    uint16_t sq_size_;                   // Send queue size (default 128)
    uint16_t rq_size_;                   // Receive queue size (default 128)
    
    // Send buffers (pre-registered memory)
    std::vector<void*> send_buffers_;    // Raw buffers for RDMA
    std::vector<std::shared_ptr<Marshallable>> sbuf_;  // Keep Marshallable alive
    
    // Receive buffers (pre-registered memory)
    std::vector<void*> recv_buffers_;    // Raw buffers for RDMA
    
    // Flow control
    std::atomic<uint16_t> window_size_;      // Credits available (sender-side)
    std::atomic<uint16_t> new_rq_wrs_;       // ACKs to send (receiver-side)
    
    // Queue indices
    uint16_t sq_current_;                // Next send slot
    uint16_t rq_received_;               // Receives processed
    
    // Configuration
    static const uint32_t DEFAULT_BUFFER_SIZE = 8192;   // 8KB
    static const uint16_t DEFAULT_SQ_SIZE = 128;
    static const uint16_t DEFAULT_RQ_SIZE = 128;
    static const uint16_t ACK_THRESHOLD = 16;           // Send ACK every 16 receives
    
    // Disable copy
    RdmaEndpoint(const RdmaEndpoint&) = delete;
    RdmaEndpoint& operator=(const RdmaEndpoint&) = delete;
};

} // namespace rdma
} // namespace rrr
