// RDMA Endpoint Implementation
// Ported from bRPC, adapted for RRR

#include "rdma_endpoint.h"
#include "rdma_helper.h"
#include "block_pool.h"
#include "../../base/logging.hpp"
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <algorithm>
#include <chrono>

namespace rrr {
namespace rdma {

// HelloMessage serialization helpers
void RdmaEndpoint::HelloMessage::Serialize(void* data) const {
    uint16_t* pos = (uint16_t*)data;
    *pos++ = htons(msg_len);
    *pos++ = htons(hello_ver);
    *pos++ = htons(impl_ver);
    *(uint32_t*)pos = htonl(block_size);
    pos += 2;  // Move forward 4 bytes
    *pos++ = htons(sq_size);
    *pos++ = htons(rq_size);
    *pos++ = htons(lid);
    memcpy(pos, gid.raw, 16);
    *(uint32_t*)((char*)pos + 16) = htonl(qp_num);
}

void RdmaEndpoint::HelloMessage::Deserialize(void* data) {
    uint16_t* pos = (uint16_t*)data;
    msg_len = ntohs(*pos++);
    hello_ver = ntohs(*pos++);
    impl_ver = ntohs(*pos++);
    block_size = ntohl(*(uint32_t*)pos);
    pos += 2;  // Move forward 4 bytes
    sq_size = ntohs(*pos++);
    rq_size = ntohs(*pos++);
    lid = ntohs(*pos++);
    memcpy(gid.raw, pos, 16);
    qp_num = ntohl(*(uint32_t*)((char*)pos + 16));
}

RdmaEndpoint::RdmaEndpoint()
    : state_(State::UNINIT)
    , in_buffer_(nullptr)
    , qp_(nullptr)
    , cq_(nullptr)
    , comp_channel_(nullptr)
    , sq_size_(DEFAULT_SQ_SIZE)
    , rq_size_(DEFAULT_RQ_SIZE)
    , window_size_(0)
    , new_rq_wrs_(0)
    , sq_current_(0)
    , rq_received_(0)
{
}

RdmaEndpoint::~RdmaEndpoint() {
    Close();
}

bool RdmaEndpoint::Initialize() {
    if (state_ != State::UNINIT) {
        Log_warn("RdmaEndpoint: already initialized");
        return false;
    }
    
    if (!AllocateResources()) {
        Log_error("RdmaEndpoint: failed to allocate resources");
        return false;
    }
    
    state_ = State::INITIALIZED;
    Log_info("RdmaEndpoint: initialized");
    return true;
}

bool RdmaEndpoint::AllocateResources() {
    ibv_context* ctx = GetRdmaContext();
    ibv_pd* pd = GetRdmaProtectionDomain();
    
    if (!ctx || !pd) {
        Log_error("RDMA not initialized");
        return false;
    }
    
    // Create completion channel (for epoll integration)
    comp_channel_ = ibv_create_comp_channel(ctx);
    if (!comp_channel_) {
        Log_error("Failed to create completion channel: %s", strerror(errno));
        return false;
    }
    
    // Create CQ
    cq_ = ibv_create_cq(ctx, sq_size_ + rq_size_, nullptr, comp_channel_, 0);
    if (!cq_) {
        Log_error("Failed to create CQ: %s", strerror(errno));
        return false;
    }
    
    // Request notification on CQ
    if (ibv_req_notify_cq(cq_, 0) != 0) {
        Log_error("Failed to request CQ notification: %s", strerror(errno));
        return false;
    }
    
    // Create QP
    ibv_qp_init_attr qp_attr = {};
    qp_attr.send_cq = cq_;
    qp_attr.recv_cq = cq_;
    qp_attr.cap.max_send_wr = sq_size_;
    qp_attr.cap.max_recv_wr = rq_size_;
    qp_attr.cap.max_send_sge = 1;  // Simple: one SGE per send
    qp_attr.cap.max_recv_sge = 1;
    qp_attr.qp_type = IBV_QPT_RC;  // Reliable Connected
    
    qp_ = ibv_create_qp(pd, &qp_attr);
    if (!qp_) {
        Log_error("Failed to create QP: %s", strerror(errno));
        return false;
    }
    
    // Allocate send buffers
    send_buffers_.resize(sq_size_);
    sbuf_.resize(sq_size_);
    
    for (uint16_t i = 0; i < sq_size_; ++i) {
        send_buffers_[i] = AllocBlock(DEFAULT_BUFFER_SIZE);
        if (!send_buffers_[i]) {
            Log_error("Failed to allocate send buffer");
            return false;
        }
    }
    
    // Allocate receive buffers
    recv_buffers_.resize(rq_size_);
    
    for (uint16_t i = 0; i < rq_size_; ++i) {
        recv_buffers_[i] = AllocBlock(DEFAULT_BUFFER_SIZE);
        if (!recv_buffers_[i]) {
            Log_error("Failed to allocate recv buffer");
            return false;
        }
    }
    
    Log_info("RdmaEndpoint: allocated QP num=%u, SQ=%u, RQ=%u", 
             qp_->qp_num, sq_size_, rq_size_);
    
    return true;
}

void RdmaEndpoint::DeallocateResources() {
    // Destroy QP
    if (qp_) {
        ibv_destroy_qp(qp_);
        qp_ = nullptr;
    }
    
    // Destroy CQ
    if (cq_) {
        ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
    
    // Destroy completion channel
    if (comp_channel_) {
        ibv_destroy_comp_channel(comp_channel_);
        comp_channel_ = nullptr;
    }
    
    // Deallocate buffers
    for (auto buf : send_buffers_) {
        if (buf) DeallocBlock(buf);
    }
    send_buffers_.clear();
    sbuf_.clear();
    
    for (auto buf : recv_buffers_) {
        if (buf) DeallocBlock(buf);
    }
    recv_buffers_.clear();
}

bool RdmaEndpoint::BringUpQp(uint16_t remote_lid, ibv_gid remote_gid, uint32_t remote_qp_num) {
    // Log remote GID in readable format
    char remote_gid_str[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &remote_gid, remote_gid_str, sizeof(remote_gid_str));
    
    Log_info("========== BringUpQp START ==========");
    Log_info("Local QP:  %u", qp_->qp_num);
    Log_info("Remote QP: %u", remote_qp_num);
    Log_info("Remote LID: %u", remote_lid);
    Log_info("Remote GID: %s", remote_gid_str);
    
    // Log local GID for comparison
    ibv_gid local_gid;
    if (ibv_query_gid(GetRdmaContext(), 1, 1, &local_gid) == 0) {
        char local_gid_str[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET6, &local_gid, local_gid_str, sizeof(local_gid_str));
        Log_info("Local GID (index 1): %s", local_gid_str);
    }
    
    // Transition QP: RESET -> INIT
    ibv_qp_attr attr = {};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = 1;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    
    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(qp_, &attr, flags) != 0) {
        Log_error("Failed to transition QP to INIT: %s", strerror(errno));
        return false;
    }
    Log_info("QP transitioned to INIT");
    
    // Transition QP: INIT -> RTR (Ready to Receive)
    // Match bRPC implementation exactly
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.ah_attr.grh.dgid = remote_gid;
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.sgid_index = 1;  // GID 1 is ipv4 mapped, hardcoding for now
    attr.ah_attr.grh.hop_limit = 255;  // MAX_HOP_LIMIT
    attr.ah_attr.grh.traffic_class = 0;
    attr.ah_attr.dlid = remote_lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.static_rate = 0;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = 1;
    attr.dest_qp_num = remote_qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 0;  // bRPC uses 0, not 1
    attr.min_rnr_timer = 0;  // bRPC uses 0 (no RNR tolerance)
    
    flags = IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER | 
            IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN;
    
    Log_info("--- RTR Transition Parameters ---");
    Log_info("  path_mtu: %d (1024)", attr.path_mtu);
    Log_info("  dest_qp_num: %u", attr.dest_qp_num);
    Log_info("  rq_psn: %u", attr.rq_psn);
    Log_info("  max_dest_rd_atomic: %u", attr.max_dest_rd_atomic);
    Log_info("  min_rnr_timer: %u", attr.min_rnr_timer);
    Log_info("  ah_attr.is_global: %d", attr.ah_attr.is_global);
    Log_info("  ah_attr.dlid: %u", attr.ah_attr.dlid);
    Log_info("  ah_attr.port_num: %u", attr.ah_attr.port_num);
    Log_info("  ah_attr.grh.sgid_index: %u", attr.ah_attr.grh.sgid_index);
    Log_info("  ah_attr.grh.hop_limit: %u", attr.ah_attr.grh.hop_limit);
    Log_info("  ah_attr.grh.dgid: %s", remote_gid_str);
    
    if (ibv_modify_qp(qp_, &attr, flags) != 0) {
        Log_error("Failed to transition QP to RTR: %s (errno=%d)", strerror(errno), errno);
        return false;
    }
    Log_info("✓ QP transitioned to RTR (Ready to Receive)");
    
    // Transition QP: RTR -> RTS (Ready to Send)
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 0;  // bRPC: no RNR tolerance
    attr.sq_psn = 0;
    attr.max_rd_atomic = 0;  // bRPC uses 0, not 1
    
    flags = IBV_QP_STATE | IBV_QP_RNR_RETRY | IBV_QP_RETRY_CNT | 
            IBV_QP_TIMEOUT | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    
    if (ibv_modify_qp(qp_, &attr, flags) != 0) {
        Log_error("Failed to transition QP to RTS: %s (errno=%d)", strerror(errno), errno);
        return false;
    }
    Log_info("✓ QP transitioned to RTS (Ready to Send)");
    
    Log_info("========== QP FULLY ESTABLISHED ==========");
    Log_info("Local QP %u <-> Remote QP %u", qp_->qp_num, remote_qp_num);
    Log_info("Local GID: (query above) -> Remote GID: %s", remote_gid_str);
    return true;
}

bool RdmaEndpoint::PostRecv(uint32_t count) {
    Log_info("Posting %u initial receives to RQ", count);
    
    for (uint32_t i = 0; i < count; ++i) {
        uint16_t rq_idx = rq_received_ % rq_size_;
        void* buf = recv_buffers_[rq_idx];
        
        ibv_sge sge;
        sge.addr = (uint64_t)buf;
        sge.length = DEFAULT_BUFFER_SIZE;
        sge.lkey = GetRegionId(buf);
        
        ibv_recv_wr wr = {};
        wr.wr_id = rq_idx;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        
        ibv_recv_wr* bad = nullptr;
        if (ibv_post_recv(qp_, &wr, &bad) != 0) {
            Log_error("Failed to post recv: %s", strerror(errno));
            return false;
        }
        
        rq_received_++;
    }
    
    Log_info("Posted %u receives successfully", count);
    return true;
}

bool RdmaEndpoint::IsWritable() const {
    return state_ == State::ESTABLISHED && window_size_.load() > 0;
}

void RdmaEndpoint::Close() {
    if (state_ != State::UNINIT) {
        DeallocateResources();
        state_ = State::UNINIT;
    }
}

// TCP handshake helpers (from bRPC)
int RdmaEndpoint::ReadFromFd(int fd, void* data, size_t len) {
    size_t read_bytes = 0;
    const int timeout_ms = 60000;  // 60 seconds
    const int poll_interval_ms = 100;  // Poll every 100ms (not 1ms)
    int elapsed_ms = 0;
    
    do {
        ssize_t nr = read(fd, (char*)data + read_bytes, len - read_bytes);
        if (nr < 0) {
            if (errno == EINTR) {
                // Interrupted by signal (e.g., Ctrl+C) - exit gracefully
                Log_info("Handshake read interrupted by signal");
                return -1;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Non-blocking socket - retry with delay
                usleep(poll_interval_ms * 1000);  // Convert to microseconds
                elapsed_ms += poll_interval_ms;
                
                if (elapsed_ms >= timeout_ms) {
                    Log_error("Handshake read timeout after %d ms (expected %zu bytes, got %zu)", 
                              timeout_ms, len, read_bytes);
                    errno = ETIMEDOUT;
                    return -1;
                }
                continue;
            }
            Log_error("Failed to read from fd: %s", strerror(errno));
            return -1;
        }
        if (nr == 0) {
            Log_error("Connection closed during read (expected %zu bytes, got %zu)", 
                      len, read_bytes);
            errno = ECONNRESET;
            return -1;
        }
        read_bytes += nr;
    } while (read_bytes < len);
    return 0;
}

int RdmaEndpoint::WriteToFd(int fd, const void* data, size_t len) {
    size_t written = 0;
    const int timeout_ms = 60000;  // 60 seconds
    const int poll_interval_ms = 100;  // Poll every 100ms (not 1ms)
    int elapsed_ms = 0;
    
    do {
        ssize_t nw = write(fd, (char*)data + written, len - written);
        if (nw < 0) {
            if (errno == EINTR) {
                // Interrupted by signal (e.g., Ctrl+C) - exit gracefully
                Log_info("Handshake write interrupted by signal");
                return -1;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // Non-blocking socket - retry with delay
                usleep(poll_interval_ms * 1000);  // Convert to microseconds
                elapsed_ms += poll_interval_ms;
                
                if (elapsed_ms >= timeout_ms) {
                    Log_error("Handshake write timeout after %d ms (expected %zu bytes, sent %zu)", 
                              timeout_ms, len, written);
                    errno = ETIMEDOUT;
                    return -1;
                }
                continue;
            }
            Log_error("Failed to write to fd: %s", strerror(errno));
            return -1;
        }
        written += nw;
    } while (written < len);
    return 0;
}

bool RdmaEndpoint::DoClientHandshake(int tcp_fd) {
    Log_info("Starting client handshake (60s timeout)");
    auto start_time = std::chrono::steady_clock::now();
    
    uint8_t data[64];  // MAGIC(4) + HelloMessage(36)
    
    // Send HelloMessage to server
    state_ = State::HANDSHAKING;
    
    HelloMessage local_msg;
    local_msg.msg_len = 40;  // Fixed size
    local_msg.hello_ver = 1;
    local_msg.impl_ver = 1;
    local_msg.block_size = DEFAULT_BUFFER_SIZE;
    local_msg.sq_size = sq_size_;
    local_msg.rq_size = rq_size_;
    
    RdmaDeviceInfo dev_info;
    if (!GetRdmaDeviceInfo(&dev_info)) {
        Log_error("Failed to get device info");
        return false;
    }
    
    local_msg.lid = dev_info.lid;
    local_msg.gid = dev_info.gid;
    local_msg.qp_num = qp_->qp_num;
    
    memcpy(data, "RDMA", 4);
    local_msg.Serialize((char*)data + 4);
    
    Log_info("Sending HelloMessage: qp_num=%u, sq=%u, rq=%u", 
             local_msg.qp_num, local_msg.sq_size, local_msg.rq_size);
    
    if (WriteToFd(tcp_fd, data, 40) < 0) {
        Log_error("Failed to send hello message");
        return false;
    }
    
    // Read HelloMessage from server
    Log_info("Waiting for server HelloMessage...");
    if (ReadFromFd(tcp_fd, data, 4) < 0) {
        Log_error("Failed to read magic from server");
        return false;
    }
    
    if (memcmp(data, "RDMA", 4) != 0) {
        Log_error("Invalid magic from server");
        return false;
    }
    
    if (ReadFromFd(tcp_fd, data, 36) < 0) {
        Log_error("Failed to read hello message from server");
        return false;
    }
    
    HelloMessage remote_msg;
    remote_msg.Deserialize(data);
    
    Log_info("Received HelloMessage: remote_qp=%u, sq=%u, rq=%u", 
             remote_msg.qp_num, remote_msg.sq_size, remote_msg.rq_size);
    
    // Bring up QP
    if (!BringUpQp(remote_msg.lid, remote_msg.gid, remote_msg.qp_num)) {
        Log_error("Failed to bring up QP");
        return false;
    }
    
    // Calculate window size (flow control)
    uint16_t local_window = std::min(sq_size_, remote_msg.rq_size);
    window_size_.store(local_window);
    
    // Send ACK
    uint32_t ack = htonl(1);  // RDMA OK
    if (WriteToFd(tcp_fd, &ack, 4) < 0) {
        Log_error("Failed to send ACK");
        return false;
    }
    
    // Post initial receives
    PostRecv(rq_size_);
    
    state_ = State::ESTABLISHED;
    
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    Log_info("Client handshake complete (took %ld ms)", elapsed);
    
    return true;
}

bool RdmaEndpoint::DoServerHandshake(int tcp_fd) {
    Log_info("Starting server handshake (60s timeout)");
    auto start_time = std::chrono::steady_clock::now();
    
    uint8_t data[64];
    
    // Read HelloMessage from client
    state_ = State::HANDSHAKING;
    
    Log_info("Waiting for client HelloMessage...");
    if (ReadFromFd(tcp_fd, data, 4) < 0) {
        Log_error("Failed to read magic from client");
        return false;
    }
    
    if (memcmp(data, "RDMA", 4) != 0) {
        Log_warn("Client not using RDMA, fallback to TCP");
        return false;
    }
    
    if (ReadFromFd(tcp_fd, data, 36) < 0) {
        Log_error("Failed to read hello message from client");
        return false;
    }
    
    HelloMessage remote_msg;
    remote_msg.Deserialize(data);
    
    Log_info("Received HelloMessage: remote_qp=%u, sq=%u, rq=%u", 
             remote_msg.qp_num, remote_msg.sq_size, remote_msg.rq_size);
    
    // Bring up QP
    if (!BringUpQp(remote_msg.lid, remote_msg.gid, remote_msg.qp_num)) {
        Log_error("Failed to bring up QP");
        return false;
    }
    
    // Calculate window size
    uint16_t local_window = std::min(sq_size_, remote_msg.rq_size);
    window_size_.store(local_window);
    
    // Send HelloMessage to client
    HelloMessage local_msg;
    local_msg.msg_len = 40;
    local_msg.hello_ver = 1;
    local_msg.impl_ver = 1;
    local_msg.block_size = DEFAULT_BUFFER_SIZE;
    local_msg.sq_size = sq_size_;
    local_msg.rq_size = rq_size_;
    
    RdmaDeviceInfo dev_info;
    if (!GetRdmaDeviceInfo(&dev_info)) {
        Log_error("Failed to get device info");
        return false;
    }
    
    local_msg.lid = dev_info.lid;
    local_msg.gid = dev_info.gid;
    local_msg.qp_num = qp_->qp_num;
    
    memcpy(data, "RDMA", 4);
    local_msg.Serialize((char*)data + 4);
    
    Log_info("Sending HelloMessage: qp_num=%u, sq=%u, rq=%u", 
             local_msg.qp_num, local_msg.sq_size, local_msg.rq_size);
    
    if (WriteToFd(tcp_fd, data, 40) < 0) {
        Log_error("Failed to send hello message");
        return false;
    }
    
    // Read ACK from client
    uint32_t ack;
    if (ReadFromFd(tcp_fd, &ack, 4) < 0) {
        Log_error("Failed to read ACK from client");
        return false;
    }
    
    if (ntohl(ack) != 1) {
        Log_error("Client ACK failed");
        return false;
    }
    
    // Post initial receives
    PostRecv(rq_size_);
    
    state_ = State::ESTABLISHED;
    
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();
    Log_info("Server handshake complete (took %ld ms)", elapsed);
    
    return true;
}

bool RdmaEndpoint::ConnectTo(const char* addr, int port) {
    Log_info("RdmaEndpoint::ConnectTo: connecting to %s:%d", addr, port);
    
    // Create TCP connection first
    int tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd < 0) {
        Log_error("Failed to create socket: %s", strerror(errno));
        return false;
    }
    
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, addr, &server_addr.sin_addr) <= 0) {
        Log_error("Invalid address: %s", addr);
        close(tcp_fd);
        return false;
    }
    
    if (connect(tcp_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        Log_error("Failed to connect to %s:%d: %s", addr, port, strerror(errno));
        close(tcp_fd);
        return false;
    }
    
    // Do RDMA handshake over TCP
    bool success = DoClientHandshake(tcp_fd);
    
    // TCP connection no longer needed after handshake
    close(tcp_fd);
    
    return success;
}

bool RdmaEndpoint::AcceptFrom(int tcp_fd) {
    Log_info("RdmaEndpoint::AcceptFrom: accepting RDMA connection on fd=%d", tcp_fd);
    
    // RDMA handshake over accepted TCP connection
    bool success = DoServerHandshake(tcp_fd);
    
    // TCP connection no longer needed after handshake
    // Note: Caller is responsible for closing tcp_fd
    
    return success;
}

// ============================================================================
// Pollable Interface Implementation
// ============================================================================

int RdmaEndpoint::fd() const {
    if (!comp_channel_) {
        return -1;
    }
    return comp_channel_->fd;
}

int RdmaEndpoint::poll_mode() const {
    // RDMA only needs READ for completion events
    // No EPOLLOUT - sends are non-blocking
    return Pollable::READ;
}

void RdmaEndpoint::handle_read() {
    // CQ has completion events
    HandleCompletions();
}

int RdmaEndpoint::handle_write() {
    // RDMA doesn't use EPOLLOUT
    return Pollable::MODE_NO_CHANGE;
}

void RdmaEndpoint::handle_error(uint32_t events) {
    // Ignore spurious EPOLLRDHUP on completion channel
    if ((events & EPOLLRDHUP) && !(events & (EPOLLERR | EPOLLHUP))) {
        Log_debug("RdmaEndpoint: ignoring EPOLLRDHUP on completion channel");
        return;
    }
    Log_error("RdmaEndpoint: error on completion channel, events=0x%x", events);
    Close();
}

// ============================================================================
// RDMA Send/Recv Implementation
// ============================================================================

void RdmaEndpoint::SetReceiveBuffer(Marshal* in_buffer) {
    in_buffer_ = in_buffer;
}

ssize_t RdmaEndpoint::SendMessage(Marshal& data) {
    if (state_ != State::ESTABLISHED) {
        errno = ENOTCONN;
        return -1;
    }
    
    // Check flow control
    if (window_size_.load(std::memory_order_relaxed) == 0) {
        errno = EAGAIN;
        return -1;
    }
    
    size_t size = data.content_size();
    if (size > DEFAULT_BUFFER_SIZE) {
        Log_error("Message too large: %zu > %u", size, DEFAULT_BUFFER_SIZE);
        errno = EMSGSIZE;
        return -1;
    }
    
    // Get current send buffer
    uint16_t sq_idx = sq_current_;
    void* send_buf = send_buffers_[sq_idx];
    
    // Copy data to registered send buffer
    // TODO: Zero-copy optimization later
    size_t copied = 0;
    while (copied < size && !data.empty()) {
        size_t to_copy = std::min(size - copied, (size_t)DEFAULT_BUFFER_SIZE - copied);
        data.read((char*)send_buf + copied, to_copy);
        copied += to_copy;
    }
    
    // Prepare SGE
    ibv_sge sge;
    sge.addr = (uint64_t)send_buf;
    sge.length = size;
    sge.lkey = GetMemoryLKey(send_buf);
    
    // Prepare send WR
    ibv_send_wr wr = {};
    wr.wr_id = sq_idx;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    
    // Piggyback ACKs in immediate data
    uint32_t acks = new_rq_wrs_.exchange(0, std::memory_order_relaxed);
    wr.imm_data = htonl((size << 16) | (acks & 0xFFFF));
    
    // Post send
    ibv_send_wr* bad = nullptr;
    if (ibv_post_send(qp_, &wr, &bad) != 0) {
        Log_error("Failed to post send: %s", strerror(errno));
        return -1;
    }
    
    // Update state
    sq_current_ = (sq_current_ + 1) % sq_size_;
    window_size_.fetch_sub(1, std::memory_order_relaxed);
    
    return size;
}

void RdmaEndpoint::HandleCompletions() {
    // Acknowledge CQ event
    ibv_cq* ev_cq;
    void* ev_ctx;
    if (ibv_get_cq_event(comp_channel_, &ev_cq, &ev_ctx) == 0) {
        ibv_ack_cq_events(cq_, 1);
    }
    
    // Request next notification
    ibv_req_notify_cq(cq_, 0);
    
    // Poll all available completions
    ibv_wc wc[32];
    int n = ibv_poll_cq(cq_, 32, wc);
    
    for (int i = 0; i < n; i++) {
        if (wc[i].status != IBV_WC_SUCCESS) {
            Log_error("Work completion error: %s", 
                     ibv_wc_status_str(wc[i].status));
            continue;
        }
        
        if (wc[i].opcode == IBV_WC_RECV) {
            HandleRecvCompletion(wc[i]);
        } else if (wc[i].opcode == IBV_WC_SEND) {
            HandleSendCompletion(wc[i]);
        }
    }
}

void RdmaEndpoint::HandleSendCompletion(ibv_wc& wc) {
    // bRPC approach: Do nothing here
    // Send buffer cleanup happens in HandleRecvCompletion when ACK arrives
    // (See HandleRecvCompletion processing of imm_data)
}

void RdmaEndpoint::HandleRecvCompletion(ibv_wc& wc) {
    uint16_t rq_idx = wc.wr_id;
    void* buf = recv_buffers_[rq_idx];
    
    // Extract message size and ACK from immediate data
    uint32_t imm = ntohl(wc.imm_data);
    uint16_t msg_size = imm >> 16;
    uint16_t ack_count = imm & 0xFFFF;
    
    // Append received data to in_buffer (provided by Client/ServerConnection)
    if (msg_size > 0 && in_buffer_) {
        in_buffer_->write(buf, msg_size);
    }
    
    // Process ACKs (sender-side flow control)
    if (ack_count > 0) {
        // Remote acknowledges it processed our sends
        window_size_.fetch_add(ack_count, std::memory_order_relaxed);
        
        // Free corresponding send buffers
        // (bRPC pattern: cleanup happens on ACK, not send completion)
        for (uint16_t i = 0; i < ack_count; i++) {
            // Send buffers will be reused automatically via circular index
        }
    }
    
    // Re-post receive buffer
    PostRecv(1);
    
    // Increment ACK counter (will be piggybacked on next send)
    if (msg_size > 0) {
        new_rq_wrs_.fetch_add(1, std::memory_order_relaxed);
    }
}

} // namespace rdma
} // namespace rrr
