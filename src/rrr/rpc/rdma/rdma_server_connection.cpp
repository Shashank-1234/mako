#include "rdma_server_connection.h"
#include "../../base/all.hpp"
#include <execinfo.h>
#include <sys/timerfd.h>
#include <cstring>

namespace rrr {

RdmaServerConnection::RdmaServerConnection(Server* server, int socket)
    : ServerConnection(server, socket), ctrl_socket_(socket) {
    
    // Override status to HANDSHAKING for RDMA connections
    status_ = HANDSHAKING;

    Log_info("RdmaServerConnection: created with ctrl_socket=%d (handshake pending)", socket);

    // Initialize RDMA endpoint as SERVER type (resources allocated after HelloMessage)
    rdma_endpoint_ = std::make_unique<rdma::RdmaEndpoint>(rdma::EndpointType::SERVER);
    if (!rdma_endpoint_->Initialize()) {
        Log_error("RdmaServerConnection: RDMA endpoint initialization failed");
        verify(0);
    }

    // Set receive buffer for RDMA completions
    rdma_endpoint_->SetReceiveBuffer(&in_);
    
    // Set callback for immediate message processing (low-latency mode)
    // Each message is processed and replied to immediately as it arrives
    rdma_endpoint_->SetOnMessageCallback([this](void* data, size_t len) {
        (void)data; (void)len;  // Unused - we read from in_ buffer
        this->process_message();
    });

    // Note: handshake thread is started via start_handshake() after weak_self_ is set
}

void RdmaServerConnection::start_handshake() {
    // Start handshake in a separate thread to avoid blocking the poll thread
    // Must be called after weak_self_ is initialized
    Log_info("RdmaServerConnection: starting handshake thread for fd=%d", ctrl_socket_);
    handshake_thread_ = std::thread(&RdmaServerConnection::handshake_thread_func, this);
}

int RdmaServerConnection::fd() const {
    if (status_ == CLOSED) {
        return -1;  // Invalid FD if closed
    }
    
    if (status_ == HANDSHAKING) {
        // During handshake, poll the TCP control socket
        return ctrl_socket_;
    } else if (status_ == ESTABLISHED && rdma_endpoint_) {
        // After handshake, poll the RDMA completion channel
        return rdma_endpoint_->fd();
    }
    
    return -1;  // Shouldn't reach here
}

// Poll mode is always READ for RDMA connections (i.e. completion channel), write is async
int RdmaServerConnection::poll_mode() const {
    if (status_ == CLOSED) {
        return 0;  // No events if closed
    }
    
    if (status_ == HANDSHAKING) {
        // During handshake, only read from control socket
        return Pollable::READ;
    } else {
        // After handshake, RDMA only needs READ for completion events
        return Pollable::READ;
    }
}

void RdmaServerConnection::handshake_thread_func() {
    Log_info("RdmaServerConnection: Handshake thread started for fd=%d", ctrl_socket_);
    
    // Perform RDMA handshake over TCP socket (blocking call)
    if (!rdma_endpoint_->AcceptFrom(ctrl_socket_)) {
        Log_error("RdmaServerConnection: RDMA handshake failed");
        // Mark as failed and let the main thread handle cleanup
        status_ = CLOSED;
        handshake_complete_.store(true, std::memory_order_release);
        return;
    }

    // Now transition to ESTABLISHED - fd() will return completion channel
    status_ = ESTABLISHED;
    handshake_complete_.store(true, std::memory_order_release);

    // Add to poll with new FD (completion channel)
    // This is safe because the connection was never added to poll during HANDSHAKING
    auto self_arc = weak_self_.upgrade();
    if (self_arc.is_some()) {
        Log_info("RdmaServerConnection: Adding to poll with RDMA FD, self_arc strong_count=%zu", 
                 self_arc.as_ref().unwrap().strong_count());
        server_->poll_thread_worker_.as_ref().unwrap()->add(self_arc.unwrap());
    } else {
        Log_error("RdmaServerConnection: Failed to upgrade weak_self_ during FD transition");
    }
}

void RdmaServerConnection::handle_read() {
    if (status_ == CLOSED) {
        return;
    }

    if (status_ == HANDSHAKING) {
        // Handshake is happening in a separate thread, should not get here
        Log_warn("RdmaServerConnection::handle_read() called during HANDSHAKING - ignoring");
        return;
    }

    // Status is ESTABLISHED - handle RDMA completions
    // In callback mode, process_message is called directly for each received message
    // This provides lower latency by processing and replying immediately
    rdma_endpoint_->handle_read();
}

void RdmaServerConnection::process_message() {
    // Parse packet from in_ buffer (already populated by RdmaEndpoint)
    // Format: <packet_size:i32> <xid:v64> <rpc_id:i32> <payload...>
    
    // Read packet_size
    i32 packet_size;
    in_ >> packet_size;
    
    // Create request and read payload
    auto req = rusty::Box<Request>(new Request());
    verify(req->m.read_from_marshal(in_, packet_size) == (size_t)packet_size);
    
    v64 v_xid;
    req->m >> v_xid;
    req->xid = v_xid.get();
    
    if (req->m.content_size() < sizeof(i32)) {
        // rpc id not provided
        begin_reply(*req, EINVAL);
        end_reply();
        return;
    }
    
    i32 rpc_id;
    req->m >> rpc_id;

#ifdef RPC_STATISTICS
    stat_server_rpc_counting(rpc_id);
#endif // RPC_STATISTICS

    auto it = server_->handlers_.find(rpc_id);
    if (it != server_->handlers_.end()) {
        // Log_debug("RdmaServerConnection: dispatching rpc_id=0x%08x, xid=%ld", rpc_id, req->xid);
        auto weak_this = weak_self_;
        it->second(std::move(req), weak_this);
    } else {
        rpc_id_missing_l_s.lock();
        if (rpc_id_missing_s.find(rpc_id) == rpc_id_missing_s.end()) {
            Log_warn("rrr::ServerConnection: no handler for rpc_id=0x%08x", rpc_id);
            rpc_id_missing_s.insert(rpc_id);
        }
        rpc_id_missing_l_s.unlock();
        begin_reply(*req, ENOENT);
        end_reply();
    }
}

int RdmaServerConnection::handle_write() {
    if (status_ == CLOSED) {
        return Pollable::MODE_NO_CHANGE;
    }
    
    if (status_ == HANDSHAKING) {
        // Should not get write events during handshake
        return Pollable::MODE_NO_CHANGE;
    }
    
    // RDMA writes are posted directly in end_reply(), not through epoll
    return Pollable::MODE_NO_CHANGE;
}

void RdmaServerConnection::end_reply() {
    // Set reply size in packet (same as base class)
    if (bmark_.is_some()) {
        i32 reply_size = out_.get_and_reset_write_cnt();
        out_.write_bookmark(&*bmark_.as_mut().unwrap(), &reply_size);
        bmark_ = rusty::None;
    }

    // Only send if connection is still active and established
    if (status_ == ESTABLISHED) {
        // RDMA: Post send directly (non-blocking)
        ssize_t sent = rdma_endpoint_->SendMessage(out_);
        if (sent < 0) {
            if (errno == EAGAIN) {
                Log_warn("RdmaServerConnection: RDMA send failed: no flow control credits");
            } else {
                Log_error("RdmaServerConnection: RDMA send failed: %s", strerror(errno));
            }
        }
        // Clear output buffer after posting
        out_.reset();
    }

    out_l_.unlock();
}

void RdmaServerConnection::handle_error(uint32_t events) {
    // Sanity check: valid epoll events are small values
    // EPOLLIN=0x1, EPOLLOUT=0x4, EPOLLERR=0x8, EPOLLHUP=0x10, EPOLLRDHUP=0x2000
    // Maximum valid combination is around 0x2FFF
    if (events > 0x3FFF) {
        Log_error("RdmaServerConnection::handle_error() INVALID events=0x%x (garbage?), this=%p, status=%d, fd=%d",
                  events, this, status_, fd());
        // Print backtrace to find caller
        void* callstack[20];
        int frames = backtrace(callstack, 20);
        char** symbols = backtrace_symbols(callstack, frames);
        Log_error("RdmaServerConnection::handle_error() backtrace:");
        for (int i = 0; i < frames; i++) {
            Log_error("  [%d] %s", i, symbols[i]);
        }
        free(symbols);
        // Don't close on garbage - this is likely a bug
        return;
    }
    
    // RDMA completion channels can receive spurious EPOLLRDHUP events.
    // Unlike TCP sockets, EPOLLRDHUP on a completion channel doesn't mean
    // the peer disconnected. Only ignore pure EPOLLRDHUP; real errors should close.
    if ((events & EPOLLRDHUP) && !(events & (EPOLLERR | EPOLLHUP))) {
        Log_debug("RdmaServerConnection::handle_error() ignoring EPOLLRDHUP on fd=%d", fd());
        return;
    }
    Log_info("RdmaServerConnection::handle_error() events=0x%x, closing", events);
    close();
}

void RdmaServerConnection::close() {
    Log_info("RdmaServerConnection::close() called, status=%d", status_);
    
    if (status_ == CLOSED) {
        return;
    }

    server_->sconns_l_.lock();
    for (auto it = server_->sconns_.begin(); it != server_->sconns_.end(); ++it) {
        if (it->get() == this) {
            server_->sconns_.erase(it);
            break;
        }
    }
    server_->sconns_l_.unlock();

    server_->poll_thread_worker_.as_ref().unwrap()->remove(*this);

    // Close RDMA-specific resources
    if (rdma_endpoint_) {
        rdma_endpoint_->Close();
        rdma_endpoint_.reset();
    }

    // Close control socket
    if (ctrl_socket_ >= 0) {
        ::close(ctrl_socket_);
        ctrl_socket_ = -1;
    }

    status_ = CLOSED;
}

} // namespace rrr
