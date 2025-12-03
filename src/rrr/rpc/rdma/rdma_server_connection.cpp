#include "rdma_server_connection.h"
#include "../../base/all.hpp"
#include <execinfo.h>

namespace rrr {

RdmaServerConnection::RdmaServerConnection(Server* server, int socket)
    : ServerConnection(server, socket), ctrl_socket_(socket) {
    
    // Override status to HANDSHAKING for RDMA connections
    status_ = HANDSHAKING;

    Log_info("RdmaServerConnection: created with ctrl_socket=%d (handshake pending)", socket);

    // Initialize RDMA endpoint (does not perform handshake yet)
    rdma_endpoint_ = std::make_unique<rdma::RdmaEndpoint>();
    if (!rdma_endpoint_->Initialize()) {
        Log_error("RdmaServerConnection: RDMA endpoint initialization failed");
        verify(0);
    }

    // Set receive buffer for RDMA completions (used after handshake)
    rdma_endpoint_->SetReceiveBuffer(&in_);

    // Note: We keep ctrl_socket_ in the poll thread for now
    // Handshake will be performed in handle_read()
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

void RdmaServerConnection::handle_read() {
    if (status_ == CLOSED) {
        return;
    }

    if (status_ == HANDSHAKING) {
        Log_info("RdmaServerConnection: Starting server-side handshake on fd=%d", ctrl_socket_);
        
        // Perform RDMA handshake over TCP socket (blocking call)
        if (!rdma_endpoint_->AcceptFrom(ctrl_socket_)) {
            Log_error("RdmaServerConnection: RDMA handshake failed");
            close();
            return;
        }

        Log_info("RdmaServerConnection: Handshake complete, switching to RDMA completion channel");

        // CRITICAL: The FD changes from TCP socket to RDMA completion channel.
        // Remove with old FD (before status change), then add back with new FD.
        // Remove while fd() still returns ctrl_socket_
        server_->poll_thread_worker_.as_ref().unwrap()->remove(*this);

        // Now transition to ESTABLISHED - fd() will return completion channel
        status_ = ESTABLISHED;

        Log_info("RdmaServerConnection: FD transition %d -> %d", ctrl_socket_, fd());

        // Add back with new FD (completion channel)
        auto self_arc = weak_self_.upgrade();
        if (self_arc.is_some()) {
            Log_info("RdmaServerConnection: Adding to poll with new FD, self_arc strong_count=%zu", 
                     self_arc.as_ref().unwrap().strong_count());
            server_->poll_thread_worker_.as_ref().unwrap()->add(self_arc.unwrap());
            Log_info("RdmaServerConnection: After add()");
        } else {
            Log_error("RdmaServerConnection: Failed to upgrade weak_self_ during FD transition");
        }

        Log_info("RdmaServerConnection: Now polling completion channel fd=%d", fd());
        return;
    }

    // Status is ESTABLISHED - handle RDMA completions
    // RDMA: Handle completions (appends to in_ buffer)
    rdma_endpoint_->handle_read();
    Log_info("RdmaServerConnection::handle_read: in_.content_size()=%zu", in_.content_size());
    if (in_.content_size() == 0) {
        return;
    }

    // Process all complete packets in the buffer
    std::list<rusty::Box<Request>> complete_requests;
    
    while (true) {
        i32 packet_size;
        int n_peek = in_.peek(&packet_size, sizeof(i32));
        
        if (n_peek != sizeof(i32)) {
            // not enough data to read packet size
            break;
        }

        if (in_.content_size() < packet_size + sizeof(i32)) {
            // packet not complete
            break;
        }

        // got a complete packet
        Log_info("RdmaServerConnection: parsing packet_size=%d", packet_size);
        in_ >> packet_size;

        auto req = rusty::Box<Request>(new Request());
        verify(req->m.read_from_marshal(in_, packet_size) == (size_t) packet_size);
        
        v64 v_xid;
        req->m >> v_xid;
        req->xid = v_xid.get();
        Log_info("RdmaServerConnection: got request xid=%ld", req->xid);
        complete_requests.push_back(std::move(req));
    }

#ifdef RPC_STATISTICS
    stat_server_batching(complete_requests.size());
#endif // RPC_STATISTICS

    for (auto& req: complete_requests) {
        if (req->m.content_size() < sizeof(i32)) {
            // rpc id not provided
            begin_reply(*req, EINVAL);
            end_reply();
            continue;
        }

        i32 rpc_id;
        req->m >> rpc_id;

#ifdef RPC_STATISTICS
        stat_server_rpc_counting(rpc_id);
#endif // RPC_STATISTICS

        auto it = server_->handlers_.find(rpc_id);
        if (it != server_->handlers_.end()) {
            Log_info("RdmaServerConnection: dispatching rpc_id=0x%08x", rpc_id);
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
    // Print backtrace to find caller
    void* callstack[20];
    int frames = backtrace(callstack, 20);
    char** symbols = backtrace_symbols(callstack, frames);
    if (symbols) {
        Log_info("RdmaServerConnection::close() backtrace:");
        for (int i = 0; i < frames; i++) {
            Log_info("  [%d] %s", i, symbols[i]);
        }
        free(symbols);
    }
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
