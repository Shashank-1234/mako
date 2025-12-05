#pragma once

#include "../server.hpp"
#include "rdma_endpoint.h"
#include <thread>
#include <atomic>

namespace rrr {

// RDMA-specific server connection that performs handshake and data transfer asynchronously
class RdmaServerConnection : public ServerConnection {
public:
    RdmaServerConnection(Server* server, int socket);

    // Start the handshake thread. Must be called after weak_self_ is set.
    void start_handshake();

    virtual ~RdmaServerConnection() {
        if (handshake_thread_.joinable()) {
            handshake_thread_.join();
        }
        Log_info("RdmaServerConnection: destroyed");
    };

    // Override to return completion channel FD after handshake completes
    int fd() const override;
    
    // Override poll mode to handle handshake and completion events
    int poll_mode() const override;
    
    // Override to handle handshake first, then RDMA completions
    void handle_read() override;
    
    // Override to handle RDMA writes
    int handle_write() override;
    
    // Override to handle RDMA sends in end_reply
    void end_reply() override;
    
    // Override to cleanup RDMA resources
    void close() override;
    
    // Override to ignore spurious EPOLLRDHUP on RDMA completion channel
    void handle_error(uint32_t events) override;

private:
    void handshake_thread_func();

    std::unique_ptr<rdma::RdmaEndpoint> rdma_endpoint_;
    int ctrl_socket_;  // TCP socket used for handshake
    std::thread handshake_thread_;
    std::atomic<bool> handshake_complete_{false};
};

} // namespace rrr
