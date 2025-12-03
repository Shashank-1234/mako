#pragma once

#include "../server.hpp"
#include "rdma_endpoint.h"

namespace rrr {

// RDMA-specific server connection that performs handshake and data transfer asynchronously
class RdmaServerConnection : public ServerConnection {
public:
    RdmaServerConnection(Server* server, int socket);
    virtual ~RdmaServerConnection() {
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

private:
    std::unique_ptr<rdma::RdmaEndpoint> rdma_endpoint_;
    int ctrl_socket_;  // TCP socket used for handshake
};

} // namespace rrr
