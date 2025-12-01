#pragma once

#include <string>
#include <stdexcept>
#include <cstdlib>
#include "../base/logging.hpp"

namespace rrr {

/**
 * Replication transport type enumeration
 *
 * TCP:  Standard TCP sockets for replication
 * RDMA: RDMA verbs for low-latency replication
 */
enum class ReplicationTransport {
    TCP = 0,   // Standard TCP sockets
    RDMA = 1   // RDMA verbs (InfiniBand)
};

/**
 * Parse replication transport type from string
 *
 * @param type_str String representation ("tcp" or "rdma")
 * @return ReplicationTransport enum value
 * @throws If type_str is invalid
 */
inline ReplicationTransport ParseReplicationTransport(const std::string& type_str) {
    if (type_str == "rdma" || type_str == "RDMA") {
        return ReplicationTransport::RDMA;
    } else {
        return ReplicationTransport::TCP;
    }
}

/**
 * Convert replication transport type to string
 *
 * @param type ReplicationTransport enum value
 * @return String representation ("tcp" or "rdma")
 */
inline const char* ReplicationTransportToString(ReplicationTransport type) {
    switch (type) {
        case ReplicationTransport::RDMA: return "rdma";
        case ReplicationTransport::TCP: return "tcp";
        default: return "tcp";
    }
}

/**
 * Get configured replication transport (singleton accessor)
 * Reads MAKO_REPLICATION_TRANSPORT environment variable
 * Default: TCP
 *
 * @return Configured ReplicationTransport
 */
inline ReplicationTransport GetReplicationTransport() {
    static ReplicationTransport cached = ReplicationTransport::TCP;
    static bool initialized = false;
    
    if (!initialized) {
        const char* env = std::getenv("MAKO_REPLICATION_TRANSPORT");
        if (env) {
            try {
                cached = ParseReplicationTransport(env);
            } catch (const std::exception& e) {
                Log_warn("[RRR] WARNING: %s, defaulting to TCP", e.what());
            }
        }
        Log_info("[RRR] Using %s replication transport (via MAKO_REPLICATION_TRANSPORT)", 
                 ReplicationTransportToString(cached));
        initialized = true;
    }
    return cached;
}

} // namespace rrr
