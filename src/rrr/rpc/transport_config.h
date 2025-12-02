#pragma once

#include <string>
#include <stdexcept>
#include <cstdlib>
#include <set>
#include <arpa/inet.h>
#include <sys/types.h>
#include <ifaddrs.h>
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

/**
 * Get all local IP addresses on this machine
 * Caches the result on first call
 *
 * @return Set of local IP address strings
 */
inline const std::set<std::string>& GetLocalIPs() {
    static std::set<std::string> local_ips;
    static bool initialized = false;

    if (!initialized) {
        // Always include loopback addresses
        local_ips.insert("127.0.0.1");
        local_ips.insert("::1");
        local_ips.insert("localhost");

#ifndef _WIN32
        // Query network interfaces using getifaddrs()
        struct ifaddrs* ifaddr = nullptr;
        if (getifaddrs(&ifaddr) == 0) {
            for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
                if (ifa->ifa_addr == nullptr) continue;

                char ip_str[INET6_ADDRSTRLEN];

                if (ifa->ifa_addr->sa_family == AF_INET) {
                    // IPv4
                    struct sockaddr_in* addr = (struct sockaddr_in*)ifa->ifa_addr;
                    inet_ntop(AF_INET, &(addr->sin_addr), ip_str, sizeof(ip_str));
                    local_ips.insert(ip_str);
                } else if (ifa->ifa_addr->sa_family == AF_INET6) {
                    // IPv6
                    struct sockaddr_in6* addr = (struct sockaddr_in6*)ifa->ifa_addr;
                    inet_ntop(AF_INET6, &(addr->sin6_addr), ip_str, sizeof(ip_str));
                    local_ips.insert(ip_str);
                }
            }
            freeifaddrs(ifaddr);
        }
#endif

        // Log discovered local IPs
        Log_info("[RRR] Discovered local IPs:");
        for (const auto& ip : local_ips) {
            Log_info("[RRR]   - %s", ip.c_str());
        }

        initialized = true;
    }

    return local_ips;
}

/**
 * Check if an IP address is local to this machine
 * Uses cached list of local interface addresses
 *
 * @param ip_str IP address string (e.g., "127.0.0.1", "10.1.0.14")
 * @return true if IP is local to this machine
 */
inline bool IsLocalIP(const char* ip_str) {
    if (!ip_str) return false;

    const auto& local_ips = GetLocalIPs();
    return local_ips.find(ip_str) != local_ips.end();
}

} // namespace rrr
