#pragma once

#include <string>
#include <stdexcept>
#include <cstdlib>
#include <set>
#include <sstream>
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

/**
 * Get IPs of other nodes in the same datacenter (for RDMA eligibility)
 * Reads from MAKO_LOCAL_DC_IPS environment variable (comma-separated list)
 * These are IPs of OTHER nodes in the same DC - NOT including self.
 * 
 * For Paxos: Set automatically by setPaxosProcName() based on config file
 * For RPCBench: User can set manually via environment variable
 * TODO: Figure a common and generic approach and not use Env variables
 *
 * @return Set of same-DC IP address strings (excluding self)
 */
inline const std::set<std::string>& GetSameDcIPs() {
    static std::set<std::string> same_dc_ips;
    static bool initialized = false;

    if (!initialized) {
        const char* env = std::getenv("MAKO_LOCAL_DC_IPS");
        if (env && strlen(env) > 0) {
            std::string env_str(env);
            std::stringstream ss(env_str);
            std::string ip;
            while (std::getline(ss, ip, ',')) {
                // Trim whitespace
                size_t start = ip.find_first_not_of(" \t");
                size_t end = ip.find_last_not_of(" \t");
                if (start != std::string::npos && end != std::string::npos) {
                    std::string trimmed = ip.substr(start, end - start + 1);
                    if (!trimmed.empty()) {
                        same_dc_ips.insert(trimmed);
                    }
                }
            }
            Log_info("[RRR] Same-DC IPs (MAKO_LOCAL_DC_IPS):");
            for (const auto& dc_ip : same_dc_ips) {
                Log_info("[RRR]   - %s", dc_ip.c_str());
            }
        } else {
            Log_info("[RRR] No same-DC IPs configured (MAKO_LOCAL_DC_IPS not set)");
        }
        initialized = true;
    }

    return same_dc_ips;
}

/**
 * Reinitialize same-DC IPs (call after setting MAKO_LOCAL_DC_IPS programmatically)
 * This is needed because the environment variable may be set after initial load
 */
inline void ReinitializeSameDcIPs() {
    // Force re-read on next GetSameDcIPs() call by clearing and re-parsing
    static std::set<std::string>& same_dc_ips = const_cast<std::set<std::string>&>(GetSameDcIPs());
    same_dc_ips.clear();
    
    const char* env = std::getenv("MAKO_LOCAL_DC_IPS");
    if (env && strlen(env) > 0) {
        std::string env_str(env);
        std::stringstream ss(env_str);
        std::string ip;
        while (std::getline(ss, ip, ',')) {
            size_t start = ip.find_first_not_of(" \t");
            size_t end = ip.find_last_not_of(" \t");
            if (start != std::string::npos && end != std::string::npos) {
                std::string trimmed = ip.substr(start, end - start + 1);
                if (!trimmed.empty()) {
                    same_dc_ips.insert(trimmed);
                }
            }
        }
        Log_info("[RRR] Reinitialized same-DC IPs (MAKO_LOCAL_DC_IPS):");
        for (const auto& dc_ip : same_dc_ips) {
            Log_info("[RRR]   - %s", dc_ip.c_str());
        }
    } else {
        Log_info("[RRR] No same-DC IPs configured after reinit");
    }
}

/**
 * Check if an IP is in the same datacenter as this node
 * Used to determine if RDMA should be used (same-DC = RDMA eligible)
 *
 * @param ip_str IP address string to check
 * @return true if IP is in same datacenter (RDMA eligible)
 */
inline bool IsSameDcIP(const char* ip_str) {
    if (!ip_str) return false;
    
    const auto& same_dc_ips = GetSameDcIPs();
    return same_dc_ips.find(ip_str) != same_dc_ips.end();
}

/**
 * Determine if RDMA should be used for a connection to the given IP
 * 
 * Logic:
 * - RDMA only if MAKO_REPLICATION_TRANSPORT=rdma AND IP is in MAKO_LOCAL_DC_IPS
 * - Local IPs are NOT added to MAKO_LOCAL_DC_IPS, so they use TCP
 * - Cross-DC IPs are NOT in MAKO_LOCAL_DC_IPS, so they use TCP
 * - If no datacenter config, all connections use TCP
 *
 * @param ip_str Target IP address
 * @return true if RDMA should be used for this connection
 */
inline bool ShouldUseRdma(const char* ip_str) {
    if (!ip_str) return false;
    
    // Check if RDMA is configured
    if (GetReplicationTransport() != ReplicationTransport::RDMA) {
        return false;
    }
    
    // RDMA only for IPs explicitly in MAKO_LOCAL_DC_IPS (same-DC peers, excludes self)
    return IsSameDcIP(ip_str);
}

} // namespace rrr
