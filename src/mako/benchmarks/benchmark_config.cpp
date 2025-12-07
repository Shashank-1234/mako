#include "benchmark_config.h"
#include "deptran/config.h"
#include "rrr/rpc/transport_config.h"
#include <cstdlib>
#include <sstream>

void BenchmarkConfig::initSameDcIPs() {
    // Get the Paxos config (must be loaded already)
    auto* config = janus::Config::GetConfig();
    if (!config) {
        Log_warn("BenchmarkConfig::initSameDcIPs: Config not loaded yet");
        return;
    }
    
    // Get same-DC IPs for this process
    auto same_dc_ips = config->GetSameDcIPs(paxos_proc_name_);
    
    if (same_dc_ips.empty()) {
        Log_info("BenchmarkConfig::initSameDcIPs: No same-DC peers found for '%s'", 
                 paxos_proc_name_.c_str());
        return;
    }
    
    // Build comma-separated list
    std::stringstream ss;
    bool first = true;
    for (const auto& ip : same_dc_ips) {
        if (!first) ss << ",";
        ss << ip;
        first = false;
    }
    
    std::string ip_list = ss.str();
    Log_info("BenchmarkConfig::initSameDcIPs: Setting MAKO_LOCAL_DC_IPS=%s for process '%s'",
             ip_list.c_str(), paxos_proc_name_.c_str());
    
    // Set environment variable
    setenv("MAKO_LOCAL_DC_IPS", ip_list.c_str(), 1);
    
    // Reinitialize the transport config to pick up the new IPs
    rrr::ReinitializeSameDcIPs();
}
