#include "odps_reader.hpp"
#include "odps_storage_reader.hpp"
#include "odps_tunnel_reader.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

std::unique_ptr<ODPSReader> ODPSReaderFactory::CreateReader(
    ODPSReaderType type,
    const std::string& access_id,
    const std::string& access_key,
    const std::string& project,
    const std::string& endpoint,
    const std::string& table_name,
    const std::string& tunnel_endpoint) {
    
    switch (type) {
        case ODPSReaderType::STORAGE_API:
            return std::make_unique<ODPSStorageReader>(
                access_id, access_key, project, endpoint, table_name);
        case ODPSReaderType::TUNNEL:
            return std::make_unique<ODPSTunnelReader>(
                access_id, access_key, project, endpoint, table_name, tunnel_endpoint);
        default:
            throw Exception("Unknown ODPS reader type");
    }
}

} // namespace duckdb 