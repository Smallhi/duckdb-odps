#pragma once

#include "duckdb.hpp"
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// Abstract base class for ODPS data readers
class ODPSReader {
public:
    virtual ~ODPSReader() = default;
    
    // Common interface for all readers
    virtual bool Initialize() = 0;
    virtual bool HasNext() = 0;
    virtual bool ReadNextBatch(DataChunk& output) = 0;
    virtual void Close() = 0;
    
    // Schema information
    virtual std::vector<std::string> GetColumnNames() const = 0;
    virtual std::vector<LogicalType> GetColumnTypes() const = 0;
    virtual int64_t GetTotalRows() const = 0;
    
    // Configuration
    virtual void SetColumns(const std::vector<std::string>& columns) = 0;
    virtual void SetFilter(const std::string& filter) = 0;
    virtual void SetPartitions(const std::vector<std::string>& partitions) = 0;
};

// Reader factory
enum class ODPSReaderType {
    STORAGE_API,
    TUNNEL
};

class ODPSReaderFactory {
public:
    static std::unique_ptr<ODPSReader> CreateReader(
        ODPSReaderType type,
        const std::string& access_id,
        const std::string& access_key,
        const std::string& project,
        const std::string& endpoint,
        const std::string& table_name,
        const std::string& tunnel_endpoint = ""
    );
};

} // namespace duckdb 