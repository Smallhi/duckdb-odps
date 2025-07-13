#pragma once

#include "odps_reader.hpp"
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// Tunnel API specific reader implementation
class ODPSTunnelReader : public ODPSReader {
public:
    ODPSTunnelReader(const std::string& access_id,
                     const std::string& access_key,
                     const std::string& project,
                     const std::string& endpoint,
                     const std::string& table_name,
                     const std::string& tunnel_endpoint);
    
    ~ODPSTunnelReader() override;
    
    // ODPSReader interface implementation
    bool Initialize() override;
    bool HasNext() override;
    bool ReadNextBatch(DataChunk& output) override;
    void Close() override;
    
    std::vector<std::string> GetColumnNames() const override;
    std::vector<LogicalType> GetColumnTypes() const override;
    int64_t GetTotalRows() const override;
    
    void SetColumns(const std::vector<std::string>& columns) override;
    void SetFilter(const std::string& filter) override;
    void SetPartitions(const std::vector<std::string>& partitions) override;

private:
    // Connection configuration
    std::string access_id_;
    std::string access_key_;
    std::string project_;
    std::string endpoint_;
    std::string table_name_;
    std::string tunnel_endpoint_;
    
    // Tunnel session
    std::string tunnel_id_;
    int64_t total_rows_;
    int64_t current_row_;
    
    // Schema information
    std::vector<std::string> column_names_;
    std::vector<LogicalType> column_types_;
    std::vector<std::string> partition_columns_;
    
    // Configuration
    std::vector<std::string> selected_columns_;
    std::string filter_predicate_;
    std::vector<std::string> required_partitions_;
    
    // HTTP client
    class HttpClient;
    std::unique_ptr<HttpClient> http_client_;
    
    // Data stream
    class DataStream;
    std::unique_ptr<DataStream> data_stream_;
    
    // Internal methods
    bool CreateTunnelSession();
    bool OpenDataStream();
    std::string GenerateSignature(const std::string& method, const std::string& path, 
                                 const std::string& timestamp, const std::string& body = "");
    std::string GetAuthHeaders(const std::string& method, const std::string& path, 
                              const std::string& body = "");
    bool ParseTunnelResponse(const std::string& response);
    bool ParseSchemaFromResponse(const std::string& response);
    LogicalType MapODPSTypeToDuckDBType(const std::string& odps_type);
    bool ParseRecord(const std::string& record, DataChunk& output, int row_index);
};

} // namespace duckdb 