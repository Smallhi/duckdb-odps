#pragma once

#include "odps_reader.hpp"
#include <memory>
#include <string>
#include <unordered_map>

namespace duckdb {

// Storage API specific reader implementation
class ODPSStorageReader : public ODPSReader {
public:
    ODPSStorageReader(const std::string& access_id,
                      const std::string& access_key,
                      const std::string& project,
                      const std::string& endpoint,
                      const std::string& table_name);
    
    ~ODPSStorageReader() override;
    
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
    
    // Session management
    std::string session_id_;
    int split_count_;
    int current_split_;
    int64_t total_rows_;
    
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
    
    // Arrow reader for current split
    class ArrowReader;
    std::unique_ptr<ArrowReader> current_arrow_reader_;
    
    // Internal methods
    bool CreateReadSession();
    bool GetReadSession();
    bool CreateArrowReader();
    std::string GenerateSignature(const std::string& method, const std::string& path, 
                                 const std::string& timestamp, const std::string& body = "");
    std::string GetAuthHeaders(const std::string& method, const std::string& path, 
                              const std::string& body = "");
    std::string CreateTableBatchScanRequest();
    std::string CreateReadRowsRequest(int split_index, int row_index, int row_count);
    bool ParseTableBatchScanResponse(const std::string& response);
    bool ParseSchemaFromResponse(const std::string& response);
    LogicalType MapODPSTypeToDuckDBType(const std::string& odps_type);
};

} // namespace duckdb 