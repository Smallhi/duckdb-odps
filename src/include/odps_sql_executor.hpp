#pragma once

#include "duckdb.hpp"
#include <memory>
#include <string>
#include <vector>

namespace duckdb {

// ODPS SQL Instance status
enum class ODPSInstanceStatus {
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED
};

// ODPS SQL Instance information
struct ODPSInstanceInfo {
    std::string instance_id;
    ODPSInstanceStatus status;
    std::string result_table;
    int64_t total_rows;
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
    std::string error_message;
    std::string start_time;
    std::string end_time;
};

// ODPS SQL Executor
class ODPSSQLExecutor {
public:
    ODPSSQLExecutor(const std::string& access_id,
                    const std::string& access_key,
                    const std::string& project,
                    const std::string& endpoint);
    
    ~ODPSSQLExecutor();
    
    // Execute SQL query and return instance ID
    std::string ExecuteSQL(const std::string& sql);
    
    // Get instance status
    ODPSInstanceInfo GetInstanceInfo(const std::string& instance_id);
    
    // Wait for instance completion
    ODPSInstanceInfo WaitForCompletion(const std::string& instance_id, int timeout_seconds = 300);
    
    // Cancel instance
    bool CancelInstance(const std::string& instance_id);
    
    // List instances
    std::vector<ODPSInstanceInfo> ListInstances(int limit = 100);
    
    // Get result data from instance
    class ResultReader;
    std::unique_ptr<ResultReader> GetResultReader(const std::string& instance_id, const std::string& tunnel_endpoint = "");

private:
    std::string access_id_;
    std::string access_key_;
    std::string project_;
    std::string endpoint_;
    
    // HTTP client
    class HttpClient;
    std::unique_ptr<HttpClient> http_client_;
    
    // Internal methods
    std::string GenerateSignature(const std::string& method, const std::string& path, 
                                 const std::string& timestamp, const std::string& body = "");
    std::string GetAuthHeaders(const std::string& method, const std::string& path, 
                              const std::string& body = "");
    std::string CreateSQLRequest(const std::string& sql);
    bool ParseInstanceResponse(const std::string& response, ODPSInstanceInfo& info);
    bool ParseInstanceListResponse(const std::string& response, std::vector<ODPSInstanceInfo>& instances);
};

// Result Reader for reading SQL execution results
class ODPSSQLExecutor::ResultReader {
public:
    ResultReader(const std::string& instance_id, const std::string& endpoint) 
        : instance_id_(instance_id), endpoint_(endpoint) {}
    
    virtual ~ResultReader() = default;
    
    // Read next batch of data
    virtual bool ReadNextBatch(DataChunk& output) = 0;
    
    // Get schema information
    virtual std::vector<std::string> GetColumnNames() const = 0;
    virtual std::vector<LogicalType> GetColumnTypes() const = 0;
    virtual int64_t GetTotalRows() const = 0;
    
    // Close reader
    virtual void Close() = 0;
    
protected:
    std::string instance_id_;
    std::string endpoint_;
};

} // namespace duckdb 