#include "odps_sql_executor.hpp"
#include "odps_tunnel_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <nlohmann/json.hpp>

#ifdef DUCKDB_EXTENSION_MAIN
#include <httplib.h>
#endif

namespace duckdb {

// HTTP Client implementation
class ODPSSQLExecutor::HttpClient {
public:
    HttpClient(const std::string& endpoint) : endpoint_(endpoint) {}
    
    struct Response {
        int status_code;
        std::string body;
        std::unordered_map<std::string, std::string> headers;
    };
    
    Response Post(const std::string& path, const std::string& body, 
                  const std::unordered_map<std::string, std::string>& headers) {
#ifdef DUCKDB_EXTENSION_MAIN
        httplib::Client client(endpoint_);
        client.set_connection_timeout(30);
        client.set_read_timeout(60);
        
        // Set headers
        for (const auto& header : headers) {
            client.set_default_headers({{header.first, header.second}});
        }
        
        auto result = client.Post(path, body, "application/json");
        if (!result) {
            throw Exception("HTTP request failed: " + std::string(result.error()));
        }
        
        Response response;
        response.status_code = result->status;
        response.body = result->body;
        
        for (const auto& header : result->headers) {
            response.headers[header.first] = header.second;
        }
        
        return response;
#else
        throw Exception("HTTP client not available");
#endif
    }
    
    Response Get(const std::string& path, 
                 const std::unordered_map<std::string, std::string>& headers) {
#ifdef DUCKDB_EXTENSION_MAIN
        httplib::Client client(endpoint_);
        client.set_connection_timeout(30);
        client.set_read_timeout(60);
        
        // Set headers
        for (const auto& header : headers) {
            client.set_default_headers({{header.first, header.second}});
        }
        
        auto result = client.Get(path);
        if (!result) {
            throw Exception("HTTP request failed: " + std::string(result.error()));
        }
        
        Response response;
        response.status_code = result->status;
        response.body = result->body;
        
        for (const auto& header : result->headers) {
            response.headers[header.first] = header.second;
        }
        
        return response;
#else
        throw Exception("HTTP client not available");
#endif
    }
    
private:
    std::string endpoint_;
};

// Result Reader implementation using Tunnel API
class ODPSSQLResultReader : public ODPSSQLExecutor::ResultReader {
public:
    ODPSSQLResultReader(const std::string& instance_id, const std::string& endpoint,
                        const std::string& result_table, const std::string& access_id,
                        const std::string& access_key, const std::string& project,
                        const std::string& tunnel_endpoint)
        : ResultReader(instance_id, endpoint), result_table_(result_table),
          access_id_(access_id), access_key_(access_key), project_(project),
          tunnel_endpoint_(tunnel_endpoint) {
        
        // Create tunnel reader for the result table
        tunnel_reader_ = std::make_unique<ODPSTunnelReader>(
            access_id_, access_key_, project_, endpoint_, result_table_, tunnel_endpoint_);
    }
    
    ~ODPSSQLResultReader() override {
        Close();
    }
    
    bool ReadNextBatch(DataChunk& output) override {
        if (!tunnel_reader_) {
            return false;
        }
        
        if (!initialized_) {
            if (!tunnel_reader_->Initialize()) {
                return false;
            }
            initialized_ = true;
        }
        
        return tunnel_reader_->ReadNextBatch(output);
    }
    
    std::vector<std::string> GetColumnNames() const override {
        if (tunnel_reader_) {
            return tunnel_reader_->GetColumnNames();
        }
        return {};
    }
    
    std::vector<LogicalType> GetColumnTypes() const override {
        if (tunnel_reader_) {
            return tunnel_reader_->GetColumnTypes();
        }
        return {};
    }
    
    int64_t GetTotalRows() const override {
        if (tunnel_reader_) {
            return tunnel_reader_->GetTotalRows();
        }
        return 0;
    }
    
    void Close() override {
        if (tunnel_reader_) {
            tunnel_reader_->Close();
        }
    }
    
private:
    std::string result_table_;
    std::string access_id_;
    std::string access_key_;
    std::string project_;
    std::string tunnel_endpoint_;
    std::unique_ptr<ODPSTunnelReader> tunnel_reader_;
    bool initialized_ = false;
};

ODPSSQLExecutor::ODPSSQLExecutor(const std::string& access_id,
                                 const std::string& access_key,
                                 const std::string& project,
                                 const std::string& endpoint)
    : access_id_(access_id), access_key_(access_key), project_(project), endpoint_(endpoint) {
    http_client_ = std::make_unique<HttpClient>(endpoint);
}

ODPSSQLExecutor::~ODPSSQLExecutor() = default;

std::string ODPSSQLExecutor::ExecuteSQL(const std::string& sql) {
    std::string request_body = CreateSQLRequest(sql);
    std::string path = "/api/instances";
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string auth_header = GetAuthHeaders("POST", path, request_body);
    
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = auth_header;
    headers["Content-Type"] = "application/json";
    headers["x-odps-date"] = timestamp_str;
    
    auto response = http_client_->Post(path, request_body, headers);
    
    if (response.status_code != 200 && response.status_code != 201) {
        throw Exception("Failed to execute SQL: " + response.body);
    }
    
    try {
        auto json = nlohmann::json::parse(response.body);
        return json.value("InstanceId", "");
    } catch (const std::exception& e) {
        throw Exception("Failed to parse instance response: " + std::string(e.what()));
    }
}

ODPSInstanceInfo ODPSSQLExecutor::GetInstanceInfo(const std::string& instance_id) {
    std::string path = "/api/instances/" + instance_id;
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string auth_header = GetAuthHeaders("GET", path);
    
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = auth_header;
    headers["x-odps-date"] = timestamp_str;
    
    auto response = http_client_->Get(path, headers);
    
    if (response.status_code != 200) {
        throw Exception("Failed to get instance info: " + response.body);
    }
    
    ODPSInstanceInfo info;
    if (!ParseInstanceResponse(response.body, info)) {
        throw Exception("Failed to parse instance response");
    }
    
    return info;
}

ODPSInstanceInfo ODPSSQLExecutor::WaitForCompletion(const std::string& instance_id, int timeout_seconds) {
    auto start_time = std::chrono::steady_clock::now();
    
    while (true) {
        auto info = GetInstanceInfo(instance_id);
        
        if (info.status == ODPSInstanceStatus::SUCCESS) {
            return info;
        }
        
        if (info.status == ODPSInstanceStatus::FAILED) {
            throw Exception("SQL execution failed: " + info.error_message);
        }
        
        if (info.status == ODPSInstanceStatus::CANCELLED) {
            throw Exception("SQL execution was cancelled");
        }
        
        // Check timeout
        auto current_time = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time);
        if (elapsed.count() >= timeout_seconds) {
            throw Exception("SQL execution timeout after " + std::to_string(timeout_seconds) + " seconds");
        }
        
        // Wait before next check
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

bool ODPSSQLExecutor::CancelInstance(const std::string& instance_id) {
    std::string path = "/api/instances/" + instance_id + "/cancel";
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string auth_header = GetAuthHeaders("POST", path);
    
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = auth_header;
    headers["Content-Type"] = "application/json";
    headers["x-odps-date"] = timestamp_str;
    
    auto response = http_client_->Post(path, "", headers);
    
    return response.status_code == 200;
}

std::vector<ODPSInstanceInfo> ODPSSQLExecutor::ListInstances(int limit) {
    std::string path = "/api/instances?limit=" + std::to_string(limit);
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string auth_header = GetAuthHeaders("GET", path);
    
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = auth_header;
    headers["x-odps-date"] = timestamp_str;
    
    auto response = http_client_->Get(path, headers);
    
    if (response.status_code != 200) {
        throw Exception("Failed to list instances: " + response.body);
    }
    
    std::vector<ODPSInstanceInfo> instances;
    if (!ParseInstanceListResponse(response.body, instances)) {
        throw Exception("Failed to parse instance list response");
    }
    
    return instances;
}

std::unique_ptr<ODPSSQLExecutor::ResultReader> ODPSSQLExecutor::GetResultReader(const std::string& instance_id, const std::string& tunnel_endpoint) {
    auto info = GetInstanceInfo(instance_id);
    
    if (info.status != ODPSInstanceStatus::SUCCESS) {
        throw Exception("Instance is not in SUCCESS state: " + instance_id);
    }
    
    if (info.result_table.empty()) {
        throw Exception("No result table found for instance: " + instance_id);
    }
    
    // Use default tunnel endpoint if not provided
    std::string actual_tunnel_endpoint = tunnel_endpoint;
    if (actual_tunnel_endpoint.empty()) {
        // Extract tunnel endpoint from main endpoint
        // For example: https://service.cn.maxcompute.aliyun.com -> https://dt.cn.maxcompute.aliyun.com
        size_t pos = endpoint_.find("service.");
        if (pos != std::string::npos) {
            actual_tunnel_endpoint = endpoint_.substr(0, pos) + "dt." + endpoint_.substr(pos + 8);
        } else {
            // Fallback: use the same endpoint
            actual_tunnel_endpoint = endpoint_;
        }
    }
    
    return std::make_unique<ODPSSQLResultReader>(
        instance_id, endpoint_, info.result_table, access_id_, access_key_, project_, actual_tunnel_endpoint);
}

std::string ODPSSQLExecutor::GenerateSignature(const std::string& method, const std::string& path, 
                                               const std::string& timestamp, const std::string& body) {
    std::string string_to_sign = method + "\n" + path + "\n" + timestamp + "\n";
    if (!body.empty()) {
        string_to_sign += body;
    }
    
    unsigned char hash[SHA_DIGEST_LENGTH];
    HMAC(EVP_sha1(), access_key_.c_str(), access_key_.length(),
         reinterpret_cast<const unsigned char*>(string_to_sign.c_str()), string_to_sign.length(),
         hash, nullptr);
    
    std::stringstream ss;
    for (int i = 0; i < SHA_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    
    return ss.str();
}

std::string ODPSSQLExecutor::GetAuthHeaders(const std::string& method, const std::string& path, 
                                            const std::string& body) {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string signature = GenerateSignature(method, path, timestamp_str, body);
    return "ODPS " + access_id_ + ":" + signature;
}

std::string ODPSSQLExecutor::CreateSQLRequest(const std::string& sql) {
    nlohmann::json request;
    request["SQL"] = sql;
    request["Project"] = project_;
    
    // Add optional parameters
    nlohmann::json properties;
    properties["odps.sql.mapper.merge.limit"] = "67108864"; // 64MB
    properties["odps.sql.mapper.split.size"] = "268435456"; // 256MB
    request["Properties"] = properties;
    
    return request.dump();
}

bool ODPSSQLExecutor::ParseInstanceResponse(const std::string& response, ODPSInstanceInfo& info) {
    try {
        auto json = nlohmann::json::parse(response);
        
        info.instance_id = json.value("InstanceId", "");
        info.start_time = json.value("StartTime", "");
        info.end_time = json.value("EndTime", "");
        info.error_message = json.value("ErrorMessage", "");
        
        // Parse status
        std::string status_str = json.value("Status", "");
        if (status_str == "Running") {
            info.status = ODPSInstanceStatus::RUNNING;
        } else if (status_str == "Success") {
            info.status = ODPSInstanceStatus::SUCCESS;
        } else if (status_str == "Failed") {
            info.status = ODPSInstanceStatus::FAILED;
        } else if (status_str == "Cancelled") {
            info.status = ODPSInstanceStatus::CANCELLED;
        }
        
        // Parse result table if available
        if (json.contains("ResultTable")) {
            info.result_table = json["ResultTable"].value("TableName", "");
            info.total_rows = json["ResultTable"].value("RecordCount", 0);
            
            // Parse schema
            if (json["ResultTable"].contains("Schema")) {
                auto schema = json["ResultTable"]["Schema"];
                if (schema.contains("Columns")) {
                    for (const auto& col : schema["Columns"]) {
                        info.column_names.push_back(col["Name"]);
                        info.column_types.push_back(MapODPSTypeToDuckDBType(col["Type"]));
                    }
                }
            }
        }
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

bool ODPSSQLExecutor::ParseInstanceListResponse(const std::string& response, std::vector<ODPSInstanceInfo>& instances) {
    try {
        auto json = nlohmann::json::parse(response);
        
        if (json.contains("Instances")) {
            for (const auto& instance : json["Instances"]) {
                ODPSInstanceInfo info;
                if (ParseInstanceResponse(instance.dump(), info)) {
                    instances.push_back(info);
                }
            }
        }
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

LogicalType MapODPSTypeToDuckDBType(const std::string& odps_type) {
    if (odps_type == "BIGINT") return LogicalType::BIGINT;
    if (odps_type == "DOUBLE") return LogicalType::DOUBLE;
    if (odps_type == "STRING") return LogicalType::VARCHAR;
    if (odps_type == "BOOLEAN") return LogicalType::BOOLEAN;
    if (odps_type == "DATETIME") return LogicalType::TIMESTAMP;
    if (odps_type == "DECIMAL") return LogicalType::DECIMAL(38, 18);
    
    // Default to VARCHAR for unknown types
    return LogicalType::VARCHAR;
}

} // namespace duckdb 