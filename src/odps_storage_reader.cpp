#include "odps_storage_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <nlohmann/json.hpp>

#ifdef DUCKDB_EXTENSION_MAIN
#include <httplib.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#endif

namespace duckdb {

// HTTP Client implementation
class ODPSStorageReader::HttpClient {
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
    
private:
    std::string endpoint_;
};

// Arrow Reader implementation
class ODPSStorageReader::ArrowReader {
public:
    ArrowReader(const std::string& session_id, const std::string& endpoint) 
        : session_id_(session_id), endpoint_(endpoint), current_batch_(0) {
#ifdef DUCKDB_EXTENSION_MAIN
        arrow::Status status = arrow::Initialize();
        if (!status.ok()) {
            throw Exception("Failed to initialize Arrow: " + status.ToString());
        }
#endif
    }
    
    ~ArrowReader() {
        Close();
    }
    
    bool ReadNextBatch() {
        // TODO: Implement actual Arrow batch reading from ODPS Storage API
        return false;
    }
    
#ifdef DUCKDB_EXTENSION_MAIN
    std::shared_ptr<arrow::RecordBatch> GetCurrentBatch() {
        return current_batch_;
    }
    
    std::shared_ptr<arrow::Schema> GetSchema() {
        return schema_;
    }
#endif
    
    void Close() {
        // TODO: Implement cleanup
    }
    
private:
    std::string session_id_;
    std::string endpoint_;
    int current_batch_;
    
#ifdef DUCKDB_EXTENSION_MAIN
    std::shared_ptr<arrow::RecordBatch> current_batch_;
    std::shared_ptr<arrow::Schema> schema_;
#endif
};

ODPSStorageReader::ODPSStorageReader(const std::string& access_id,
                                     const std::string& access_key,
                                     const std::string& project,
                                     const std::string& endpoint,
                                     const std::string& table_name)
    : access_id_(access_id), access_key_(access_key), project_(project), 
      endpoint_(endpoint), table_name_(table_name), split_count_(0), current_split_(0), total_rows_(0) {
    http_client_ = std::make_unique<HttpClient>(endpoint);
}

ODPSStorageReader::~ODPSStorageReader() {
    Close();
}

bool ODPSStorageReader::Initialize() {
    try {
        return CreateReadSession();
    } catch (const Exception& e) {
        return false;
    }
}

bool ODPSStorageReader::HasNext() {
    return current_split_ < split_count_;
}

bool ODPSStorageReader::ReadNextBatch(DataChunk& output) {
    if (!HasNext()) {
        return false;
    }
    
    // Create Arrow reader for current split if needed
    if (!current_arrow_reader_) {
        if (!CreateArrowReader()) {
            return false;
        }
    }
    
    // Read next batch from Arrow reader
    if (current_arrow_reader_->ReadNextBatch()) {
#ifdef DUCKDB_EXTENSION_MAIN
        auto arrow_batch = current_arrow_reader_->GetCurrentBatch();
        if (arrow_batch) {
            // TODO: Convert Arrow batch to DuckDB DataChunk
            // ConvertArrowToDuckDB(*arrow_batch, output);
            return true;
        }
#endif
    }
    
    // Move to next split
    current_split_++;
    current_arrow_reader_.reset();
    
    return HasNext();
}

void ODPSStorageReader::Close() {
    current_arrow_reader_.reset();
    session_id_.clear();
    split_count_ = 0;
    current_split_ = 0;
}

std::vector<std::string> ODPSStorageReader::GetColumnNames() const {
    return column_names_;
}

std::vector<LogicalType> ODPSStorageReader::GetColumnTypes() const {
    return column_types_;
}

int64_t ODPSStorageReader::GetTotalRows() const {
    return total_rows_;
}

void ODPSStorageReader::SetColumns(const std::vector<std::string>& columns) {
    selected_columns_ = columns;
}

void ODPSStorageReader::SetFilter(const std::string& filter) {
    filter_predicate_ = filter;
}

void ODPSStorageReader::SetPartitions(const std::vector<std::string>& partitions) {
    required_partitions_ = partitions;
}

bool ODPSStorageReader::CreateReadSession() {
    std::string request_body = CreateTableBatchScanRequest();
    std::string path = "/api/storage/v1/projects/" + project_ + "/tables/" + table_name_ + "/batchScan";
    
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string auth_header = GetAuthHeaders("POST", path, request_body);
    
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = auth_header;
    headers["Content-Type"] = "application/json";
    headers["x-odps-date"] = timestamp_str;
    
    auto response = http_client_->Post(path, request_body, headers);
    
    if (response.status_code != 200) {
        throw Exception("Failed to create read session: " + response.body);
    }
    
    return ParseTableBatchScanResponse(response.body);
}

bool ODPSStorageReader::GetReadSession() {
    if (session_id_.empty()) {
        return false;
    }
    
    std::string path = "/api/storage/v1/sessions/" + session_id_;
    std::string auth_header = GetAuthHeaders("GET", path);
    
    std::unordered_map<std::string, std::string> headers;
    headers["Authorization"] = auth_header;
    
    auto response = http_client_->Post(path, "", headers);
    
    if (response.status_code != 200) {
        return false;
    }
    
    return ParseTableBatchScanResponse(response.body);
}

bool ODPSStorageReader::CreateArrowReader() {
    if (session_id_.empty()) {
        return false;
    }
    
    current_arrow_reader_ = std::make_unique<ArrowReader>(session_id_, endpoint_);
    return true;
}

std::string ODPSStorageReader::GenerateSignature(const std::string& method, const std::string& path, 
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

std::string ODPSStorageReader::GetAuthHeaders(const std::string& method, const std::string& path, 
                                              const std::string& body) {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string timestamp_str = std::to_string(timestamp);
    
    std::string signature = GenerateSignature(method, path, timestamp_str, body);
    return "ODPS " + access_id_ + ":" + signature;
}

std::string ODPSStorageReader::CreateTableBatchScanRequest() {
    nlohmann::json request;
    
    // Required data columns
    if (selected_columns_.empty()) {
        request["RequiredDataColumns"] = nlohmann::json::array();
    } else {
        request["RequiredDataColumns"] = selected_columns_;
    }
    
    // Partition columns
    request["RequiredPartitionColumns"] = partition_columns_;
    request["RequiredPartitions"] = required_partitions_;
    request["RequiredBucketIds"] = nlohmann::json::array();
    
    // Split options
    nlohmann::json split_options;
    split_options["SplitMode"] = "Size";
    split_options["SplitNumber"] = 268435456; // 256MB
    split_options["CrossPartition"] = true;
    request["SplitOptions"] = split_options;
    
    // Arrow options
    nlohmann::json arrow_options;
    arrow_options["TimestampUnit"] = "nano";
    arrow_options["DatetimeUnit"] = "milli";
    request["ArrowOptions"] = arrow_options;
    
    // Filter predicate
    request["FilterPredicate"] = filter_predicate_;
    
    return request.dump();
}

std::string ODPSStorageReader::CreateReadRowsRequest(int split_index, int row_index, int row_count) {
    nlohmann::json request;
    request["SessionId"] = session_id_;
    request["SplitIndex"] = split_index;
    request["RowIndex"] = row_index;
    request["RowCount"] = row_count;
    request["MaxBatchRows"] = 4096;
    request["Compression"] = 2; // LZ4_FRAME
    
    nlohmann::json data_format;
    data_format["Type"] = "Arrow";
    data_format["Version"] = "1.0";
    request["DataFormat"] = data_format;
    
    return request.dump();
}

bool ODPSStorageReader::ParseTableBatchScanResponse(const std::string& response) {
    try {
        auto json = nlohmann::json::parse(response);
        
        session_id_ = json.value("SessionId", "");
        split_count_ = json.value("SplitsCount", 0);
        total_rows_ = json.value("RecordCount", 0);
        
        if (json.contains("DataSchema")) {
            ParseSchemaFromResponse(response);
        }
        
        return !session_id_.empty();
    } catch (const std::exception& e) {
        return false;
    }
}

bool ODPSStorageReader::ParseSchemaFromResponse(const std::string& response) {
    try {
        auto json = nlohmann::json::parse(response);
        auto data_schema = json["DataSchema"];
        
        // Parse data columns
        if (data_schema.contains("DataColumns")) {
            for (const auto& col : data_schema["DataColumns"]) {
                column_names_.push_back(col["Name"]);
                column_types_.push_back(MapODPSTypeToDuckDBType(col["Type"]));
            }
        }
        
        // Parse partition columns
        if (data_schema.contains("PartitionColumns")) {
            for (const auto& col : data_schema["PartitionColumns"]) {
                partition_columns_.push_back(col["Name"]);
            }
        }
        
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

LogicalType ODPSStorageReader::MapODPSTypeToDuckDBType(const std::string& odps_type) {
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