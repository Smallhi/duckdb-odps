#include "odps_table_function.hpp"
#include "odps_reader.hpp"
#include "odps_storage_reader.hpp"
#include "odps_tunnel_reader.hpp"
#include "odps_sql_executor.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/table_ref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include <memory>
#include <unordered_map>
#include "quack_extension.hpp"

namespace duckdb {

// Global reader manager
static std::unordered_map<std::string, std::unique_ptr<ODPSReader>> readers;

// Global SQL executor manager
static std::unordered_map<std::string, std::unique_ptr<ODPSSQLExecutor>> sql_executors;

// 全局凭证存储在 ClientContext 的 client_data 中
struct ODPSStateWrapper {
    ODPSGlobalState state;
};

ODPSGlobalState &GetODPSState(ClientContext &context) {
    auto &client_data = ClientData::Get(context);
    if (!client_data.execution_context) {
        client_data.execution_context = std::make_unique<ODPSStateWrapper>();
    }
    return ((ODPSStateWrapper *)client_data.execution_context.get())->state;
}

void SetODPSState(ClientContext &context, const ODPSGlobalState &state) {
    auto &client_data = ClientData::Get(context);
    if (!client_data.execution_context) {
        client_data.execution_context = std::make_unique<ODPSStateWrapper>();
    }
    ((ODPSStateWrapper *)client_data.execution_context.get())->state = state;
}

struct ODPSFunctionData : public FunctionData {
    ODPSReaderType reader_type;
    std::string access_id;
    std::string access_key;
    std::string project;
    std::string endpoint;
    std::string table_name;
    std::string tunnel_endpoint;
    std::vector<std::string> columns;
    std::string filter;
    std::vector<std::string> partitions;
    
    ODPSFunctionData(ODPSReaderType reader_type, const std::string& access_id, const std::string& access_key,
                     const std::string& project, const std::string& endpoint, const std::string& table_name,
                     const std::string& tunnel_endpoint, const std::vector<std::string>& columns, 
                     const std::string& filter, const std::vector<std::string>& partitions)
        : reader_type(reader_type), access_id(access_id), access_key(access_key), project(project),
          endpoint(endpoint), table_name(table_name), tunnel_endpoint(tunnel_endpoint),
          columns(columns), filter(filter), partitions(partitions) {}
    
    unique_ptr<FunctionData> Copy() const override {
        return make_uniq<ODPSFunctionData>(reader_type, access_id, access_key, project, endpoint,
                                          table_name, tunnel_endpoint, columns, filter, partitions);
    }
    
    bool Equals(const FunctionData &other_p) const override {
        auto &other = (const ODPSFunctionData &)other_p;
        return reader_type == other.reader_type &&
               access_id == other.access_id &&
               project == other.project &&
               table_name == other.table_name;
    }
};

void ODPSTableFunction::ODPSScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &data = (ODPSFunctionData &)*input.bind_data;
    
    // Create reader key
    std::string reader_key = data.access_id + "@" + data.project + "@" + data.table_name + "@" + 
                            std::to_string(static_cast<int>(data.reader_type));
    
    // Get or create reader
    if (readers.find(reader_key) == readers.end()) {
        readers[reader_key] = ODPSReaderFactory::CreateReader(
            data.reader_type, data.access_id, data.access_key, data.project, 
            data.endpoint, data.table_name, data.tunnel_endpoint);
        
        // Configure reader
        readers[reader_key]->SetColumns(data.columns);
        readers[reader_key]->SetFilter(data.filter);
        readers[reader_key]->SetPartitions(data.partitions);
        
        // Initialize reader
        if (!readers[reader_key]->Initialize()) {
            throw Exception("Failed to initialize ODPS reader");
        }
    }
    
    // Read next batch
    if (!readers[reader_key]->ReadNextBatch(output)) {
        // No more data, remove reader
        readers.erase(reader_key);
        output.SetCardinality(0);
    }
}

unique_ptr<FunctionData> ODPSTableFunction::ODPSScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
    // 优先从 session/global 读取凭证
    auto &odps_state = GetODPSState(context);
    std::string access_id = odps_state.access_id;
    std::string access_key = odps_state.access_key;
    std::string project = odps_state.project;
    std::string endpoint = odps_state.endpoint;
    std::string tunnel_endpoint = odps_state.tunnel_endpoint;
    if (!odps_state.connected) {
        access_id = input.named_parameters["access_id"].GetValue<string>();
        access_key = input.named_parameters["access_key"].GetValue<string>();
        project = input.named_parameters["project"].GetValue<string>();
        endpoint = input.named_parameters["endpoint"].GetValue<string>();
        if (input.named_parameters.find("tunnel_endpoint") != input.named_parameters.end()) {
            tunnel_endpoint = input.named_parameters["tunnel_endpoint"].GetValue<string>();
        }
    }
    // 其它参数照常
    auto table_name = input.named_parameters["table"].GetValue<string>();
    std::vector<std::string> columns;
    if (input.named_parameters.find("columns") != input.named_parameters.end()) {
        columns = StringUtil::Split(input.named_parameters["columns"].GetValue<string>(), ",");
    }
    std::string filter;
    if (input.named_parameters.find("filter") != input.named_parameters.end()) {
        filter = input.named_parameters["filter"].GetValue<string>();
    }
    std::vector<std::string> partitions;
    if (input.named_parameters.find("partitions") != input.named_parameters.end()) {
        partitions = StringUtil::Split(input.named_parameters["partitions"].GetValue<string>(), ",");
    }
    auto reader_type = ODPSReaderType::STORAGE_API;
    if (input.named_parameters.find("reader_type") != input.named_parameters.end()) {
        auto type_str = input.named_parameters["reader_type"].GetValue<string>();
        if (type_str == "tunnel") reader_type = ODPSReaderType::TUNNEL;
    }
    return make_uniq<ODPSFunctionData>(reader_type, access_id, access_key, project, endpoint, table_name, tunnel_endpoint, columns, filter, partitions);
}

unique_ptr<TableRef> ODPSTableFunction::ODPSScanReplacement(ClientContext &context, const string &table_name, 
                                                           ReplacementScanData *data) {
    // This function allows using ODPS tables directly in SQL
    // For example: SELECT * FROM odps_table_name
    
    // TODO: Implement table replacement logic
    // This would involve:
    // 1. Checking if the table name matches ODPS pattern
    // 2. Extracting connection parameters from context or configuration
    // 3. Creating a table function reference
    
    return nullptr;
}

TableFunctionSet ODPSTableFunction::GetFunctionSet() {
    TableFunctionSet set("odps_scan");
    set.AddFunction(TableFunction({}, nullptr, ODPSScanBind, ODPSScanFunction));
    return set;
}

// Storage API specific table function
void ODPSStorageTableFunction::ODPSStorageScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &data = (ODPSFunctionData &)*input.bind_data;
    
    // Force reader type to STORAGE_API
    data.reader_type = ODPSReaderType::STORAGE_API;
    
    // Use the same implementation as ODPSScanFunction
    ODPSTableFunction::ODPSScanFunction(context, input, output);
}

unique_ptr<FunctionData> ODPSStorageTableFunction::ODPSStorageScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                                       vector<LogicalType> &return_types, vector<string> &names) {
    // Force reader type to STORAGE_API
    input.named_parameters["reader_type"] = Value("storage");
    
    return ODPSTableFunction::ODPSScanBind(context, input, return_types, names);
}

TableFunctionSet ODPSStorageTableFunction::GetFunctionSet() {
    TableFunctionSet set("odps_storage_scan");
    set.AddFunction(TableFunction({}, nullptr, ODPSStorageScanBind, ODPSStorageScanFunction));
    return set;
}

// Tunnel API specific table function
void ODPSTunnelTableFunction::ODPSTunnelScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &data = (ODPSFunctionData &)*input.bind_data;
    
    // Force reader type to TUNNEL
    data.reader_type = ODPSReaderType::TUNNEL;
    
    // Use the same implementation as ODPSScanFunction
    ODPSTableFunction::ODPSScanFunction(context, input, output);
}

unique_ptr<FunctionData> ODPSTunnelTableFunction::ODPSTunnelScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                                    vector<LogicalType> &return_types, vector<string> &names) {
    // Force reader type to TUNNEL
    input.named_parameters["reader_type"] = Value("tunnel");
    
    return ODPSTableFunction::ODPSScanBind(context, input, return_types, names);
}

TableFunctionSet ODPSTunnelTableFunction::GetFunctionSet() {
    TableFunctionSet set("odps_tunnel_scan");
    set.AddFunction(TableFunction({}, nullptr, ODPSTunnelScanBind, ODPSTunnelScanFunction));
    return set;
}

// SQL Query Table Function implementation
struct ODPSSQLFunctionData : public FunctionData {
    std::string access_id;
    std::string access_key;
    std::string project;
    std::string endpoint;
    std::string tunnel_endpoint;
    std::string sql;
    std::string instance_id;
    bool wait_for_completion;
    int timeout_seconds;
    
    ODPSSQLFunctionData(const std::string& access_id, const std::string& access_key,
                        const std::string& project, const std::string& endpoint,
                        const std::string& tunnel_endpoint, const std::string& sql, 
                        bool wait_for_completion, int timeout_seconds)
        : access_id(access_id), access_key(access_key), project(project), endpoint(endpoint),
          tunnel_endpoint(tunnel_endpoint), sql(sql), wait_for_completion(wait_for_completion), 
          timeout_seconds(timeout_seconds) {}
    
    unique_ptr<FunctionData> Copy() const override {
        return make_uniq<ODPSSQLFunctionData>(access_id, access_key, project, endpoint,
                                             tunnel_endpoint, sql, wait_for_completion, timeout_seconds);
    }
    
    bool Equals(const FunctionData &other_p) const override {
        auto &other = (const ODPSSQLFunctionData &)other_p;
        return access_id == other.access_id &&
               project == other.project &&
               sql == other.sql;
    }
};

void ODPSSQLTableFunction::ODPSSQLQueryFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
    auto &data = (ODPSSQLFunctionData &)*input.bind_data;
    // 优先用 session/global 凭证
    auto &odps_state = GetODPSState(context);
    std::string access_id = data.access_id;
    std::string access_key = data.access_key;
    std::string project = data.project;
    std::string endpoint = data.endpoint;
    std::string tunnel_endpoint = data.tunnel_endpoint;
    if (odps_state.connected) {
        access_id = odps_state.access_id;
        access_key = odps_state.access_key;
        project = odps_state.project;
        endpoint = odps_state.endpoint;
        tunnel_endpoint = odps_state.tunnel_endpoint;
    }
    // Create executor key
    std::string executor_key = access_id + "@" + project + "@" + endpoint;
    // Get or create SQL executor
    if (sql_executors.find(executor_key) == sql_executors.end()) {
        sql_executors[executor_key] = std::make_unique<ODPSSQLExecutor>(
            access_id, access_key, project, endpoint);
    }
    // Execute SQL if instance_id is empty
    if (data.instance_id.empty()) {
        data.instance_id = sql_executors[executor_key]->ExecuteSQL(data.sql);
        // Wait for completion if requested
        if (data.wait_for_completion) {
            try {
                sql_executors[executor_key]->WaitForCompletion(data.instance_id, data.timeout_seconds);
            } catch (const Exception& e) {
                throw Exception("SQL execution failed: " + std::string(e.what()));
            }
        }
    }
    // Get result reader
    static std::unordered_map<std::string, std::unique_ptr<ODPSSQLExecutor::ResultReader>> result_readers;
    if (result_readers.find(data.instance_id) == result_readers.end()) {
        result_readers[data.instance_id] = sql_executors[executor_key]->GetResultReader(data.instance_id, tunnel_endpoint);
    }
    // Read next batch
    if (!result_readers[data.instance_id]->ReadNextBatch(output)) {
        // No more data, remove reader
        result_readers.erase(data.instance_id);
        output.SetCardinality(0);
    }
}

unique_ptr<FunctionData> ODPSSQLTableFunction::ODPSSQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                               vector<LogicalType> &return_types, vector<string> &names) {
    // 优先从 session/global 读取凭证
    auto &odps_state = GetODPSState(context);
    std::string access_id = odps_state.access_id;
    std::string access_key = odps_state.access_key;
    std::string project = odps_state.project;
    std::string endpoint = odps_state.endpoint;
    std::string tunnel_endpoint = odps_state.tunnel_endpoint;
    if (!odps_state.connected) {
        // 兼容老用法，允许参数传递
        access_id = input.named_parameters["access_id"].GetValue<string>();
        access_key = input.named_parameters["access_key"].GetValue<string>();
        project = input.named_parameters["project"].GetValue<string>();
        endpoint = input.named_parameters["endpoint"].GetValue<string>();
        if (input.named_parameters.find("tunnel_endpoint") != input.named_parameters.end()) {
            tunnel_endpoint = input.named_parameters["tunnel_endpoint"].GetValue<string>();
        }
    }
    auto sql = input.named_parameters["sql"].GetValue<string>();
    bool wait_for_completion = true;
    if (input.named_parameters.find("wait_for_completion") != input.named_parameters.end()) {
        wait_for_completion = input.named_parameters["wait_for_completion"].GetValue<bool>();
    }
    int timeout_seconds = 300;
    if (input.named_parameters.find("timeout_seconds") != input.named_parameters.end()) {
        timeout_seconds = input.named_parameters["timeout_seconds"].GetValue<int>();
    }
    // Create SQL executor to get schema
    auto executor = std::make_unique<ODPSSQLExecutor>(access_id, access_key, project, endpoint);
    // Execute SQL to get schema
    std::string instance_id = executor->ExecuteSQL(sql);
    // Wait for completion to get result schema
    ODPSInstanceInfo info;
    if (wait_for_completion) {
        info = executor->WaitForCompletion(instance_id, timeout_seconds);
    } else {
        info = executor->GetInstanceInfo(instance_id);
    }
    // Set return types and names based on result schema
    names = info.column_names;
    return_types = info.column_types;
    return make_uniq<ODPSSQLFunctionData>(access_id, access_key, project, endpoint,
                                         tunnel_endpoint, sql, wait_for_completion, timeout_seconds);
}

TableFunctionSet ODPSSQLTableFunction::GetFunctionSet() {
    TableFunctionSet set("odps_sql_query");
    set.AddFunction(TableFunction({}, nullptr, ODPSSQLQueryBind, ODPSSQLQueryFunction));
    return set;
}

// Connection management functions
void ODPSConnectionFunction::ODPSConnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    auto access_id = args.data[0].GetValue(0).GetValue<string>();
    auto access_key = args.data[1].GetValue(0).GetValue<string>();
    auto project = args.data[2].GetValue(0).GetValue<string>();
    auto endpoint = args.data[3].GetValue(0).GetValue<string>();
    std::string tunnel_endpoint;
    if (args.ColumnCount() > 4) {
        tunnel_endpoint = args.data[4].GetValue(0).GetValue<string>();
    }
    ODPSGlobalState odps_state;
    odps_state.access_id = access_id;
    odps_state.access_key = access_key;
    odps_state.project = project;
    odps_state.endpoint = endpoint;
    odps_state.tunnel_endpoint = tunnel_endpoint;
    odps_state.connected = true;
    SetODPSState(context, odps_state);
    result.SetValue(0, Value::BOOLEAN(true));
}

void ODPSConnectionFunction::ODPSDisconnectFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &context = state.GetContext();
    ODPSGlobalState odps_state;
    odps_state.connected = false;
    SetODPSState(context, odps_state);
    result.SetValue(0, Value::BOOLEAN(true));
}

void ODPSConnectionFunction::ODPSListTablesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // TODO: Implement list tables function
    // This would return a list of available tables
    
    result.SetValue(0, Value("[]"));
}

void ODPSConnectionFunction::ODPSExecuteSQLFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Extract parameters
    auto access_id = args.data[0].GetValue(0).GetValue<string>();
    auto access_key = args.data[1].GetValue(0).GetValue<string>();
    auto project = args.data[2].GetValue(0).GetValue<string>();
    auto endpoint = args.data[3].GetValue(0).GetValue<string>();
    auto sql = args.data[4].GetValue(0).GetValue<string>();
    
    // Create executor key
    std::string executor_key = access_id + "@" + project + "@" + endpoint;
    
    // Get or create SQL executor
    if (sql_executors.find(executor_key) == sql_executors.end()) {
        sql_executors[executor_key] = std::make_unique<ODPSSQLExecutor>(
            access_id, access_key, project, endpoint);
    }
    
    // Execute SQL
    std::string instance_id = sql_executors[executor_key]->ExecuteSQL(sql);
    
    result.SetValue(0, Value(instance_id));
}

void ODPSConnectionFunction::ODPSGetInstanceInfoFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Extract parameters
    auto access_id = args.data[0].GetValue(0).GetValue<string>();
    auto access_key = args.data[1].GetValue(0).GetValue<string>();
    auto project = args.data[2].GetValue(0).GetValue<string>();
    auto endpoint = args.data[3].GetValue(0).GetValue<string>();
    auto instance_id = args.data[4].GetValue(0).GetValue<string>();
    
    // Create executor key
    std::string executor_key = access_id + "@" + project + "@" + endpoint;
    
    // Get or create SQL executor
    if (sql_executors.find(executor_key) == sql_executors.end()) {
        sql_executors[executor_key] = std::make_unique<ODPSSQLExecutor>(
            access_id, access_key, project, endpoint);
    }
    
    // Get instance info
    auto info = sql_executors[executor_key]->GetInstanceInfo(instance_id);
    
    // Return status as string
    std::string status_str;
    switch (info.status) {
        case ODPSInstanceStatus::RUNNING:
            status_str = "RUNNING";
            break;
        case ODPSInstanceStatus::SUCCESS:
            status_str = "SUCCESS";
            break;
        case ODPSInstanceStatus::FAILED:
            status_str = "FAILED";
            break;
        case ODPSInstanceStatus::CANCELLED:
            status_str = "CANCELLED";
            break;
    }
    
    result.SetValue(0, Value(status_str));
}

void ODPSConnectionFunction::ODPSListInstancesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Extract parameters
    auto access_id = args.data[0].GetValue(0).GetValue<string>();
    auto access_key = args.data[1].GetValue(0).GetValue<string>();
    auto project = args.data[2].GetValue(0).GetValue<string>();
    auto endpoint = args.data[3].GetValue(0).GetValue<string>();
    
    int limit = 100;
    if (args.data.size() > 4) {
        limit = args.data[4].GetValue(0).GetValue<int>();
    }
    
    // Create executor key
    std::string executor_key = access_id + "@" + project + "@" + endpoint;
    
    // Get or create SQL executor
    if (sql_executors.find(executor_key) == sql_executors.end()) {
        sql_executors[executor_key] = std::make_unique<ODPSSQLExecutor>(
            access_id, access_key, project, endpoint);
    }
    
    // List instances
    auto instances = sql_executors[executor_key]->ListInstances(limit);
    
    // Return instance IDs as JSON array
    nlohmann::json json_array = nlohmann::json::array();
    for (const auto& instance : instances) {
        json_array.push_back(instance.instance_id);
    }
    
    result.SetValue(0, Value(json_array.dump()));
}

void ODPSConnectionFunction::ODPSCancelInstanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    // Extract parameters
    auto access_id = args.data[0].GetValue(0).GetValue<string>();
    auto access_key = args.data[1].GetValue(0).GetValue<string>();
    auto project = args.data[2].GetValue(0).GetValue<string>();
    auto endpoint = args.data[3].GetValue(0).GetValue<string>();
    auto instance_id = args.data[4].GetValue(0).GetValue<string>();
    
    // Create executor key
    std::string executor_key = access_id + "@" + project + "@" + endpoint;
    
    // Get or create SQL executor
    if (sql_executors.find(executor_key) == sql_executors.end()) {
        sql_executors[executor_key] = std::make_unique<ODPSSQLExecutor>(
            access_id, access_key, project, endpoint);
    }
    
    // Cancel instance
    bool success = sql_executors[executor_key]->CancelInstance(instance_id);
    
    result.SetValue(0, Value::BOOLEAN(success));
}

ScalarFunctionSet ODPSConnectionFunction::GetFunctionSet() {
    ScalarFunctionSet set("odps_connection");
    set.AddFunction(ScalarFunction("odps_connect", {}, LogicalType::BOOLEAN, ODPSConnectFunction));
    set.AddFunction(ScalarFunction("odps_disconnect", {}, LogicalType::BOOLEAN, ODPSDisconnectFunction));
    set.AddFunction(ScalarFunction("odps_list_tables", {}, LogicalType::VARCHAR, ODPSListTablesFunction));
    
    // SQL execution functions
    set.AddFunction(ScalarFunction("odps_execute_sql", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, ODPSExecuteSQLFunction));
    set.AddFunction(ScalarFunction("odps_get_instance_info", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, ODPSGetInstanceInfoFunction));
    set.AddFunction(ScalarFunction("odps_list_instances", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, ODPSListInstancesFunction));
    set.AddFunction(ScalarFunction("odps_cancel_instance", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::BOOLEAN, ODPSCancelInstanceFunction));
    return set;
}

} // namespace duckdb 