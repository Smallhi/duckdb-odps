#define DUCKDB_EXTENSION_MAIN

#include "odps_extension.hpp"
#include "odps_table_function.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
    // Register ODPS table functions
    auto odps_table_function = TableFunction("odps_scan", {}, ODPSTableFunction::ODPSScanFunction, 
                                            ODPSTableFunction::ODPSScanBind);
    odps_table_function.named_parameters["access_id"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["access_key"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["project"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["endpoint"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["table"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["reader_type"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["tunnel_endpoint"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["columns"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["filter"] = LogicalType::VARCHAR;
    odps_table_function.named_parameters["partitions"] = LogicalType::VARCHAR;
    
    ExtensionUtil::RegisterFunction(instance, odps_table_function);
    
    // Register Storage API specific table function
    auto odps_storage_table_function = TableFunction("odps_storage_scan", {}, 
                                                    ODPSStorageTableFunction::ODPSStorageScanFunction, 
                                                    ODPSStorageTableFunction::ODPSStorageScanBind);
    odps_storage_table_function.named_parameters["access_id"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["access_key"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["project"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["endpoint"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["table"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["columns"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["filter"] = LogicalType::VARCHAR;
    odps_storage_table_function.named_parameters["partitions"] = LogicalType::VARCHAR;
    
    ExtensionUtil::RegisterFunction(instance, odps_storage_table_function);
    
    // Register Tunnel API specific table function
    auto odps_tunnel_table_function = TableFunction("odps_tunnel_scan", {}, 
                                                   ODPSTunnelTableFunction::ODPSTunnelScanFunction, 
                                                   ODPSTunnelTableFunction::ODPSTunnelScanBind);
    odps_tunnel_table_function.named_parameters["access_id"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["access_key"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["project"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["endpoint"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["table"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["tunnel_endpoint"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["columns"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["filter"] = LogicalType::VARCHAR;
    odps_tunnel_table_function.named_parameters["partitions"] = LogicalType::VARCHAR;
    
    ExtensionUtil::RegisterFunction(instance, odps_tunnel_table_function);
    
    // Register ODPS SQL query table function
    auto odps_sql_table_function = TableFunction("odps_sql_query", {}, 
                                                ODPSSQLTableFunction::ODPSSQLQueryFunction, 
                                                ODPSSQLTableFunction::ODPSSQLQueryBind);
    odps_sql_table_function.named_parameters["access_id"] = LogicalType::VARCHAR;
    odps_sql_table_function.named_parameters["access_key"] = LogicalType::VARCHAR;
    odps_sql_table_function.named_parameters["project"] = LogicalType::VARCHAR;
    odps_sql_table_function.named_parameters["endpoint"] = LogicalType::VARCHAR;
    odps_sql_table_function.named_parameters["sql"] = LogicalType::VARCHAR;
    odps_sql_table_function.named_parameters["wait_for_completion"] = LogicalType::BOOLEAN;
    odps_sql_table_function.named_parameters["timeout_seconds"] = LogicalType::INTEGER;
    
    ExtensionUtil::RegisterFunction(instance, odps_sql_table_function);

    // Register ODPS connection management functions
    auto odps_connect_function = ScalarFunction("odps_connect", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::BOOLEAN, ODPSConnectionFunction::ODPSConnectFunction);
    ExtensionUtil::RegisterFunction(instance, odps_connect_function);

    auto odps_disconnect_function = ScalarFunction("odps_disconnect", {}, LogicalType::BOOLEAN, 
                                                  ODPSConnectionFunction::ODPSDisconnectFunction);
    ExtensionUtil::RegisterFunction(instance, odps_disconnect_function);

    auto odps_list_tables_function = ScalarFunction("odps_list_tables", {}, LogicalType::VARCHAR, 
                                                   ODPSConnectionFunction::ODPSListTablesFunction);
    ExtensionUtil::RegisterFunction(instance, odps_list_tables_function);
    
    // Register ODPS SQL execution functions
    auto odps_execute_sql_function = ScalarFunction("odps_execute_sql", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, ODPSConnectionFunction::ODPSExecuteSQLFunction);
    ExtensionUtil::RegisterFunction(instance, odps_execute_sql_function);
    
    auto odps_get_instance_info_function = ScalarFunction("odps_get_instance_info", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, ODPSConnectionFunction::ODPSGetInstanceInfoFunction);
    ExtensionUtil::RegisterFunction(instance, odps_get_instance_info_function);
    
    auto odps_list_instances_function = ScalarFunction("odps_list_instances", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::VARCHAR, ODPSConnectionFunction::ODPSListInstancesFunction);
    ExtensionUtil::RegisterFunction(instance, odps_list_instances_function);
    
    auto odps_cancel_instance_function = ScalarFunction("odps_cancel_instance", 
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
        LogicalType::BOOLEAN, ODPSConnectionFunction::ODPSCancelInstanceFunction);
    ExtensionUtil::RegisterFunction(instance, odps_cancel_instance_function);
}

void OdpsExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader.GetDatabaseInstance());
}

std::string OdpsExtension::Name() {
    return "odps";
}

std::string OdpsExtension::Version() const {
#ifdef EXT_VERSION_ODPS
    return EXT_VERSION_ODPS;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void odps_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OdpsExtension>();
}

DUCKDB_EXTENSION_API const char *odps_version() {
    return duckdb::DuckDB::LibraryVersion();
}

} // extern "C"

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif 