#pragma once

#include "duckdb.hpp"
#include "odps_reader.hpp"
#include "odps_sql_executor.hpp"

namespace duckdb {

// Table function to read ODPS tables
class ODPSTableFunction {
public:
    static TableFunctionSet GetFunctionSet();
    
private:
    static void ODPSScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output);
    static unique_ptr<FunctionData> ODPSScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names);
    static unique_ptr<TableRef> ODPSScanReplacement(ClientContext &context, const string &table_name, 
                                                   ReplacementScanData *data);
};

// Table function for Storage API
class ODPSStorageTableFunction {
public:
    static TableFunctionSet GetFunctionSet();
    
private:
    static void ODPSStorageScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output);
    static unique_ptr<FunctionData> ODPSStorageScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names);
};

// Table function for Tunnel API
class ODPSTunnelTableFunction {
public:
    static TableFunctionSet GetFunctionSet();
    
private:
    static void ODPSTunnelScanFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output);
    static unique_ptr<FunctionData> ODPSTunnelScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names);
};

// SQL query table function
class ODPSSQLTableFunction {
public:
    static TableFunctionSet GetFunctionSet();
    
private:
    static void ODPSSQLQueryFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output);
    static unique_ptr<FunctionData> ODPSSQLQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
};

// Connection management functions
class ODPSConnectionFunction {
public:
    static ScalarFunctionSet GetFunctionSet();
    
private:
    static void ODPSConnectFunction(DataChunk &args, ExpressionState &state, Vector &result);
    static void ODPSDisconnectFunction(DataChunk &args, ExpressionState &state, Vector &result);
    static void ODPSListTablesFunction(DataChunk &args, ExpressionState &state, Vector &result);
    static void ODPSExecuteSQLFunction(DataChunk &args, ExpressionState &state, Vector &result);
    static void ODPSGetInstanceInfoFunction(DataChunk &args, ExpressionState &state, Vector &result);
    static void ODPSListInstancesFunction(DataChunk &args, ExpressionState &state, Vector &result);
    static void ODPSCancelInstanceFunction(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb 