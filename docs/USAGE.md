# DuckDB ODPS Extension Usage Guide

本扩展推荐通过 `odps_connect` 进行凭证配置，后续所有表函数/SQL函数无需再传 access_id/access_key/project/endpoint。

## 推荐用法

```sql
-- 连接一次
SELECT odps_connect('your_access_id', 'your_access_key', 'your_project', 'https://service.cn.maxcompute.aliyun.com', 'https://dt.cn.maxcompute.aliyun.com');

-- 查询表
SELECT * FROM odps_scan(table => 'your_table');

-- 查询 SQL
SELECT * FROM odps_sql_query(sql => 'SELECT * FROM your_table LIMIT 10');

-- 断开连接
SELECT odps_disconnect();
```

如未连接，也兼容老用法（在表函数/SQL函数中传递凭证参数）。

## Installation

1. Build the extension:
```bash
make
```

2. Load the extension in DuckDB:
```sql
LOAD 'path/to/odps_extension';
```

## Configuration

The extension requires the following ODPS connection parameters:

- `access_id`: Your Aliyun Access Key ID
- `access_key`: Your Aliyun Access Key Secret
- `project`: ODPS project name
- `endpoint`: ODPS endpoint URL (e.g., `https://service.cn.maxcompute.aliyun.com`)

## Usage Examples

### 1. Connect to ODPS

```sql
-- Connect to ODPS
SELECT odps_connect('your_access_id', 'your_access_key', 'your_project', 'https://service.cn.maxcompute.aliyun.com');

-- List available tables
SELECT odps_list_tables();

-- Disconnect when done
SELECT odps_disconnect();
```

### 2. Read ODPS Table Data

The extension supports two reading methods: Storage API and Tunnel API.

#### Using Storage API (Recommended for large datasets)

```sql
-- Read all data from a table using Storage API
SELECT * FROM odps_storage_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name'
);

-- Read specific columns
SELECT * FROM odps_storage_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name',
    columns => 'col1,col2,col3'
);

-- Read with filter and partitions
SELECT * FROM odps_storage_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name',
    filter => 'col1 > 100',
    partitions => 'dt=20240101,region=cn'
);
```

#### Using Tunnel API (Alternative method)

```sql
-- Read all data from a table using Tunnel API
SELECT * FROM odps_tunnel_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com'
);

-- Read specific columns
SELECT * FROM odps_tunnel_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com',
    columns => 'col1,col2,col3'
);
```

#### Using Generic odps_scan (Auto-selects best method)

```sql
-- Auto-selects Storage API by default
SELECT * FROM odps_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name'
);

-- Explicitly specify reader type
SELECT * FROM odps_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'your_table_name',
    reader_type => 'tunnel',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com'
);
```

### 3. Execute ODPS SQL Queries

The extension supports executing ODPS SQL queries and reading their results.

#### Using SQL Query Table Function

```sql
-- Execute SQL query and read results (results read via Tunnel API)
SELECT * FROM odps_sql_query(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com',
    sql => 'SELECT user_id, count(*) as cnt FROM user_logs GROUP BY user_id HAVING cnt > 100'
);

-- Execute SQL with custom timeout
SELECT * FROM odps_sql_query(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com',
    sql => 'SELECT * FROM large_table WHERE dt = "20240101"',
    wait_for_completion => true,
    timeout_seconds => 600
);

-- Execute SQL without waiting (get instance ID)
SELECT * FROM odps_sql_query(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com',
    sql => 'SELECT * FROM large_table',
    wait_for_completion => false
);
```

#### Using SQL Execution Functions

```sql
-- Execute SQL and get instance ID
SELECT odps_execute_sql('your_access_id', 'your_access_key', 'your_project', 
                       'https://service.cn.maxcompute.aliyun.com', 
                       'SELECT * FROM test_table');

-- Get instance status
SELECT odps_get_instance_info('your_access_id', 'your_access_key', 'your_project', 
                             'https://service.cn.maxcompute.aliyun.com', 'instance_id_123');

-- List recent instances
SELECT odps_list_instances('your_access_id', 'your_access_key', 'your_project', 
                          'https://service.cn.maxcompute.aliyun.com');

-- Cancel running instance
SELECT odps_cancel_instance('your_access_id', 'your_access_key', 'your_project', 
                           'https://service.cn.maxcompute.aliyun.com', 'instance_id_123');
```

### 4. Join ODPS Data with Local Data

```sql
-- Join ODPS table with local table
SELECT o.*, l.local_column 
FROM odps_scan(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    table => 'odps_table'
) o
JOIN local_table l ON o.id = l.id;

-- Join ODPS SQL query result with local table
SELECT q.*, l.local_column 
FROM odps_sql_query(
    access_id => 'your_access_id',
    access_key => 'your_access_key',
    project => 'your_project',
    endpoint => 'https://service.cn.maxcompute.aliyun.com',
    tunnel_endpoint => 'https://dt.cn.maxcompute.aliyun.com',
    sql => 'SELECT user_id, sum(amount) as total FROM transactions GROUP BY user_id'
) q
JOIN local_table l ON q.user_id = l.user_id;
```

## Supported Data Types

The extension supports the following ODPS data types:

- `BIGINT` → `BIGINT`
- `DOUBLE` → `DOUBLE`
- `STRING` → `VARCHAR`
- `BOOLEAN` → `BOOLEAN`
- `DATETIME` → `TIMESTAMP`
- `DECIMAL` → `DECIMAL`

## Performance Considerations

1. **Column Selection**: Only select the columns you need to reduce data transfer
2. **Filtering**: Use filters to reduce the amount of data read from ODPS
3. **Partitioning**: Use partition filters to read only specific partitions
4. **Reader Selection**: 
   - Use Storage API for large datasets and when you need Arrow format
   - Use Tunnel API for smaller datasets or when Storage API is not available
5. **SQL Query Optimization**:
   - Use `wait_for_completion => false` for long-running queries to get instance ID immediately
   - Set appropriate `timeout_seconds` based on query complexity
   - Monitor instance status using `odps_get_instance_info`
   - SQL query results are read via Tunnel API (required for ODPS SQL execution)
6. **Batch Size**: The extension reads data in batches for optimal performance
7. **Connection Reuse**: Readers and executors are automatically reused for multiple queries

## Error Handling

Common error scenarios:

1. **Authentication Failed**: Check your access_id and access_key
2. **Table Not Found**: Verify the table name and project
3. **Network Issues**: Check your network connection and endpoint URL
4. **Permission Denied**: Ensure your account has read access to the table

## Limitations

1. **Read-Only**: The extension currently only supports reading data
2. **No Write Operations**: Writing data back to ODPS is not supported
3. **Limited Schema Discovery**: Some complex ODPS types may not be fully supported
4. **Network Dependency**: Requires stable network connection to ODPS
5. **SQL Execution**: SQL queries are executed asynchronously on ODPS side
6. **Result Storage**: SQL query results are stored as temporary tables in ODPS

## Troubleshooting

### Connection Issues

```sql
-- Test connection
SELECT odps_connect('access_id', 'access_key', 'project', 'endpoint');

-- Check if connected
SELECT odps_list_tables();
```

### Data Type Issues

If you encounter data type conversion issues, you can cast columns explicitly:

```sql
SELECT CAST(col1 AS VARCHAR) as col1_str,
       CAST(col2 AS BIGINT) as col2_int
FROM odps_scan(...);
```

### Performance Issues

For large tables, consider:

1. Using filters to reduce data volume
2. Selecting only necessary columns
3. Using partitioning if available
4. Running queries during off-peak hours 