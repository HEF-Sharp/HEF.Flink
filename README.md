# HEF.Flink
[![Build status](https://ci.appveyor.com/api/projects/status/g7lgj33ow2jsy0lf?svg=true)](https://ci.appveyor.com/project/wanlitao/hef-flink)  [![License Apache](https://img.shields.io/badge/license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)

Flink dotNet sdk by HEF, currently contains Flink SQL Gateway Client and Flink Ado.Net Driver

# Flink SQL Gateway Client
[![Latest version](https://img.shields.io/nuget/v/HEF.Flink.SqlApiClient.svg)](https://www.nuget.org/packages/HEF.Flink.SqlApiClient/)

Flink SQL Gateway Client is a CSharp library for invoking Restful Api provided by [Flink SQL gateway](https://github.com/ververica/flink-sql-gateway)

## Usage
dependency injection during application startup

**Startup.cs**
```cs
services.AddHttpApi<IFlinkSqlGatewayApi>(o =>
{
    o.HttpHost = new Uri("http://localhost:8083/v1/");
});
```
then invoking by get implementation of IFlinkSqlGatewayApi
```cs
provider.GetRequiredService<IFlinkSqlGatewayApi>();
```

# Flink Ado.Net Driver
[![Latest version](https://img.shields.io/nuget/v/HEF.Flink.SqlClient.svg)](https://www.nuget.org/packages/HEF.Flink.SqlClient/)

Flink Ado.Net driver is a CSharp library for accessing and manipulating [Apache Flink](https://flink.apache.org/) clusters by connecting to a [Flink SQL gateway](https://github.com/ververica/flink-sql-gateway) as the Ado.Net server, base on the above Flink SQL Gatewy Client

## Usage
### Using DbProviderFactories
dependency injection during application startup

**Startup.cs**
```cs
DbProviderFactories.RegisterFactory("FlinkSqlClient", FlinkSqlClientFactory.Instance);
```
then invoking as follows
```cs
var factory = DbProviderFactories.GetFactory("FlinkSqlClient");
using var connection = factory.CreateConnection();
connection.ConnectionString = "DataSource=localhost;Port=8083;Planner=blink";
await connection.OpenAsync();
```

### Direct New
```cs
using var connection = new FlinkSqlConnection("DataSource=localhost;Port=8083;Planner=blink");
await connection.OpenAsync();
```

### Sample
```cs
using var connection = new FlinkSqlConnection(ConnectionString);
await connection.OpenAsync();

var createTableSql = "CREATE TABLE T(\n" +
                "  a INT,\n" +
                "  b VARCHAR(10)\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',\n" +
                "  'connector.path' = 'file:///tmp/T.csv',\n" +
                "  'format.type' = 'csv',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")";
var createTableCommand = connection.CreateCommand(createTableSql);
await createTableCommand.ExecuteNonQueryAsync();

var insertSql = "INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')";
var insertCommand = connection.CreateCommand(insertSql);
await insertCommand.ExecuteNonQueryAsync();

var selectSql = "SELECT * FROM T";
var selectCommand = connection.CreateCommand(selectSql);
using var reader = await selectCommand.ExecuteReaderAsync();
while(await reader.ReadAsync())
{
    Console.WriteLine($"{reader.GetInt32(0)}, {reader.GetString(1)}");    
}
```
