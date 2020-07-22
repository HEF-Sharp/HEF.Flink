using HEF.Util;
using System.Threading.Tasks;
using Xunit;

namespace HEF.Flink.SqlClient.Test
{
    public class FlinkSqlClientTest
    {
        public const string ConnectionString = "DataSource=172.29.55.67;Port=8083;Planner=blink";

        [Fact]
        public async Task TestGetMetaInfo()
        {
            using var connection = new FlinkSqlConnection(ConnectionString);

            await connection.OpenAsync();

            Assert.False(string.IsNullOrWhiteSpace(connection.DataSource));
            Assert.False(string.IsNullOrWhiteSpace(connection.ServerVersion));
            Assert.False(string.IsNullOrWhiteSpace(connection.Database));
        }

        [Fact]
        public async Task TestExecuteReader()
        {
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
            var result = await createTableCommand.ExecuteNonQueryAsync();
            Assert.Equal(0, result);

            var insertSql = "INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')";
            var insertCommand = connection.CreateCommand(insertSql);
            result = await insertCommand.ExecuteNonQueryAsync();
            Assert.Equal(0, result);

            var selectSql = "SELECT * FROM T";
            var selectCommand = connection.CreateCommand(selectSql);
            using var reader = await selectCommand.ExecuteReaderAsync();
            while(await reader.ReadAsync())
            {
                Assert.True(reader.GetInt32(0) > 0);
                Assert.False(string.IsNullOrWhiteSpace(reader.GetString(1)));
            }
        }

        [Fact]
        public async Task TestReaderGetBoolean()
        {
            using var connection = new FlinkSqlConnection(ConnectionString);
            await connection.OpenAsync();

            var selectSql = "SELECT " +
                "true x, 0, '0', " +
                "CAST(NULL AS BOOLEAN)";
            var selectCommand = connection.CreateCommand(selectSql);
            using var reader = await selectCommand.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.True(reader.GetBoolean(0));
            Assert.False(reader.GetByte(1) == 1);
            Assert.False(reader.GetString(2).ParseInt() == 1) ;
            Assert.True(reader.IsDBNull(3));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task TestReaderGetString()
        {
            using var connection = new FlinkSqlConnection(ConnectionString);
            await connection.OpenAsync();

            var selectSql = "SELECT " +
                "CAST('str1' AS CHAR(4)) x, " +
                "CAST('str2' AS VARCHAR(4)), " +
                "CAST('str3' AS STRING), " +
                "CAST('str4' AS BINARY(4)), " +
                "CAST('str5' AS VARBINARY(4)), " +
                "CAST('str6' AS BYTES), " +
                "CAST(NULL AS VARCHAR(4))";
            var selectCommand = connection.CreateCommand(selectSql);
            using var reader = await selectCommand.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal("str1", reader.GetString(0));
            Assert.Equal("str2", reader.GetString(1));
            Assert.Equal("str3", reader.GetString(2));
            Assert.Equal("str4", reader.GetRawText(3).FromBase64String());
            Assert.Equal("str5", reader.GetRawText(4).FromBase64String());
            Assert.Equal("str6", reader.GetRawText(5).FromBase64String());
            Assert.True(reader.IsDBNull(6));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task TestReaderGetInteger()
        {
            using var connection = new FlinkSqlConnection(ConnectionString);
            await connection.OpenAsync();

            var selectSql = "SELECT " +
                "CAST(1 AS TINYINT) x, " +
                "CAST(2 AS SMALLINT), '3', 4, " +
                "CAST(5 AS BIGINT), " +
                "CAST(NULL AS INT)";
            var selectCommand = connection.CreateCommand(selectSql);
            using var reader = await selectCommand.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal(1, reader.GetByte(0));
            Assert.Equal(2, reader.GetInt16(1));
            Assert.Equal(3, reader.GetString(2).ParseInt());
            Assert.Equal(4, reader.GetInt32(3));
            Assert.Equal(5, reader.GetInt64(4));
            Assert.True(reader.IsDBNull(5));
            Assert.False(await reader.ReadAsync());
        }

        [Fact]
        public async Task TestReaderGetNumeric()
        {
            using var connection = new FlinkSqlConnection(ConnectionString);
            await connection.OpenAsync();

            var selectSql = "SELECT " +
                "CAST(0.2 AS FLOAT) x, " +
                "CAST(0.4 AS DOUBLE), " +
                "CAST(123.45 AS DECIMAL(5, 2)), " +
                "CAST(NULL AS DOUBLE)";
            var selectCommand = connection.CreateCommand(selectSql);
            using var reader = await selectCommand.ExecuteReaderAsync();

            Assert.True(await reader.ReadAsync());
            Assert.Equal(0.2f, reader.GetFloat(0));
            Assert.Equal(0.4d, reader.GetDouble(1));
            Assert.Equal(123.45m, reader.GetDecimal(2));            
            Assert.True(reader.IsDBNull(3));
            Assert.False(await reader.ReadAsync());
        }
    }
}
