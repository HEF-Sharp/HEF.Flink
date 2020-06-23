using System;
using System.Threading.Tasks;
using Xunit;

namespace HEF.Flink.SqlClient.Test
{
    public class FlinkSqlGatewayTest : FlinkSqlGatewayApiTestBase
    {
        public FlinkSqlGatewayTest(FlinkSqlGatewayApiFixture fixture)
            : base(fixture)
        { }

        [Fact]
        public async Task TestGetInfo()
        {
            var response = await SqlGatewayApi.GetInfoAsync();

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.ProductName));
            Assert.False(string.IsNullOrWhiteSpace(response.Version));
        }

        #region Session
        [Fact]
        public async Task TestSessionOperation()
        {
            var response = await SqlGatewayApi.CreateSessionAsync(new SessionCreateRequest
            {
                Planner = "blink",
                ExecutionType = "batch"
            });

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.SessionId));

            await SqlGatewayApi.HeartbeatSessionAsync(response.SessionId);

            var closeResponse = await SqlGatewayApi.CloseSessionAsync(response.SessionId);

            Assert.NotNull(closeResponse);
            Assert.True(string.Compare(closeResponse.Status, "CLOSED", true) == 0);
        }
        #endregion

        #region Statement
        [Fact]
        public async Task TestExecuteStatement()
        {
            var response = await SqlGatewayApi.CreateSessionAsync(new SessionCreateRequest
            {
                Planner = "blink",
                ExecutionType = "batch"
            });

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.SessionId));

            var executeResponse = await SqlGatewayApi.ExecuteStatementAsync(response.SessionId, new StatementExecuteRequest
            {
                Statement = "CREATE TABLE T(\n" +
                            "  a INT,\n" +
                            "  b VARCHAR(10)\n" +
                            ") WITH (\n" +
                            "  'connector.type' = 'filesystem',\n" +
                            "  'connector.path' = 'file:///tmp/T.csv',\n" +
                            "  'format.type' = 'csv',\n" +
                            "  'format.derive-schema' = 'true'\n" +
                            ")",
                ExecutionTimeout = long.MaxValue
            });

            Assert.NotNull(executeResponse);
            Assert.NotNull(executeResponse.StatementTypes);
            Assert.True(string.Compare(executeResponse.StatementTypes[0], "CREATE_TABLE", true) == 0);

            executeResponse = await SqlGatewayApi.ExecuteStatementAsync(response.SessionId, new StatementExecuteRequest
            {
                Statement = "INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')",
                ExecutionTimeout = long.MaxValue
            });

            Assert.NotNull(executeResponse);
            Assert.NotNull(executeResponse.StatementTypes);
            Assert.True(string.Compare(executeResponse.StatementTypes[0], "INSERT_INTO", true) == 0);

            Assert.NotNull(executeResponse.Results);
            Assert.NotNull(executeResponse.Results[0].Data);
            var jobId = Convert.ToString(executeResponse.Results[0].Data[0][0]);
            Assert.False(string.IsNullOrWhiteSpace(jobId));

            executeResponse = await SqlGatewayApi.ExecuteStatementAsync(response.SessionId, new StatementExecuteRequest
            {
                Statement = "SELECT * FROM T",
                ExecutionTimeout = 30 * 1000L
            });

            Assert.NotNull(executeResponse);
            Assert.NotNull(executeResponse.StatementTypes);
            Assert.True(string.Compare(executeResponse.StatementTypes[0], "SELECT", true) == 0);

            Assert.NotNull(executeResponse.Results);
            Assert.NotNull(executeResponse.Results[0].Data);
            jobId = Convert.ToString(executeResponse.Results[0].Data[0][0]);
            Assert.False(string.IsNullOrWhiteSpace(jobId));

            var closeResponse = await SqlGatewayApi.CloseSessionAsync(response.SessionId);
            Assert.NotNull(closeResponse);
            Assert.True(string.Compare(closeResponse.Status, "CLOSED", true) == 0);
        }
        #endregion

        #region Job
        [Fact]
        public async Task TestFetchResult()
        {
            var response = await SqlGatewayApi.CreateSessionAsync(new SessionCreateRequest
            {
                Planner = "blink",
                ExecutionType = "batch"
            });
            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.SessionId));

            var executeResponse = await SqlGatewayApi.ExecuteStatementAsync(response.SessionId, new StatementExecuteRequest
            {
                Statement = "CREATE TABLE T(\n" +
                            "  a INT,\n" +
                            "  b VARCHAR(10)\n" +
                            ") WITH (\n" +
                            "  'connector.type' = 'filesystem',\n" +
                            "  'connector.path' = 'file:///tmp/T.csv',\n" +
                            "  'format.type' = 'csv',\n" +
                            "  'format.derive-schema' = 'true'\n" +
                            ")",
                ExecutionTimeout = long.MaxValue
            });
            Assert.NotNull(executeResponse);            

            executeResponse = await SqlGatewayApi.ExecuteStatementAsync(response.SessionId, new StatementExecuteRequest
            {
                Statement = "INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')",
                ExecutionTimeout = long.MaxValue
            });
            Assert.NotNull(executeResponse);

            executeResponse = await SqlGatewayApi.ExecuteStatementAsync(response.SessionId, new StatementExecuteRequest
            {
                Statement = "SELECT * FROM T",
                ExecutionTimeout = 30 * 1000L
            });

            Assert.NotNull(executeResponse);
            Assert.NotNull(executeResponse.StatementTypes);
            Assert.True(string.Compare(executeResponse.StatementTypes[0], "SELECT", true) == 0);

            Assert.NotNull(executeResponse.Results);
            Assert.NotNull(executeResponse.Results[0].Data);
            var jobId = Convert.ToString(executeResponse.Results[0].Data[0][0]);
            Assert.False(string.IsNullOrWhiteSpace(jobId));

            var jobStatusResponse = await SqlGatewayApi.GetJobStatusAsync(response.SessionId, jobId);
            Assert.NotNull(jobStatusResponse);
            Assert.True(string.Compare(jobStatusResponse.Status, "RUNNING", true) == 0);

            #region Fetch Result
            long token = 0;
            var fetchResponse = await SqlGatewayApi.FetchResultAsync(response.SessionId, jobId, token);

            Assert.NotNull(fetchResponse);

            while(!string.IsNullOrWhiteSpace(fetchResponse.NextResultUri))
            {
                fetchResponse = await SqlGatewayApi.FetchResultAsync(response.SessionId, jobId, ++token);

                Assert.NotNull(fetchResponse);
                if (fetchResponse.Results != null)
                {
                    foreach (var rowData in fetchResponse.Results[0].Data)
                    {
                        Assert.True(Convert.ToInt32(Convert.ToString(rowData[0])) > 0);
                        Assert.False(string.IsNullOrWhiteSpace(Convert.ToString(rowData[1])));
                    }
                }
            }
            #endregion

            jobStatusResponse = await SqlGatewayApi.CancelJobAsync(response.SessionId, jobId);
            Assert.NotNull(jobStatusResponse);
            Assert.True(string.Compare(jobStatusResponse.Status, "CANCELED", true) == 0);

            var closeResponse = await SqlGatewayApi.CloseSessionAsync(response.SessionId);
            Assert.NotNull(closeResponse);
            Assert.True(string.Compare(closeResponse.Status, "CLOSED", true) == 0);
        }
        #endregion
    }
}