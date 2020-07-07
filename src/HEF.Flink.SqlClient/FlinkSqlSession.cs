using HEF.Flink.SqlApiClient;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    internal class FlinkSqlSession : IAsyncDisposable
    {
        internal FlinkSqlSession(FlinkSqlConnectionStringBuilder connectionSettings)
        {
            ConnectionSettings = connectionSettings ?? throw new ArgumentNullException(nameof(connectionSettings));

            SqlGatewayApi = FlinkSqlGatewayApiFactory.GetSqlGatewayApi(ConnectionSettings);
        }

        internal FlinkSqlConnectionStringBuilder ConnectionSettings { get; }

        internal IFlinkSqlGatewayApi SqlGatewayApi { get; }

        internal string Version { get; private set; }

        internal string SessionId { get; private set; }

        internal async Task GetInfoAsync()
        {
            var response = await SqlGatewayApi.GetInfoAsync();

            if (response is null || string.IsNullOrWhiteSpace(response.Version))
                throw new FlinkSqlException("connect gateway api failed");

            Version = response.Version;
        }

        internal async Task ConnectAsync(CancellationToken cancellationToken)
        {
            await GetInfoAsync();

            cancellationToken.ThrowIfCancellationRequested();

            //currently the Ado.Net interfaces designed for database, not support on streaming 
            var response = await SqlGatewayApi.CreateSessionAsync(new SessionCreateRequest
            {
                Planner = ConnectionSettings.Planner,
                ExecutionType = "batch"
            });

            if (response is null || string.IsNullOrWhiteSpace(response.SessionId))
                throw new FlinkSqlException("create session failed");

            SessionId = response.SessionId;
        }

        internal Task<StatementExecuteResponse> ExecuteStatementAsync(string sqlStatement, long executionTimeoutMillis)
        {
            if (string.IsNullOrWhiteSpace(sqlStatement))
                throw new ArgumentNullException(nameof(sqlStatement));

            var requestParam = new StatementExecuteRequest
            {
                Statement = sqlStatement,
                ExecutionTimeout = executionTimeoutMillis
            };

            return SqlGatewayApi.ExecuteStatementAsync(SessionId, requestParam);
        }

        public async ValueTask DisposeAsync()
        {
            if (string.IsNullOrWhiteSpace(SessionId))
                return;

            await SqlGatewayApi.CloseSessionAsync(SessionId);

            SessionId = null;
        }
    }
}
