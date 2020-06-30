using HEF.Flink.SqlApiClient;
using System;
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

        internal async Task ConnectAsync()
        {
            await GetInfoAsync();

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

        public async ValueTask DisposeAsync()
        {
            if (string.IsNullOrWhiteSpace(SessionId))
                return;

            await SqlGatewayApi.CloseSessionAsync(SessionId);

            SessionId = null;
        }
    }
}
