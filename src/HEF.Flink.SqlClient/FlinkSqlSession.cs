using HEF.Flink.SqlApiClient;
using System;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    internal class FlinkSqlSession
    {
        internal FlinkSqlSession(FlinkSqlConnectionStringBuilder connectionSettings)
        {
            ConnectionSettings = connectionSettings ?? throw new ArgumentNullException(nameof(connectionSettings));

            SqlGatewayApi = FlinkSqlGatewayApiFactory.GetSqlGatewayApi(ConnectionSettings);
        }

        internal FlinkSqlConnectionStringBuilder ConnectionSettings { get; }

        internal IFlinkSqlGatewayApi SqlGatewayApi { get; }

        internal Task ConnectAsync()
        {
            throw new NotImplementedException();
        }
    }
}
