using Microsoft.Extensions.DependencyInjection;
using System;
using Xunit;

namespace HEF.Flink.SqlClient.Test
{
    [Collection("FlinkSqlGatewayApi")]
    public class FlinkSqlGatewayApiTestBase
    {
        public FlinkSqlGatewayApiTestBase(FlinkSqlGatewayApiFixture fixture)
        {
            Provider = fixture.Provider;
        }

        protected IServiceProvider Provider { get; }

        protected IFlinkSqlGatewayApi SqlGatewayApi => Provider.GetRequiredService<IFlinkSqlGatewayApi>();
    }
}
