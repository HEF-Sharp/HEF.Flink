using Microsoft.Extensions.DependencyInjection;
using System;
using Xunit;

namespace HEF.Flink.SqlApiClient.Test
{
    public class FlinkSqlGatewayApiFixture
    {
        public FlinkSqlGatewayApiFixture()
        {
            var services = new ServiceCollection();

            services.AddHttpApi<IFlinkSqlGatewayApi>(o =>
            {
                o.HttpHost = new Uri("http://172.29.55.67:8083/v1/");
            });

            Provider = services.BuildServiceProvider();
        }

        public IServiceProvider Provider { get; }
    }

    [CollectionDefinition("FlinkSqlGatewayApi")]
    public class FlinkSqlGatewayApiCollection : ICollectionFixture<FlinkSqlGatewayApiFixture>
    {
    }
}
