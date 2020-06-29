using HEF.Flink.SqlApiClient;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Concurrent;

namespace HEF.Flink.SqlClient
{
    internal static class FlinkSqlGatewayApiFactory
    {
        private static readonly ConcurrentDictionary<string, IServiceProvider> _sqlGatewayApiProviderCache =
            new ConcurrentDictionary<string, IServiceProvider>();

        internal static IFlinkSqlGatewayApi GetSqlGatewayApi(FlinkSqlConnectionStringBuilder connectionSettings)
            => GetSqlGatewayApiProvider(connectionSettings).GetRequiredService<IFlinkSqlGatewayApi>();

        internal static IServiceProvider GetSqlGatewayApiProvider(FlinkSqlConnectionStringBuilder connectionSettings)
        {
            if (connectionSettings == null)
                throw new ArgumentNullException(nameof(connectionSettings));

            var connectionSettingsCacheKey = GetConnectionSettingsCacheKey(connectionSettings);

            return _sqlGatewayApiProviderCache.GetOrAdd(connectionSettingsCacheKey, key => BuildSqlGatewayApiProvider(connectionSettings));
        }

        private static string GetConnectionSettingsCacheKey(FlinkSqlConnectionStringBuilder connectionSettings)
        {
            var newConnectionSettings = new FlinkSqlConnectionStringBuilder(connectionSettings.ConnectionString);
            newConnectionSettings.Remove(FlinkSqlConnectionStringOption.Planner.Key);

            return newConnectionSettings.ConnectionString;
        }

        private static IServiceProvider BuildSqlGatewayApiProvider(FlinkSqlConnectionStringBuilder connectionSettings)
        {
            var services = new ServiceCollection();

            services.AddHttpApi<IFlinkSqlGatewayApi>(o =>
            {
                o.HttpHost = new Uri($"http://{connectionSettings.Server}:{connectionSettings.Port}/v1/");
            });

            return services.BuildServiceProvider();
        }
    }
}
