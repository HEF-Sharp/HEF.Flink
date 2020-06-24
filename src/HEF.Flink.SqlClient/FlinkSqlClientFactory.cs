using System.Data.Common;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlClientFactory : DbProviderFactory
    {
        private FlinkSqlClientFactory()
        {
        }

        public static readonly FlinkSqlClientFactory Instance = new FlinkSqlClientFactory();

        //public override DbCommand CreateCommand() => new FlinkSqlCommand();

        public override DbConnection CreateConnection() => new FlinkSqlConnection();

        public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new FlinkSqlConnectionStringBuilder();
    }
}
