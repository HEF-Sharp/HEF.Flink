using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlMetaProvider
    {
        public FlinkSqlMetaProvider(FlinkSqlConnection connection)
        {
            Connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        public FlinkSqlConnection Connection { get; }

        #region Catalog
        public async IAsyncEnumerable<string> GetCatalogsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var reader = await Connection.CreateCommand("SHOW CATALOGS").ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
                yield return reader.GetString(0);
        }

        public Task<string> GetCurrentCatalogAsync(CancellationToken cancellationToken)
            => Connection.CreateCommand("SHOW CURRENT CATALOG").ExecuteScalarAsync<string>(cancellationToken);

        public Task ChangeCatalogAsync(string catalogName, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(catalogName))
                throw new ArgumentNullException(nameof(catalogName));

            return Connection.CreateCommand($"USE CATALOG {catalogName}").ExecuteNonQueryAsync(cancellationToken);
        }
        #endregion

        #region Database
        public async IAsyncEnumerable<string> GetDatabasesAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            using var reader = await Connection.CreateCommand("SHOW DATABASES").ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
                yield return reader.GetString(0);
        }

        public Task<string> GetCurrentDatabaseAsync(CancellationToken cancellationToken)
            => Connection.CreateCommand("SHOW CURRENT DATABASE").ExecuteScalarAsync<string>(cancellationToken);

        public Task ChangeDatabaseAsync(string databaseName, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                throw new ArgumentNullException(nameof(databaseName));

            return Connection.CreateCommand($"USE {databaseName}").ExecuteNonQueryAsync(cancellationToken);
        }
        #endregion
    }
}
