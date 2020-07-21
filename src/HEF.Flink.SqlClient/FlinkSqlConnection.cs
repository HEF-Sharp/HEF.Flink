using HEF.Util;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlConnection : DbConnection
    {
        private string _connectionString;
        private FlinkSqlConnectionStringBuilder _connectionSettings;

        private ConnectionState _connectionState;

        private FlinkSqlSession _sqlSession;
        private FlinkSqlDataReader _activeReader;
        private FlinkSqlMetaProvider _sqlMetaProvider;

        public FlinkSqlConnection()
            : this(default)
        { }

        public FlinkSqlConnection(string connectionString)
        {
            _connectionString = connectionString;
        }

        #region Properties
        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                if (_connectionState != ConnectionState.Closed)
                    throw new InvalidOperationException("Cannot change the connection string on an not closed connection.");

                _connectionString = value;
                _connectionSettings = null;
            }
        }

        public override ConnectionState State => _connectionState;

        protected override DbProviderFactory DbProviderFactory => FlinkSqlClientFactory.Instance;

        public override string Database => GetCurrentDatabase();

        public override string DataSource => ConnectionSettings.Server;

        public override string ServerVersion => SqlSession.Version;

        internal FlinkSqlConnectionStringBuilder ConnectionSettings =>
            _connectionSettings ??= new FlinkSqlConnectionStringBuilder(_connectionString);

        internal FlinkSqlSession SqlSession
        {
            get
            {
                VerifyNotDisposed();

                if (_sqlSession is null || State != ConnectionState.Open)
                    throw new InvalidOperationException($"Connection must be Open; current state is {State}");

                return _sqlSession;
            }
        }

        internal bool HasActiveReader => _activeReader != null;

        internal FlinkSqlMetaProvider SqlMetaProvider
        {
            get
            {
                VerifyNotDisposed();

                if (State != ConnectionState.Open)
                    throw new InvalidOperationException($"Connection must be Open; current state is {State}");

                return _sqlMetaProvider ??= new FlinkSqlMetaProvider(this);
            }
        }
        #endregion

        #region Open
        public override void Open()
        {
            Func<Task> openFunc = () => OpenAsync(CancellationToken.None);

            openFunc.RunSync();
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            VerifyNotDisposed();

            if (State != ConnectionState.Closed)
                throw new InvalidOperationException($"Cannot Open when State is {State}.");

            try
            {
                _sqlSession = await CreateSessionAsync(cancellationToken);

                SetState(ConnectionState.Open);
            }
            catch
            {
                SetState(ConnectionState.Closed);
                throw;
            }
        }
        #endregion

        #region Database
        public string GetCurrentDatabase()
        {
            Func<Task<string>> GetDatabaseFunc = () => SqlMetaProvider.GetCurrentDatabaseAsync(CancellationToken.None);

            return GetDatabaseFunc.RunSync();
        }

        public override void ChangeDatabase(string databaseName)
        {
            Func<Task> changeDatabaseFunc = () => SqlMetaProvider.ChangeDatabaseAsync(databaseName, CancellationToken.None);

            changeDatabaseFunc.RunSync();
        }
        #endregion

        #region Transaction
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("Flink Sql not supported transaction");
        }
        #endregion

        #region Command
        public new FlinkSqlCommand CreateCommand() => (FlinkSqlCommand)base.CreateCommand();

        public FlinkSqlCommand CreateCommand(string commandText) => new FlinkSqlCommand(commandText, this);

        protected override DbCommand CreateDbCommand() => new FlinkSqlCommand { Connection = this };
        #endregion

        #region Executing
        internal void SetActiveReader(FlinkSqlDataReader dataReader)
        {
            if (dataReader is null)
                throw new ArgumentNullException(nameof(dataReader));

            if (_activeReader != null)
                throw new InvalidOperationException("Can't replace active reader.");

            _activeReader = dataReader;
        }

        internal void FinishExecuting()
        {
            _activeReader = null;
        }
        #endregion

        #region Close
        public override void Close()
        {
            Func<Task> closeFunc = () => CloseAsync();

            closeFunc.RunSync();
        }

        public override async Task CloseAsync()
        {
            if (State == ConnectionState.Closed)
                return;

            if (_sqlSession != null)
            {
                await _sqlSession.DisposeAsync();
                _sqlSession = null;
            }

            SetState(ConnectionState.Closed);
        }
        #endregion

        #region IDisposable
        private bool _isDisposed;

        private void VerifyNotDisposed()
        {
            if (_isDisposed)
                throw new ObjectDisposedException(GetType().Name);
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (disposing)
                {
                    Func<Task> closeFunc = () => CloseAsync();

                    closeFunc.RunSync();
                }
            }
            finally
            {
                _isDisposed = true;
                base.Dispose(disposing);
            }
        }

        public override async ValueTask DisposeAsync()
        {
            try
            {
                await CloseAsync();
            }
            finally
            {
                _isDisposed = true;
            }
        }
        #endregion

        #region Helper Functions
        internal void SetState(ConnectionState newState)
        {
            if (_connectionState != newState)
            {
                var previousState = _connectionState;
                _connectionState = newState;

                var eventArgs = new StateChangeEventArgs(previousState, newState);
                OnStateChange(eventArgs);
            }
        }

        private async ValueTask<FlinkSqlSession> CreateSessionAsync(CancellationToken cancellationToken)
        {
            var sqlSession = new FlinkSqlSession(ConnectionSettings);

            await sqlSession.ConnectAsync(cancellationToken);

            return sqlSession;
        }
        #endregion
    }
}
