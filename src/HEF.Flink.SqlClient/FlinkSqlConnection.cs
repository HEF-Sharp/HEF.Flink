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

        public override string Database => throw new NotImplementedException();

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
                _sqlSession = await CreateSessionAsync();

                SetState(ConnectionState.Open);
            }
            catch
            {
                SetState(ConnectionState.Closed);
                throw;
            }
        }
        #endregion

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("Flink Sql not supported transaction");
        }

        protected override DbCommand CreateDbCommand() => new FlinkSqlCommand { Connection = this };

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

        private async ValueTask<FlinkSqlSession> CreateSessionAsync()
        {
            var sqlSession = new FlinkSqlSession(ConnectionSettings);

            await sqlSession.ConnectAsync();

            return sqlSession;
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
    }
}
