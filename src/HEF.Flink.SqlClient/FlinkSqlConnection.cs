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
            }
        }

        public override ConnectionState State => _connectionState;

        protected override DbProviderFactory DbProviderFactory => FlinkSqlClientFactory.Instance;

        public override string Database => throw new NotImplementedException();

        public override string DataSource => ConnectionSettings.Server;

        public override string ServerVersion => throw new NotImplementedException();

        internal FlinkSqlConnectionStringBuilder ConnectionSettings =>
            _connectionSettings ??= new FlinkSqlConnectionStringBuilder(_connectionString);

        internal FlinkSqlSession SqlSession { get; private set; }
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
                SqlSession = await CreateSessionAsync();

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
            throw new NotImplementedException();
        }

        protected override DbCommand CreateDbCommand()
        {
            throw new NotImplementedException();
        }

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

        private ValueTask<FlinkSqlSession> CreateSessionAsync()
        {
            throw new NotImplementedException();
        }
        #endregion

        #region Close
        public override void Close()
        {
            Func<Task> closeFunc = () => CloseAsync();

            closeFunc.RunSync();
        }

        public override Task CloseAsync()
        {
            throw new NotImplementedException();
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
