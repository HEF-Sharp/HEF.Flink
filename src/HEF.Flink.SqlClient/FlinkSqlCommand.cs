using HEF.Util;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlCommand : DbCommand
    {
		private FlinkSqlConnection _connection;

		private string _commandText;
		private int _commandTimeout;

		public FlinkSqlCommand()
		{ }

		public FlinkSqlCommand(string commandText)
			=> CommandText = commandText;

		public FlinkSqlCommand(string commandText, FlinkSqlConnection connection)
			: this(commandText)
			=> Connection = connection;

		#region Properties
		public override bool DesignTimeVisible { get; set; }

		public override UpdateRowSource UpdatedRowSource { get; set; }

		public override CommandType CommandType
		{
			get => CommandType.Text;
			set
            {
				if (value != CommandType.Text)
					throw new ArgumentException($"Flink Sql not supported CommandType: {value}");
            }
		}

        public override int CommandTimeout
        {
			get => _commandTimeout;
			set
            {
				if (value < 0)
					throw new ArgumentOutOfRangeException(nameof(value), "CommandTimeout must be greater than or equal to zero.");

				_commandTimeout = value;
			}
        }

        public override string CommandText
		{
			get => _commandText;
			set
			{
				_commandText = value;
			}
		}

		public new FlinkSqlConnection Connection
        {
			get => _connection;
			set
            {
				_connection = value;
            }
        }

		protected override DbConnection DbConnection
		{
			get => Connection;
			set => Connection = (FlinkSqlConnection)value;
		}

        protected override DbTransaction DbTransaction 
		{
			get => throw new NotSupportedException("Flink Sql not supported transaction");
			set => throw new NotSupportedException("Flink Sql not supported transaction");
		}

        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException("Flink Sql not supported parameter");
        #endregion

		public override void Prepare()
		{
			if (Connection is null)
				throw new InvalidOperationException("Connection property must be non-null.");

			if (Connection.State != ConnectionState.Open)
				throw new InvalidOperationException($"Connection must be Open; current state is {Connection.State}");

			if (string.IsNullOrWhiteSpace(CommandText))
				throw new InvalidOperationException("CommandText must be specified");
		}

        public override Task PrepareAsync(CancellationToken cancellationToken = default)
        {
			Prepare();

			return Task.CompletedTask;
        }

        public override int ExecuteNonQuery()
        {
			Func<Task<int>> executeFunc = () => ExecuteNonQueryAsync(CancellationToken.None);

			return executeFunc.RunSync();
        }

        public override Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            return base.ExecuteNonQueryAsync(cancellationToken);
        }

        public override object ExecuteScalar()
        {
			Func<Task<object>> executeFunc = () => ExecuteScalarAsync(CancellationToken.None);

			return executeFunc.RunSync();
		}

        public override Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
        {
            return base.ExecuteScalarAsync(cancellationToken);
        }

        public override void Cancel()
		{
			throw new NotImplementedException();
		}

		#region Parameter
		public new DbParameter CreateParameter() => CreateDbParameter();

		protected override DbParameter CreateDbParameter()
        {
			throw new NotSupportedException("Flink Sql not supported parameter");
        }
        #endregion

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
			Func<Task<DbDataReader>> executeFunc = () => ExecuteDbDataReaderAsync(behavior, CancellationToken.None);

			return executeFunc.RunSync();
		}

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            return base.ExecuteDbDataReaderAsync(behavior, cancellationToken);
        }
    }
}
