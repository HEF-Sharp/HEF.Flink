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

		#region Prepare
		public override void Prepare()
		{
			if (Connection is null)
				throw new InvalidOperationException("Connection property must be non-null.");

			if (Connection.State != ConnectionState.Open)
				throw new InvalidOperationException($"Connection must be Open; current state is {Connection.State}");

			if (string.IsNullOrWhiteSpace(CommandText))
				throw new InvalidOperationException("CommandText must be specified");
		}
		#endregion

		#region ExecuteNonQuery
		public override int ExecuteNonQuery()
		{
			Func<Task<int>> executeFunc = () => ExecuteNonQueryAsync(CancellationToken.None);

			return executeFunc.RunSync();
		}

		public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
		{
            using var reader = await ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);			

            return 0;  //currently Flink Sql not implement affected_row_count(always return -2), so just return 0
		}
		#endregion

		#region ExecuteScalar
		public override object ExecuteScalar()
		{
			Func<Task<object>> executeFunc = () => ExecuteScalarAsync(CancellationToken.None);

			return executeFunc.RunSync();
		}

		public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken)
		{
			using var reader = await ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

			if (await reader.ReadAsync(cancellationToken))
				return reader.GetValue(0);

			return null;
		}
		#endregion

		public override void Cancel()
		{
			throw new NotSupportedException("Flink Sql not supported cancel execute command");
		}

		#region Parameter
		protected override DbParameter CreateDbParameter()
		{
			throw new NotSupportedException("Flink Sql not supported parameter");
		}
		#endregion

		#region ExecuteReader
		public new FlinkSqlDataReader ExecuteReader()
			=> ExecuteReader(CommandBehavior.Default);

		public new FlinkSqlDataReader ExecuteReader(CommandBehavior behavior)
		{
			Func<Task<FlinkSqlDataReader>> executeFunc = () => ExecuteReaderAsync(behavior, CancellationToken.None);

			return executeFunc.RunSync();
		}

		public new Task<FlinkSqlDataReader> ExecuteReaderAsync()
			=> ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);

		public new Task<FlinkSqlDataReader> ExecuteReaderAsync(CommandBehavior behavior)
			=> ExecuteReaderAsync(behavior, CancellationToken.None);

		public new Task<FlinkSqlDataReader> ExecuteReaderAsync(CancellationToken cancellationToken)
			=> ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

		public new async Task<FlinkSqlDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
		{
			Prepare();

			cancellationToken.ThrowIfCancellationRequested();

			var executeResponse = await Connection.SqlSession.ExecuteStatementAsync(CommandText, CommandTimeout * 1000);

			return FlinkSqlDataReader.Create(this, behavior, executeResponse);
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
		{
			Func<Task<DbDataReader>> executeFunc = () => ExecuteDbDataReaderAsync(behavior, CancellationToken.None);

			return executeFunc.RunSync();
		}

		protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
			=> await ExecuteReaderAsync(behavior, cancellationToken);
        #endregion
	}
}