namespace HEF.Flink.SqlClient
{
    public class FlinkSqlCommand
    {
		private FlinkSqlConnection _connection;
		private string _commandText;

		public FlinkSqlCommand()
			: this(null, null)
		{
		}

		public FlinkSqlCommand(string commandText)
			: this(commandText, null)
		{
		}

		public FlinkSqlCommand(string commandText, FlinkSqlConnection connection)
		{
			CommandText = commandText;
			Connection = connection;
		}

		public string CommandText
		{
			get => _commandText;
			set
			{
				_commandText = value;
			}
		}

		public FlinkSqlConnection Connection
        {
			get => _connection;
			set
            {
				_connection = value;
            }
        }
	}
}
