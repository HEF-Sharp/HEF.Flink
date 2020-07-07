using HEF.Flink.SqlApiClient;
using HEF.Util;
using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlDataReader : DbDataReader
    {
        #region Constructor
        private FlinkSqlDataReader(FlinkSqlCommand sqlCommand, CommandBehavior behavior, string jobId, ExecuteResultSet resultSet)
        {
            Command = sqlCommand ?? throw new ArgumentNullException(nameof(sqlCommand));
            Behavior = behavior;

            JobId = jobId;
            ResultSet = resultSet;
        }

        internal static FlinkSqlDataReader Create(FlinkSqlCommand sqlCommand, CommandBehavior behavior, StatementExecuteResponse executeResponse)
        {
            if (executeResponse == null)
                throw new ArgumentNullException(nameof(executeResponse));

            var resultSet = executeResponse.Results[0];
            if (resultSet.Columns.Count == 1 && string.Compare(resultSet.Columns[0].Name, FlinkSqlConstants.Job_Id, true) == 0)
            {
                var jobId = Convert.ToString(resultSet.Data[0][0]);

                return new FlinkSqlDataReader(sqlCommand, behavior, jobId, null);
            }

            return new FlinkSqlDataReader(sqlCommand, behavior, null, resultSet);
        }
        #endregion

        public override object this[int ordinal] => throw new NotImplementedException();

        public override object this[string name] => throw new NotImplementedException();

        #region Properties
        internal FlinkSqlCommand Command { get; private set; }

        internal CommandBehavior Behavior { get; }

        internal string JobId { get; }

        internal ExecuteResultSet ResultSet { get; }

        public override int Depth => throw new NotImplementedException();

        public override int FieldCount => throw new NotImplementedException();

        public override bool HasRows => throw new NotImplementedException();

        public override bool IsClosed => throw new NotImplementedException();

        public override int RecordsAffected => throw new NotSupportedException("Flink Sql currently not implement affected_row_count");
        #endregion

        #region NextResult
        public override bool NextResult()
        {
            throw new NotSupportedException("Flink Sql not supported execute multi command in one statement");
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Flink Sql not supported execute multi command in one statement");
        }
        #endregion

        #region Read
        public override bool Read()
        {
            Func<Task<bool>> readFunc = () => ReadAsync(CancellationToken.None);

            return readFunc.RunSync();
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region GetValue
        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override T GetFieldValue<T>(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal)
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

        public override async Task CloseAsync()
        {
            if ((Behavior & CommandBehavior.CloseConnection) != 0)
                await Command.Connection.CloseAsync();

            Command = null;
        }
        #endregion

        #region IDisposable
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
                base.Dispose(disposing);
            }
        }

        public override async ValueTask DisposeAsync()
        {
            await CloseAsync();
        }
        #endregion
    }
}
