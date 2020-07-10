using HEF.Flink.SqlApiClient;
using HEF.Util;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlDataReader : DbDataReader
    {
        private string _jobId;

        private ExecuteResultSet _currentResultSet;
        private string _nextResultUri;

        private long _token = -1;
        private int _currentReadIndex = -1;

        #region Constructor
        private FlinkSqlDataReader(FlinkSqlCommand sqlCommand, CommandBehavior behavior, string jobId, ExecuteResultSet resultSet)
        {
            Command = sqlCommand ?? throw new ArgumentNullException(nameof(sqlCommand));
            Behavior = behavior;

            _jobId = jobId;
            _currentResultSet = resultSet;

            // with jobId fetch the first result to get column infos
            if (!string.IsNullOrWhiteSpace(jobId))
            {
                Func<Task> fetchJobFirstResultFunc = () => FetchJobNextResultAsync();

                fetchJobFirstResultFunc.RunSync();
            }
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

        internal FlinkSqlConnection Connection => Command.Connection;

        internal CommandBehavior Behavior { get; }

        internal IList<ColumnInfo> ColumnInfos => GetCurrentResultSet().Columns;

        public override int Depth => 0;

        public override int FieldCount => GetCurrentResultSet().Columns.Count;

        public override bool HasRows => GetCurrentResultSet().Data.Count > 0;

        public override bool IsClosed => Command is null;

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

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            VerifyNotDisposed();

            //current resultset move to next
            if (_currentResultSet != null && _currentReadIndex < _currentResultSet.Data.Count - 1)
            {
                _currentReadIndex++;
                return true;
            }

            // jobId is empty menas resultset has read to end 
            if (string.IsNullOrWhiteSpace(_jobId))
                return false;

            // nextResultUri is empty means target job has no more result
            if (string.IsNullOrWhiteSpace(_nextResultUri))
                return false;

            cancellationToken.ThrowIfCancellationRequested();
            await FetchJobNextResultAsync();

            if (_currentResultSet is null)
                return false;

            _currentReadIndex++;
            return true;
        }

        private async Task FetchJobNextResultAsync()
        {
            var fetchResponse = await Connection.SqlSession.FetchResultAsync(_jobId, ++_token);

            _nextResultUri = fetchResponse.NextResultUri;

            _currentResultSet = fetchResponse.Results is null ? null : fetchResponse.Results[0];
            _currentReadIndex = -1;
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

        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

        public override Type GetFieldType(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override T GetFieldValue<T>(int ordinal)
        {
            return typeof(T) switch
            {
                Type type when type == typeof(bool) => (T)(object)GetBoolean(ordinal),
                Type type when type == typeof(byte) => (T)(object)GetByte(ordinal),
                Type type when type == typeof(short) => (T)(object)GetInt16(ordinal),
                Type type when type == typeof(int) => (T)(object)GetInt32(ordinal),
                Type type when type == typeof(long) => (T)(object)GetInt64(ordinal),
                Type type when type == typeof(char) => (T)(object)GetChar(ordinal),
                Type type when type == typeof(decimal) => (T)(object)GetDecimal(ordinal),
                Type type when type == typeof(double) => (T)(object)GetDouble(ordinal),
                Type type when type == typeof(float) => (T)(object)GetFloat(ordinal),
                Type type when type == typeof(string) => (T)(object)GetString(ordinal),
                Type type when type == typeof(DateTime) => (T)(object)GetDateTime(ordinal),
                Type type when type == typeof(Guid) => (T)(object)GetGuid(ordinal),
                _ => base.GetFieldValue<T>(ordinal)
            };
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
            Command.Connection.FinishExecuting();

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

        #region Helper Functions
        private void VerifyNotDisposed()
        {
            if (Command is null)
                throw new InvalidOperationException("Can't call this method when FlinkSqlDataReader is closed.");
        }

        private ExecuteResultSet GetCurrentResultSet()
        {
            VerifyNotDisposed();

            if (_currentResultSet is null)
                throw new InvalidOperationException("There is no current result set.");

            return _currentResultSet;
        }
        #endregion
    }
}
