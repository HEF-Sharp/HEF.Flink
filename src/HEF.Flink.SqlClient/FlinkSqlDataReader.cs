using HEF.Flink.SqlApiClient;
using HEF.Util;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlDataReader : DbDataReader
    {
        internal static readonly UTF8Encoding Utf8Encoding = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false);

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

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));
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
            if (fetchResponse is null)
                throw new FlinkSqlException("fetch result failed");

            _nextResultUri = fetchResponse.NextResultUri;

            _currentResultSet = fetchResponse.Results is null ? null : fetchResponse.Results[0];
            _currentReadIndex = -1;
        }
        #endregion

        #region GetValue
        public override bool GetBoolean(int ordinal)
            => GetJsonElementValue(ordinal).GetBoolean();

        public override byte GetByte(int ordinal)
            => GetJsonElementValue(ordinal).GetByte();        

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            var sqlDataType = GetFieldDataType(ordinal);

            if (sqlDataType.ClrType != typeof(byte[]) && sqlDataType.ClrType != typeof(string))
                throw new InvalidCastException($"can't convert {sqlDataType} to bytes");

            var rawBytes = GetRawBytes(ordinal);

            if (buffer is null) //according to SqlDataReader.GetBytes behavior
                return rawBytes.Length;

            CheckBufferArguments(dataOffset, buffer, bufferOffset, length);

            var offset = (int)dataOffset;
            var lengthToCopy = Math.Max(0, Math.Min(rawBytes.Length - offset, length));
            if (lengthToCopy > 0)
            {
                rawBytes.AsSpan().Slice(offset, lengthToCopy).CopyTo(buffer.AsSpan().Slice(bufferOffset));
                return lengthToCopy;
            }

            return 0;
        }

        public override char GetChar(int ordinal)
        {
            var stringValue = GetString(ordinal);

            if (string.IsNullOrWhiteSpace(stringValue))
                throw new InvalidCastException("the target string value is null or empty");

            return stringValue[0];
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            var stringValue = GetString(ordinal) ?? string.Empty;

            if (buffer is null)  //according to SqlDataReader.GetChars behavior
                return stringValue.Length;

            CheckBufferArguments(dataOffset, buffer, bufferOffset, length);

            var offset = (int)dataOffset;
            var lengthToCopy = Math.Max(0, Math.Min(stringValue.Length - offset, length));
            if (lengthToCopy > 0)
            {
                stringValue.CopyTo(offset, buffer, bufferOffset, lengthToCopy);
                return lengthToCopy;
            }

            return 0;
        }

        public override DateTime GetDateTime(int ordinal)
            => GetJsonElementValue(ordinal).GetDateTime();

        public override decimal GetDecimal(int ordinal)
            => GetJsonElementValue(ordinal).GetDecimal();

        public override double GetDouble(int ordinal)
            => GetJsonElementValue(ordinal).GetDouble();

        public override float GetFloat(int ordinal)
            => GetJsonElementValue(ordinal).GetSingle();

        public override Guid GetGuid(int ordinal)
            => GetJsonElementValue(ordinal).GetGuid();

        public override short GetInt16(int ordinal)
            => GetJsonElementValue(ordinal).GetInt16();

        public override int GetInt32(int ordinal)
            => GetJsonElementValue(ordinal).GetInt32();

        public override long GetInt64(int ordinal)
            => GetJsonElementValue(ordinal).GetInt64();

        public override string GetString(int ordinal)
            => GetJsonElementValue(ordinal).GetString();

        public override bool IsDBNull(int ordinal)
            => GetJsonElementValue(ordinal).ValueKind == JsonValueKind.Null;

        public override IEnumerator GetEnumerator() => new DbEnumerator(this, closeReader: false);

        public override int GetOrdinal(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            for (var index = 0; index < ColumnInfos.Count; index++)
            {
                if (string.Compare(name, ColumnInfos[index].Name, true) == 0)
                    return index;
            }

            throw new IndexOutOfRangeException($"The column name '{name}' does not exist in the result set.");
        }

        public override string GetName(int ordinal)
            => GetFieldColumn(ordinal).Name;        

        public override string GetDataTypeName(int ordinal)
            => GetFieldType(ordinal).Name;

        public override Type GetFieldType(int ordinal)
            => GetFieldDataType(ordinal).ClrType;

        public override object GetValue(int ordinal)
        {
            var fieldType = GetFieldType(ordinal);

            return fieldType switch
            {
                Type type when type == typeof(bool) => GetBoolean(ordinal),
                Type type when type == typeof(string) => GetString(ordinal),
                Type type when type == typeof(byte[]) => GetRawBytes(ordinal),
                Type type when type == typeof(decimal) => GetDecimal(ordinal),
                Type type when type == typeof(byte) => GetByte(ordinal),
                Type type when type == typeof(short) => GetInt16(ordinal),
                Type type when type == typeof(int) => GetInt32(ordinal),
                Type type when type == typeof(long) => GetInt64(ordinal),
                Type type when type == typeof(float) => GetFloat(ordinal),
                Type type when type == typeof(double) => GetDouble(ordinal),
                Type type when type == typeof(DateTime) => GetDateTime(ordinal),
                Type type when type == typeof(DateTimeOffset) => GetDateTimeOffset(ordinal),

                _ => GetJsonElementValue(ordinal)
            };
        }

        public override int GetValues(object[] values)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            var count = Math.Min(values.Length, ColumnInfos.Count);

            for (int i = 0; i < count; i++)
                values[i] = GetValue(i);

            return count;
        }

        public override T GetFieldValue<T>(int ordinal)
        {
            return typeof(T) switch
            {
                Type type when type == typeof(bool) => ConvertChangeType<bool, T>(GetBoolean(ordinal)),
                Type type when type == typeof(byte) => ConvertChangeType<byte, T>(GetByte(ordinal)),
                Type type when type == typeof(short) => ConvertChangeType<short, T>(GetInt16(ordinal)),
                Type type when type == typeof(int) => ConvertChangeType<int, T>(GetInt32(ordinal)),
                Type type when type == typeof(long) => ConvertChangeType<long, T>(GetInt64(ordinal)),                
                Type type when type == typeof(decimal) => ConvertChangeType<decimal, T>(GetDecimal(ordinal)),
                Type type when type == typeof(double) => ConvertChangeType<double, T>(GetDouble(ordinal)),
                Type type when type == typeof(float) => ConvertChangeType<float, T>(GetFloat(ordinal)),
                Type type when type == typeof(char) => ConvertChangeType<char, T>(GetChar(ordinal)),
                Type type when type == typeof(string) => ConvertChangeType<string, T>(GetString(ordinal)),
                Type type when type == typeof(DateTime) => ConvertChangeType<DateTime, T>(GetDateTime(ordinal)),
                Type type when type == typeof(Guid) => ConvertChangeType<Guid, T>(GetGuid(ordinal)),

                Type type when type == typeof(sbyte) => ConvertChangeType<sbyte, T>(GetSByte(ordinal)),
                Type type when type == typeof(DateTimeOffset) => ConvertChangeType<DateTimeOffset, T>(GetDateTimeOffset(ordinal)),
                Type type when type == typeof(ushort) => ConvertChangeType<ushort, T>(GetUInt16(ordinal)),
                Type type when type == typeof(uint) => ConvertChangeType<uint, T>(GetUInt32(ordinal)),
                Type type when type == typeof(ulong) => ConvertChangeType<ulong, T>(GetUInt64(ordinal)),

                _ => base.GetFieldValue<T>(ordinal)
            };
        }

        #region Extension Methods
        public sbyte GetSByte(int ordinal)
            => GetJsonElementValue(ordinal).GetSByte();

        public DateTimeOffset GetDateTimeOffset(int ordinal)
            => GetJsonElementValue(ordinal).GetDateTimeOffset();

        public ushort GetUInt16(int ordinal)
            => GetJsonElementValue(ordinal).GetUInt16();

        public uint GetUInt32(int ordinal)
            => GetJsonElementValue(ordinal).GetUInt32();

        public ulong GetUInt64(int ordinal)
            => GetJsonElementValue(ordinal).GetUInt64();

        public string GetRawText(int ordinal)
            => GetJsonElementValue(ordinal).GetRawText();

        public byte[] GetRawBytes(int ordinal)
            => Utf8Encoding.GetBytes(GetRawText(ordinal));
        #endregion

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

        private IList<object> GetCurrentRow() => GetCurrentResultSet().Data[_currentReadIndex];

        private ColumnInfo GetFieldColumn(int ordinal)
        {
            if (ordinal < 0 || ordinal >= ColumnInfos.Count)
                throw new IndexOutOfRangeException($"value must be between 0 and {ColumnInfos.Count}.");

            return ColumnInfos[ordinal];
        }

        private FlinkSqlDataType GetFieldDataType(int ordinal)
        {
            var column = GetFieldColumn(ordinal);

            return FlinkSqlDataTypeParser.ParseFromString(column.Type);
        }

        private JsonElement GetJsonElementValue(int ordinal)
        {
            if (ordinal < 0 || ordinal >= ColumnInfos.Count)
                throw new IndexOutOfRangeException($"value must be between 0 and {ColumnInfos.Count}.");

            if (GetCurrentRow()[ordinal] is JsonElement element)
                return element;

            throw new InvalidCastException("the target value is not type of JsonElement");
        }

        private static TResult ConvertChangeType<TValue, TResult>(TValue value)
        {
            if (value is null)
                return default;

            if (value is TResult result)
                return result;

            return (TResult)Convert.ChangeType(value, typeof(TResult));
        }

        private static void CheckBufferArguments<T>(long dataOffset, T[] buffer, int bufferOffset, int length)
        {
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset), dataOffset, "dataOffset must be non-negative");
            if (dataOffset > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(dataOffset), dataOffset, "dataOffset must be a 32-bit integer");

            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), length, "length must be non-negative");
            if (bufferOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(bufferOffset), bufferOffset, "bufferOffset must be non-negative");
            if (bufferOffset >= buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(bufferOffset), bufferOffset, "bufferOffset must be within the buffer");
            if (checked(bufferOffset + length) > buffer.Length)
                throw new ArgumentException("bufferOffset + length cannot exceed buffer.Length", nameof(length));
        }
        #endregion
    }
}
