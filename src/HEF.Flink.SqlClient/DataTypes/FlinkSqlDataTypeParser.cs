using HEF.Util;
using System;

namespace HEF.Flink.SqlClient
{
    internal class FlinkSqlDataTypeParser
    {
        internal const char WhiteSpaceChar = ' ';
        internal const char StartParameterChar = '(';
        internal const char CommaChar = ',';
        internal const char EndParameterChar = ')';

        internal static FlinkSqlDataType ParseFromString(string typeString)
        {
            if (string.IsNullOrWhiteSpace(typeString))
                throw new ArgumentNullException(nameof(typeString));

            var wordTokenArr = typeString.Split(WhiteSpaceChar, 2, StringSplitOptions.RemoveEmptyEntries);
            var startWord = wordTokenArr[0].Split(StartParameterChar, StringSplitOptions.RemoveEmptyEntries)[0];

            if (Enum.IsDefined(typeof(FlinkSqlDataTypes), startWord))
            {
                var sqlDataTypeEnum = Enum.Parse<FlinkSqlDataTypes>(startWord);

                return sqlDataTypeEnum switch
                {
                    FlinkSqlDataTypes.BOOLEAN => ParseBooleanType(wordTokenArr),
                    FlinkSqlDataTypes.CHAR => ParseCharType(wordTokenArr),
                    FlinkSqlDataTypes.VARCHAR => ParseVarCharType(wordTokenArr),
                    FlinkSqlDataTypes.STRING => ParseStringType(wordTokenArr),
                    FlinkSqlDataTypes.BINARY => ParseBinaryType(wordTokenArr),
                    FlinkSqlDataTypes.VARBINARY => ParseVarBinaryType(wordTokenArr),
                    FlinkSqlDataTypes.BYTES => ParseBytesType(wordTokenArr),
                    FlinkSqlDataTypes.DECIMAL => ParseDecimalType(wordTokenArr),
                    FlinkSqlDataTypes.DEC => ParseDecimalType(wordTokenArr),
                    FlinkSqlDataTypes.NUMERIC => ParseDecimalType(wordTokenArr),
                    FlinkSqlDataTypes.TINYINT => ParseTinyIntType(wordTokenArr),
                    FlinkSqlDataTypes.SMALLINT => ParseSmallIntType(wordTokenArr),
                    FlinkSqlDataTypes.INT => ParseIntType(wordTokenArr),
                    FlinkSqlDataTypes.INTEGER => ParseIntType(wordTokenArr),
                    FlinkSqlDataTypes.BIGINT => ParseBigIntType(wordTokenArr),
                    FlinkSqlDataTypes.FLOAT => ParseFloatType(wordTokenArr),
                    FlinkSqlDataTypes.DOUBLE => ParseDoubleType(wordTokenArr),
                    FlinkSqlDataTypes.DATE => ParseDateType(wordTokenArr),
                    FlinkSqlDataTypes.TIME => ParseTimeType(wordTokenArr),
                    FlinkSqlDataTypes.TIMESTAMP => ParseTimestampType(wordTokenArr),
                    FlinkSqlDataTypes.INTERVAL => throw new NotSupportedException("Not support parse interval DataType"),
                    _ => throw new NotSupportedException($"Can't parse '{typeString}' to Flink Sql DataType")
                };
            }

            throw new NotSupportedException($"Can't parse '{typeString}' to Flink Sql DataType");
        }

        #region Helper Functions
        private static TDataType ParseNoParameterType<TDataType>(string[] wordTokens)
            where TDataType : FlinkSqlDataType, new()
        {
            if (wordTokens.Length == 1 && !wordTokens[0].Contains(StartParameterChar))
                return new TDataType();

            throw new InvalidOperationException($"failed parse to {typeof(TDataType).Name}");
        }

        private static string[] ParseParameters(string wordToken)
        {
            if (string.IsNullOrWhiteSpace(wordToken))
                throw new ArgumentNullException(nameof(wordToken));

            throw new NotImplementedException();
        }
        #endregion

        #region Boolean
        private static FlinkSqlBooleanType ParseBooleanType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlBooleanType>(wordTokens);
        #endregion

        #region String
        private static FlinkSqlCharType ParseCharType(string[] wordTokens)
        {
            if (wordTokens.Length == 1)
            {
                var parameters = ParseParameters(wordTokens[0]);

                if (parameters.IsEmpty())
                    return new FlinkSqlCharType();

                if (parameters.Length == 1)
                    return new FlinkSqlCharType(parameters[0].ParseInt());
            }

            throw new InvalidOperationException($"failed parse to {typeof(FlinkSqlCharType).Name}");
        }

        private static FlinkSqlVarCharType ParseVarCharType(string[] wordTokens)
        {
            if (wordTokens.Length == 1)
            {
                var parameters = ParseParameters(wordTokens[0]);

                if (parameters.IsEmpty())
                    return new FlinkSqlVarCharType();

                if (parameters.Length == 1)
                    return new FlinkSqlVarCharType(parameters[0].ParseInt());
            }

            throw new InvalidOperationException($"failed parse to {typeof(FlinkSqlVarCharType).Name}");
        }

        private static FlinkSqlStringType ParseStringType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlStringType>(wordTokens);        
        #endregion

        #region Binary
        private static FlinkSqlBinaryType ParseBinaryType(string[] wordTokens)
        {
            if (wordTokens.Length == 1)
            {
                var parameters = ParseParameters(wordTokens[0]);

                if (parameters.IsEmpty())
                    return new FlinkSqlBinaryType();

                if (parameters.Length == 1)
                    return new FlinkSqlBinaryType(parameters[0].ParseInt());
            }

            throw new InvalidOperationException($"failed parse to {typeof(FlinkSqlBinaryType).Name}");
        }

        private static FlinkSqlVarBinaryType ParseVarBinaryType(string[] wordTokens)
        {
            if (wordTokens.Length == 1)
            {
                var parameters = ParseParameters(wordTokens[0]);

                if (parameters.IsEmpty())
                    return new FlinkSqlVarBinaryType();

                if (parameters.Length == 1)
                    return new FlinkSqlVarBinaryType(parameters[0].ParseInt());
            }

            throw new InvalidOperationException($"failed parse to {typeof(FlinkSqlVarBinaryType).Name}");
        }

        private static FlinkSqlBytesType ParseBytesType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlBytesType>(wordTokens);        
        #endregion

        #region Decimal
        private static FlinkSqlDecimalType ParseDecimalType(string[] wordTokens)
        {
            if (wordTokens.Length == 1)
            {
                var parameters = ParseParameters(wordTokens[0]);

                if (parameters.IsEmpty())
                    return new FlinkSqlDecimalType();

                if (parameters.Length == 1)
                    return new FlinkSqlDecimalType(parameters[0].ParseInt());

                if (parameters.Length == 2)
                    return new FlinkSqlDecimalType(parameters[0].ParseInt(), parameters[1].ParseInt());
            }

            throw new InvalidOperationException($"failed parse to {typeof(FlinkSqlDecimalType).Name}");
        }
        #endregion

        #region Integer
        private static FlinkSqlTinyIntType ParseTinyIntType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlTinyIntType>(wordTokens);

        private static FlinkSqlSmallIntType ParseSmallIntType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlSmallIntType>(wordTokens);

        private static FlinkSqlIntType ParseIntType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlIntType>(wordTokens);

        private static FlinkSqlBigIntType ParseBigIntType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlBigIntType>(wordTokens);
        #endregion

        #region Float
        private static FlinkSqlFloatType ParseFloatType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlFloatType>(wordTokens);

        private static FlinkSqlDoubleType ParseDoubleType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlDoubleType>(wordTokens);
        #endregion

        #region DateTime
        private static FlinkSqlDateType ParseDateType(string[] wordTokens)
            => ParseNoParameterType<FlinkSqlDateType>(wordTokens);        

        private static FlinkSqlTimeType ParseTimeType(string[] wordTokens)
        {
            if (wordTokens.Length == 1)
            {
                var parameters = ParseParameters(wordTokens[0]);

                if (parameters.IsEmpty())
                    return new FlinkSqlTimeType();

                if (parameters.Length == 1)
                    return new FlinkSqlTimeType(parameters[0].ParseInt());
            }

            throw new InvalidOperationException($"failed parse to {typeof(FlinkSqlTimeType).Name}");
        }

        private static FlinkSqlTimestampType ParseTimestampType(string[] wordTokens)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
