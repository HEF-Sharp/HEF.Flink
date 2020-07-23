using System;

namespace HEF.Flink.SqlClient
{
    public abstract class FlinkSqlDataType
    {
        public abstract Type ClrType { get; }
    }

    public enum FlinkSqlDataTypes
    {
        BOOLEAN,
        CHAR,
        VARCHAR,
        STRING,
        BINARY,
        VARBINARY,
        BYTES,
        DECIMAL,
        DEC,
        NUMERIC,
        TINYINT,
        SMALLINT,
        INT,
        INTEGER,
        BIGINT,
        FLOAT,
        DOUBLE,
        DATE,
        TIME,
        TIMESTAMP,
        INTERVAL
    }

    #region Boolean
    public class FlinkSqlBooleanType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(bool);

        public override string ToString() => FlinkSqlDataTypes.BOOLEAN.ToString();
    }
    #endregion

    #region String
    public class FlinkSqlCharType : FlinkSqlDataType
    {
        public FlinkSqlCharType()
            : this(1)
        { }

        public FlinkSqlCharType(int length)
        {
            if (length < 1)
                throw new IndexOutOfRangeException("Character string length must be positive number");

            Length = length;
        }

        public int Length { get; }

        public override Type ClrType => typeof(string);

        public override string ToString() => $"{FlinkSqlDataTypes.CHAR}({Length})";
    }

    public class FlinkSqlVarCharType : FlinkSqlCharType
    {
        public FlinkSqlVarCharType()
            : base()
        { }

        public FlinkSqlVarCharType(int length)
            : base(length)
        { }

        public override string ToString() => $"{FlinkSqlDataTypes.VARCHAR}({Length})";
    }

    public class FlinkSqlStringType : FlinkSqlVarCharType
    {
        public FlinkSqlStringType()
            : base(int.MaxValue)
        { }

        public override string ToString() => FlinkSqlDataTypes.STRING.ToString();
    }
    #endregion

    #region Binary
    public class FlinkSqlBinaryType : FlinkSqlDataType
    {
        public FlinkSqlBinaryType()
            : this(1)
        { }

        public FlinkSqlBinaryType(int length)
        {
            if (length < 1)
                throw new IndexOutOfRangeException("Binary string length must be positive number");

            Length = length;
        }

        public int Length { get; }

        public override Type ClrType => typeof(byte[]);

        public override string ToString() => $"{FlinkSqlDataTypes.BINARY}({Length})";
    }

    public class FlinkSqlVarBinaryType : FlinkSqlBinaryType
    {
        public FlinkSqlVarBinaryType()
            : base()
        { }

        public FlinkSqlVarBinaryType(int length)
            : base(length)
        { }

        public override string ToString() => $"{FlinkSqlDataTypes.VARBINARY}({Length})";
    }

    public class FlinkSqlBytesType : FlinkSqlVarBinaryType
    {
        public FlinkSqlBytesType()
            : base(int.MaxValue)
        { }

        public override string ToString() => FlinkSqlDataTypes.BYTES.ToString();
    }
    #endregion

    #region Decimal
    public class FlinkSqlDecimalType : FlinkSqlDataType
    {
        public const int Max_Precision = 38;
        public const int Default_Precision = 10;

        public FlinkSqlDecimalType()
            : this(Default_Precision)
        { }

        public FlinkSqlDecimalType(int precision)
            : this(precision, 0)
        { }

        public FlinkSqlDecimalType(int precision, int scale)
        {
            if (precision < 1 || precision > Max_Precision)
                throw new IndexOutOfRangeException($"Decimal precision must be between 1 and {Max_Precision}");

            if (scale < 0 || scale > precision)
                throw new IndexOutOfRangeException($"Decimal scale must be between 0 and the precision {precision}");

            Precision = precision;
            Scale = scale;
        }

        public int Precision { get; }

        public int Scale { get; }

        public override Type ClrType => typeof(decimal);

        public override string ToString() => $"{FlinkSqlDataTypes.DECIMAL}({Precision}, {Scale})";
    }
    #endregion

    #region Integer
    public class FlinkSqlTinyIntType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(byte);

        public override string ToString() => FlinkSqlDataTypes.TINYINT.ToString();
    }

    public class FlinkSqlSmallIntType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(short);

        public override string ToString() => FlinkSqlDataTypes.SMALLINT.ToString();
    }

    public class FlinkSqlIntType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(int);

        public override string ToString() => FlinkSqlDataTypes.INT.ToString();
    }

    public class FlinkSqlBigIntType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(long);

        public override string ToString() => FlinkSqlDataTypes.BIGINT.ToString();
    }
    #endregion

    #region Float
    public class FlinkSqlFloatType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(float);

        public override string ToString() => FlinkSqlDataTypes.FLOAT.ToString();
    }

    public class FlinkSqlDoubleType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(double);

        public override string ToString() => FlinkSqlDataTypes.DOUBLE.ToString();
    }
    #endregion

    #region DateTime
    public class FlinkSqlDateType : FlinkSqlDataType
    {
        public override Type ClrType => typeof(DateTime);

        public override string ToString() => FlinkSqlDataTypes.DATE.ToString();
    }

    public class FlinkSqlTimeType : FlinkSqlDataType
    {
        public const int Max_Precision = 9;

        public FlinkSqlTimeType()
            : this(0)
        { }

        public FlinkSqlTimeType(int precision)
        {
            if (precision < 0 || precision > Max_Precision)
                throw new IndexOutOfRangeException($"Time precision must be between 0 and {Max_Precision}");

            Precision = precision;
        }

        public int Precision { get; }

        public override Type ClrType => typeof(TimeSpan);

        public override string ToString() => $"{FlinkSqlDataTypes.TIME}({Precision})";
    }

    public class FlinkSqlTimestampType : FlinkSqlDataType
    {
        public const int Max_Precision = 9;
        public const int Default_Precision = 6;

        public FlinkSqlTimestampType()
            : this(Default_Precision)
        { }

        public FlinkSqlTimestampType(int precision)
        {
            if (precision < 0 || precision > Max_Precision)
                throw new IndexOutOfRangeException($"Timestamp precision must be between 0 and {Max_Precision}");

            Precision = precision;
        }

        public int Precision { get; }        

        public override Type ClrType => typeof(DateTime);

        public override string ToString() => $"{FlinkSqlDataTypes.TIMESTAMP}({Precision})";
    }

    public class FlinkSqlZonedTimestampType : FlinkSqlTimestampType
    {
        public const string TimezoneDefine = "WITH TIME ZONE";

        public FlinkSqlZonedTimestampType()
            : base()
        { }

        public FlinkSqlZonedTimestampType(int precision)
            : base(precision)
        { }

        public override Type ClrType => typeof(DateTimeOffset);

        public override string ToString() => $"{FlinkSqlDataTypes.TIMESTAMP}({Precision}) {TimezoneDefine}";
    }

    public class FlinkSqlLocalZonedTimestampType : FlinkSqlTimestampType
    {
        public const string TimezoneDefine = "WITH LOCAL TIME ZONE";

        public FlinkSqlLocalZonedTimestampType()
            : base()
        { }

        public FlinkSqlLocalZonedTimestampType(int precision)
            : base(precision)
        { }

        public override Type ClrType => typeof(DateTime);

        public override string ToString() => $"{FlinkSqlDataTypes.TIMESTAMP}({Precision}) {TimezoneDefine}";
    }
    #endregion
}
