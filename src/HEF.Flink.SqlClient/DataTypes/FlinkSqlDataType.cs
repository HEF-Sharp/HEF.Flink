using System;

namespace HEF.Flink.SqlClient
{
    public abstract class FlinkSqlDataType
    {
        public abstract string KeyWord { get; }

        public abstract Type ClrType { get; }

        public virtual string[] Alias => Array.Empty<string>();
    }

    public class FlinkSqlBooleanType : FlinkSqlDataType
    {
        public override string KeyWord => "BOOLEAN";

        public override Type ClrType => typeof(bool);

        public override string ToString() => KeyWord;
    }

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

        public override string KeyWord => "CHAR";

        public override Type ClrType => typeof(string);

        public override string ToString() => $"{KeyWord}({Length})";
    }

    public class FlinkSqlVarCharType : FlinkSqlCharType
    {
        public FlinkSqlVarCharType()
            : base()
        { }

        public FlinkSqlVarCharType(int length)
            : base(length)
        { }

        public override string KeyWord => "VARCHAR";
    }

    public class FlinkSqlStringType : FlinkSqlVarCharType
    {
        public FlinkSqlStringType()
            : base(int.MaxValue)
        { }

        public override string KeyWord => "STRING";

        public override string ToString() => KeyWord;
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

        public override string KeyWord => "BINARY";

        public override Type ClrType => typeof(byte[]);

        public override string ToString() => $"{KeyWord}({Length})";
    }

    public class FlinkSqlVarBinaryType : FlinkSqlBinaryType
    {
        public FlinkSqlVarBinaryType()
            : base()
        { }

        public FlinkSqlVarBinaryType(int length)
            : base(length)
        { }

        public override string KeyWord => "VARBINARY";
    }

    public class FlinkSqlBytesType : FlinkSqlVarBinaryType
    {
        public FlinkSqlBytesType()
            : base(int.MaxValue)
        { }

        public override string KeyWord => "BYTES";

        public override string ToString() => KeyWord;
    }
    #endregion

    #region Decimal
    public class FlinkSqlDecimalType : FlinkSqlDataType
    {
        public const int Max_Precision = 38;
        public const int Default_Precision = 10;

        public static string[] KeyWord_Alias = { "DEC", "NUMERIC" };

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

        public override string KeyWord => "DECIMAL";

        public override Type ClrType => typeof(decimal);

        public override string[] Alias => KeyWord_Alias;

        public override string ToString() => $"{KeyWord}({Precision}, {Scale})";
    }
    #endregion

    #region Integer
    public class FlinkSqlTinyIntType : FlinkSqlDataType
    {
        public override string KeyWord => "TINYINT";

        public override Type ClrType => typeof(byte);

        public override string ToString() => KeyWord;
    }

    public class FlinkSqlSmallIntType : FlinkSqlDataType
    {
        public override string KeyWord => "SMALLINT";

        public override Type ClrType => typeof(short);

        public override string ToString() => KeyWord;
    }

    public class FlinkSqlIntType : FlinkSqlDataType
    {
        public static string[] KeyWord_Alias = { "INTEGER" };

        public override string KeyWord => "INT";

        public override Type ClrType => typeof(int);

        public override string[] Alias => KeyWord_Alias;

        public override string ToString() => KeyWord;
    }

    public class FlinkSqlBigIntType : FlinkSqlDataType
    {
        public override string KeyWord => "BIGINT";

        public override Type ClrType => typeof(long);

        public override string ToString() => KeyWord;
    }
    #endregion

    #region Float
    public class FlinkSqlFloatType : FlinkSqlDataType
    {
        public override string KeyWord => "FLOAT";

        public override Type ClrType => typeof(float);

        public override string ToString() => KeyWord;
    }

    public class FlinkSqlDoubleType : FlinkSqlDataType
    {
        public override string KeyWord => "DOUBLE";

        public override Type ClrType => typeof(double);

        public override string ToString() => KeyWord;
    }
    #endregion
}
