using System;
using System.Data.Common;

namespace HEF.Flink.SqlClient
{
    public class FlinkSqlException : DbException
    {
        internal FlinkSqlException(string message)
            : this(message, null)
        {
        }

        internal FlinkSqlException(string message, Exception innerException)
            : base(message, innerException)
        { }
    }
}
