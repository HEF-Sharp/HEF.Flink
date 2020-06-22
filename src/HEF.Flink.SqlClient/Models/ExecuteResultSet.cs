using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class ExecuteResultSet
    {
        [JsonPropertyName("result_kind")]
        public ResultKind ResultKind { get; set; }

        public IList<ColumnInfo> Columns { get; set; }

        public IList<IList<string>> Data { get; set; }

        [JsonPropertyName("change_flags")]
        public IList<bool> ChangeFlags { get; set; }
    }

    public enum ResultKind
    {
        SUCCESS,
        SUCCESS_WITH_CONTENT
    }

    public class ColumnInfo
    {
        public string Name { get; set; }

        public string Type { get; set; }
    }
}
