using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace HEF.Flink.SqlApiClient
{
    public class ExecuteResultSet
    {
        [JsonPropertyName("result_kind")]
        public ResultKind ResultKind { get; set; }

        public IList<ColumnInfo> Columns { get; set; }

        public IList<IList<object>> Data { get; set; }

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
