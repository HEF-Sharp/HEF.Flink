using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class StatementExecuteResponse
    {
        [JsonPropertyName("statement_types")]
        public IList<string> StatementTypes { get; set; }

        public IList<ExecuteResultSet> Results { get; set; }
    }
}
