using System.Text.Json.Serialization;

namespace HEF.Flink.SqlApiClient
{
    public class StatementExecuteRequest
    {
        public string Statement { get; set; }

        [JsonPropertyName("execution_timeout")]
        public long ExecutionTimeout { get; set; }
    }
}
