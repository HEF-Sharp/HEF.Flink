using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class SessionCreateRequest
    {
        [JsonPropertyName("session_name")]
        public string SessionName { get; set; }

        public string Planner { get; set; }

        [JsonPropertyName("execution_type")]
        public string ExecutionType { get; set; }

        public IDictionary<string, string> Properties { get; set; }
    }
}
