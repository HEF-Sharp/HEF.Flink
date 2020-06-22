using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class SessionCreateResponse
    {
        [JsonPropertyName("session_id")]
        public string SessionId { get; set; }
    }
}
