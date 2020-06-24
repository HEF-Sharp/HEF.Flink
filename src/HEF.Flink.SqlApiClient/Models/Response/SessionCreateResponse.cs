using System.Text.Json.Serialization;

namespace HEF.Flink.SqlApiClient
{
    public class SessionCreateResponse
    {
        [JsonPropertyName("session_id")]
        public string SessionId { get; set; }
    }
}
