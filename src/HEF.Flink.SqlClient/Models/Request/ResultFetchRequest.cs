using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class ResultFetchRequest
    {
        [JsonPropertyName("max_fetch_size")]
        public int MaxFetchSize { get; set; }
    }
}
