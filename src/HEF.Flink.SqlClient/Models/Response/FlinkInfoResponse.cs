using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class FlinkInfoResponse
    {
        [JsonPropertyName("product_name")]
        public string ProductName { get; set; }

        public string Version { get; set; }
    }
}
