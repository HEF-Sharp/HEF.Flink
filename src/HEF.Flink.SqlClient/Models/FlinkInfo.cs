using System.Text.Json.Serialization;

namespace HEF.Flink.SqlClient
{
    public class FlinkInfo
    {
        [JsonPropertyName("product_name")]
        public string ProductName { get; set; }

        public string Version { get; set; }
    }
}
