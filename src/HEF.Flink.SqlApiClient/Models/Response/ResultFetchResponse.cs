using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace HEF.Flink.SqlApiClient
{
    public class ResultFetchResponse
    {
        public IList<ExecuteResultSet> Results { get; set; }

        [JsonPropertyName("next_result_uri")]
        public string NextResultUri { get; set; }
    }
}
