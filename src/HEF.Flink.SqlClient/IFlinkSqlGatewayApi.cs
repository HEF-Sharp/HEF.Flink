using System.Threading.Tasks;
using WebApiClientCore;
using WebApiClientCore.Attributes;

namespace HEF.Flink.SqlClient
{
    public interface IFlinkSqlGatewayApi : IHttpApi
    {
        [HttpGet("info")]
        Task<FlinkInfo> GetInfoAsync();
    }
}
