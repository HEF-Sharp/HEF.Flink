using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using WebApiClientCore;
using WebApiClientCore.Attributes;

namespace HEF.Flink.SqlClient
{
    public interface IFlinkSqlGatewayApi : IHttpApi
    {
        [HttpGet("info")]
        Task<FlinkInfoResponse> GetInfoAsync();

        #region Session
        [HttpPost("sessions")]
        Task<SessionCreateResponse> CreateSessionAsync([Required, JsonContent] SessionCreateRequest requestParam);

        [HttpPost("sessions/{sessionId}/heartbeat")]
        Task HeartbeatSessionAsync([Required] string sessionId);

        [HttpDelete("sessions/{sessionId}")]
        Task<SessionStatusResponse> CloseSessionAsync([Required] string sessionId);
        #endregion
       
        [HttpPost("sessions/{sessionId}/statements")]
        Task<StatementExecuteResponse> ExecuteStatementAsync([Required] string sessionId, [Required, JsonContent] StatementExecuteRequest requestParam);

        #region Job
        [HttpGet("sessions/{sessionId}/jobs/{jobId}/result/{token}")]
        Task<ResultFetchResponse> FetchResultAsync([Required] string sessionId, [Required] string jobId, [Required] long token);

        [HttpGet("sessions/{sessionId}/jobs/{jobId}/status")]
        Task<JobStatusResponse> GetJobStatusAsync([Required] string sessionId, [Required] string jobId);

        [HttpDelete("sessions/{sessionId}/jobs/{jobId}")]
        Task<JobStatusResponse> CancelJobAsync([Required] string sessionId, [Required] string jobId);
        #endregion
    }
}
