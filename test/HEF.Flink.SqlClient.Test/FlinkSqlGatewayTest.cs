using System.Threading.Tasks;
using Xunit;

namespace HEF.Flink.SqlClient.Test
{
    public class FlinkSqlGatewayTest : FlinkSqlGatewayApiTestBase
    {
        public FlinkSqlGatewayTest(FlinkSqlGatewayApiFixture fixture)
            : base(fixture)
        { }

        [Fact]
        public async Task TestGetInfo()
        {
            var response = await SqlGatewayApi.GetInfoAsync();

            Assert.NotNull(response);
            Assert.False(string.IsNullOrWhiteSpace(response.ProductName));
            Assert.False(string.IsNullOrWhiteSpace(response.Version));
        }
    }
}
