using AwesomeAssertions;
using Xunit;

namespace Clickhouse.Pure.Grpc.Tests;

public class CallHandlerTests
{
    [Fact]
    public async Task GetVersionReturnsCorrectString()
    {
        var sut = SutFactory.Create();

        var version = await sut.GetVersionThrowing();

        version
            .Should()
            .StartWith("24.3");
    }
}