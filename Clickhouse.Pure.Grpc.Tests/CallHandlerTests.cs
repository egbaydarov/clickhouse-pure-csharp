using System.Text;
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

    [Fact]
    public async Task GetVersionResultReturnsPayload()
    {
        var sut = SutFactory.Create();

        var result = await sut.GetVersionResultThrowing();

        result.Exception
            .Should()
            .BeNull();

        var text = Encoding.UTF8.GetString(result.Output.Span);

        text
            .Should()
            .StartWith("24.3");
    }

    [Fact]
    public async Task InputBulkWritesRows()
    {
        var sut = SutFactory.Create();
        var tableName = await sut.PrepareBulkTableAsync();

        try
        {
            var commit = await sut.InsertBulkNumbersAsync(tableName, 1u, 2u, 3u);

            commit.IsFailed()
                .Should()
                .BeFalse();

            var values = await sut.FetchInsertedNumbersAsync(tableName);

            values
                .Should()
                .Equal(1u, 2u, 3u);
        }
        finally
        {
            await sut.DropTableAsync(tableName);
        }
    }

    [Fact]
    public async Task NativeBulkReaderStreamsNumbers()
    {
        var sut = SutFactory.Create();

        var numbers = await sut.Read5NumbersNativeAsync();

        numbers
            .Should()
            .Equal(0ul, 1ul, 2ul, 3ul, 4ul);
    }
}