namespace Clickhouse.Pure.Grpc.Tests;

public class Sut
{
    private readonly DefaultCallHandler _handler;

    public Sut(
        DefaultCallHandler handler)
    {
        _handler = handler;
    }

    public async Task<string?> GetVersionThrowing()
    {
        var (res, ex) = await _handler.QueryRawString("SELECT VERSION()");

        if (ex != null)
        {
            throw ex;
        }

        return res;
    }
}