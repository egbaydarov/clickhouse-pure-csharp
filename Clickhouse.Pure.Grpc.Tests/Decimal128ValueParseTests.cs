using AwesomeAssertions;
using Clickhouse.Pure.Columns;
using Xunit;

namespace Clickhouse.Pure.Grpc.Tests;

public class Decimal128ValueParseTests
{
    [Theory]
    [InlineData("0", 8, "0.00000000")]
    [InlineData("6802", 8, "6802.00000000")]
    [InlineData("1", 8, "1.00000000")]
    [InlineData("0.60", 2, "0.60")]
    [InlineData("0.60", 5, "0.60000")]
    [InlineData("-123.456", 3, "-123.456")]
    [InlineData("+7.5", 1, "7.5")]
    [InlineData(".5", 1, "0.5")]
    [InlineData("5.", 1, "5.0")]
    [InlineData("1E-7", 8, "0.00000010")]
    [InlineData("1.5e3", 0, "1500")]
    [InlineData("  42  ", 0, "42")]
    [InlineData("11869.9871812", 18, "11869.987181200000000000")]
    public void Parse_ValidStrings_ProducesExpectedValue(string input, int scale, string expected)
    {
        var value = Decimal128Value.Parse(input, scale);

        value.Scale.Should().Be(scale);
        value.ToString().Should().Be(expected);
    }

    [Theory]
    [InlineData("1.2345", 2, "1.23")]
    [InlineData("1.235", 2, "1.24")]
    [InlineData("-1.235", 2, "-1.24")]
    [InlineData("0.005", 2, "0.01")]
    [InlineData("0.004", 2, "0.00")]
    public void Parse_ExcessFractionDigits_RoundsHalfAwayFromZero(string input, int scale, string expected)
    {
        Decimal128Value.Parse(input, scale).ToString().Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("abc")]
    [InlineData("1,5")]
    [InlineData(".")]
    [InlineData("-")]
    [InlineData("1e")]
    [InlineData("1.2.3")]
    public void TryParse_InvalidStrings_ReturnsFalse(string? input)
    {
        Decimal128Value.TryParse(input, 2, out _).Should().BeFalse();
    }

    [Fact]
    public void TryParse_ValueExceedingInt128_ReturnsFalse()
    {
        Decimal128Value.TryParse("1" + new string('0', 40), 0, out _).Should().BeFalse();
    }

    [Fact]
    public void TryParse_InvalidScale_ReturnsFalse()
    {
        Decimal128Value.TryParse("1", -1, out _).Should().BeFalse();
        Decimal128Value.TryParse("1", 39, out _).Should().BeFalse();
    }

    [Fact]
    public void Parse_InvalidString_Throws()
    {
        var act = () => Decimal128Value.Parse("not a number", 2);

        act.Should().Throw<FormatException>();
    }
}
