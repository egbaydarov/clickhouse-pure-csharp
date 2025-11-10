# Supported Types

## Textual Encodings
* `String` → `string`
* `FixedString(N)` → `string` (trims trailing zero bytes)
* `LowCardinality(String)` → `string`
* `Nullable(String)` → `string?`

## Date and Time Encodings
* `Date` → `DateOnly` (days since 1970-01-01)
* `Date32` → `DateOnly` (days since 1900-01-01)
* `DateTime64(scale, timezone)` → `DateTimeOffset`

## Integer Encodings
* `Bool` → `bool`
* `Int8` → `sbyte`
* `Int16` → `short`
* `Int32` → `int`
* `Int64` → `long`
* `Int128` → `Int128`
* `UInt8` → `byte`
* `UInt16` → `ushort`
* `UInt32` → `uint`
* `UInt64` → `ulong`
* `UInt128` → `UInt128`
* `IPv4` → `System.Net.IPAddress`

## Floating-Point Encodings
* `Float32` → `float`
* `Float64` → `double`

