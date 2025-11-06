## NativeFormatBlockReader Type Support Specification

### 1. Scope
The `NativeFormatBlockReader` component consumes ClickHouse Native protocol blocks delivered via gRPC. Only the column encodings enumerated in Section 3 are defined. Any other column type MUST be treated as unsupported.

### 2. Reader Contract
* Invocation of `Advance*Column()` MUST occur in the order columns are encoded. Each call consumes the column header (`name`, `type`) and returns an `ISequentialColumnReader<T>`.
* Column readers expose `Length`, `HasMoreRows()`, and `GetCellValueAndAdvance()`. Callers MUST iterate sequentially and MUST NOT read past `Length` rows.
* Unless stated otherwise, values are decoded using little-endian primitives and UTF-8 text.

### 3. Supported Column Kinds

#### 3.1 Textual Encodings
* `String` → `string`
* `FixedString(N)` → `string` (trims trailing zero bytes)
* `LowCardinality(String)` → `string`
* `Nullable(String)` → `string?`

#### 3.2 Date and Time Encodings
* `Date` → `DateOnly` (days since 1970-01-01)
* `Date32` → `DateOnly` (days since 1900-01-01)
* `DateTime64(scale, timezone)` → `DateTimeOffset`

#### 3.3 Integer Encodings
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

#### 3.4 Floating-Point Encodings
* `Float32` → `float`
* `Float64` → `double`

### 4. Non-Goals
* Array, decimal, tuple, enum, and LowCardinality global dictionary encodings are currently out of scope.
* Timezone resolution relies on the operating system database. Missing zones MUST raise `TimeZoneNotFoundException`.

