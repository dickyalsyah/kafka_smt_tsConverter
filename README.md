# Kafka Connect TsConverter SMT

A custom Kafka Connect Single Message Transform (SMT) that replaces and enhances the standard `TimestampConverter`. This SMT allows converting multiple timestamp fields at once and fixes precision loss issues, preserving up to microsecond and nanosecond precision.

## Why this SMT?

The standard `TimestampConverter` only supports converting a single field at a time, and it loses precision (truncates to milliseconds) when converting UNIX epochs with microsecond or nanosecond precision into `java.sql.Timestamp` (which JDBC sinks require for exact precision sync, e.g. to Oracle or other RDBMS).

This `TsConverter` solves both issues:
- **Multiple Fields Support**: You can provide a comma-separated list of fields in `field` configuration.
- **High Precision**: Preserves microsecond and nanosecond precision when converting from `unix` to `Timestamp` or `MicroTimestamp`.

## Deployment

1. Build the jar package:
   ```bash
   mvn clean package
   ```
2. Copy the resulting jar (e.g., `target/multi-field-timestamp-converter-1.0.0.jar`) to your Kafka Connect plugin path.
3. Restart the Kafka Connect workers.

## Configurations

| Configuration | Description | Default |
| ------------- | ----------- | ------- |
| `field` | A comma-separated list of fields containing the timestamps, or empty if the entire value is a timestamp | `""` |
| `target.type` | The desired timestamp representation: `string`, `unix`, `Date`, `Time`, `Timestamp`, or `MicroTimestamp` | (Required) |
| `format` | A SimpleDateFormat-compatible format for the timestamp. | `""` |
| `unix.precision`| The desired Unix precision: `seconds`, `milliseconds`, `microseconds`, or `nanoseconds`. You can also map per-field: `Field1:precision, Field2:precision`. Fallback is applied for untargeted fields. | `milliseconds` |
| `target.timezone` | Map timezones to specific fields using `Field:TZ, Field2:TZ`, or fallback timezone `TZ` | `""` |
| `replace.null.with.default` | Whether to replace null fields with their schema default value. | `true` |

## Usage Examples

### 1. Converting Multiple Fields (Microsecond Precision)

If your source sends `unix` epoch with microsecond precision for `DATA_INP_DTTM` but uses millisecond precision for `DATA_CHNG_DTTM`, and you want to convert both fields to `MicroTimestamp` while handling a timezone shift for one:

```properties
transforms=ConvertTs
transforms.ConvertTs.type=com.dickyalsyah.kafka.connect.transforms.TsConverter$Value
transforms.ConvertTs.field=UPDATED_AT,CREATED_AT
transforms.ConvertTs.target.type=MicroTimestamp
transforms.ConvertTs.unix.precision=UPDATED_AT:milliseconds,CREATED_AT:microseconds
transforms.ConvertTs.target.timezone=UPDATED_AT:Asia/Bangkok
```

### 2. Converting a Single Field

To convert just a single field as you would do with the standard `TimestampConverter`, you just supply a single field name.

```properties
transforms=ConvertTs
transforms.ConvertTs.type=com.dickyalsyah.kafka.connect.transforms.TsConverter$Value
transforms.ConvertTs.field=CREATED_AT
transforms.ConvertTs.target.type=MicroTimestamp
transforms.ConvertTs.unix.precision=milliseconds
```

### 3. Converting to String (Formatted Date)

Convert fields into formatted Strings:

```properties
transforms=ConvertTs
transforms.ConvertTs.type=com.dickyalsyah.kafka.connect.transforms.TsConverter$Value
transforms.ConvertTs.field=UPDATED_AT
transforms.ConvertTs.target.type=string
transforms.ConvertTs.format=yyyy-MM-dd HH:mm:ss.SSSSSS
transforms.ConvertTs.unix.precision=microseconds
```

### 4. Schemaless Record Conversion

The `TsConverter` also supports schemaless JSON records. The configuration does not change.
