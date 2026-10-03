# Change Log

## Current Version

v2.2.1

- Updated `SerializationHelper` 2.0.3 -> 2.1.0 and `System.Text.Json` 10.0.11 -> 10.0.12. No public API changes.
- Test dependencies updated: Touchstone 0.2.0, NUnit 5.0.0, NUnit3TestAdapter 6.3.0, Microsoft.NET.Test.Sdk 18.10.1
- Added a persistence test covering JSON round-trip of complex values, null/empty members, and special/Unicode characters

## Previous Versions

v2.2.0

- Built-in observability through `System.Diagnostics` `Meter` and `ActivitySource`, both named `PersistentCollection` (see `TELEMETRY.md`)
- Metrics: operation duration by collection type, operation, and outcome; lock wait; persistence read, write, and truncate duration; file size; last successful write; errors by `error.type`; live item and instance counts; build info
- Spans for disk-touching operations (open, mutations, clear), each with a child persistence span, joining the caller's W3C trace
- New public constants class `PersistentCollectionTelemetry` (meter, source, metric, and attribute names)
- New dependency: `System.Diagnostics.DiagnosticSource` 10.0.12
- `TryPeek`, `TryDequeue`, and `TryPop` check for an empty collection directly instead of catching an exception. Behavior is unchanged.
- Persistence writes use `JsonSerializer.SerializeToUtf8Bytes` + `File.WriteAllBytes` (byte-identical output)

v2.1.0

- Updated System.Text.Json to 10.x. No public API changes.

v2.0.x

- Remove expiration
- Migrate existing implementation to `PersistentList`, `PersistentQueue`, and `PersistentStack`
- Rename package to `PersistentCollections`

v1.0.x

- Initial release
