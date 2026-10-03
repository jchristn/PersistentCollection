# Change Log

## Current Version

v2.2.0

- Built-in observability through `System.Diagnostics` `Meter` and `ActivitySource`, both named `PersistentCollection` (see `TELEMETRY.md`)
- Metrics: operation duration by collection type, operation, and outcome; lock wait; persistence read, write, and truncate duration; file size; last successful write; errors by `error.type`; live item and instance counts; build info
- Spans for disk-touching operations (open, mutations, clear), each with a child persistence span, joining the caller's W3C trace
- New public constants class `PersistentCollectionTelemetry` (meter, source, metric, and attribute names)
- New dependency: `System.Diagnostics.DiagnosticSource` 10.0.12
- `TryPeek`, `TryDequeue`, and `TryPop` check for an empty collection directly instead of catching an exception. Behavior is unchanged.
- Persistence writes use `JsonSerializer.SerializeToUtf8Bytes` + `File.WriteAllBytes` (byte-identical output)

## Previous Versions

v2.1.0

- Updated System.Text.Json to 10.x. No public API changes.

v2.0.x

- Remove expiration
- Migrate existing implementation to `PersistentList`, `PersistentQueue`, and `PersistentStack`
- Rename package to `PersistentCollections`

v1.0.x

- Initial release
