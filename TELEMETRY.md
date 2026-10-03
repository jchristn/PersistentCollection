# PersistentCollection Telemetry

PersistentCollection emits metrics and traces through the standard .NET `System.Diagnostics` APIs: one `Meter` and one `ActivitySource`, both named **`PersistentCollection`**. The names are OpenTelemetry-shaped, so the data works with Prometheus, Tempo, Grafana, or any OTLP backend.

This is a **library**. It takes no dependency on an OpenTelemetry SDK, an exporter, or Radiant, and it never opens a connection. Your host process collects the data. When nothing is subscribed, each instrumented call costs one `Enabled` / `HasListeners()` check and allocates nothing extra.

## What it answers

Every mutation of a PersistentCollection holds a per-instance lock and rewrites the **entire** collection to disk as JSON. That design makes the operational questions simple, and the telemetry is built around them:

| Question | Signal |
| --- | --- |
| Which operation is slow, and on which collection type? | `persistentcollection_operation_duration_seconds` by `collection_type`, `operation` |
| Is the time spent waiting for the lock or doing disk I/O? | `persistentcollection_lock_wait_duration_seconds` vs `persistentcollection_persistence_duration_seconds` |
| Is the file getting too big? (Per the README, keep it under about 10 MB and 1000 records.) | `persistentcollection_persistence_size_bytes` |
| How deep is my queue, and is it draining? | `persistentcollection_items{collection_type="queue"}` |
| What failed: a corrupt file, a disk/permission error, or an empty-collection misuse? | `persistentcollection_errors_total` by `error_type`; `persistence_duration{outcome="error"}` |
| Has anything been persisted recently? | `persistentcollection_persistence_last_success_seconds` |
| Why was *this* enqueue slow? | Trace: `PersistentQueue enqueue` → `PersistentQueue persistence.write`, with lock wait, bytes, and item count on the spans |

## Names

All names live in the public constants class `PersistentCollection.PersistentCollectionTelemetry` (`src/PersistentCollection/PersistentCollectionTelemetry.cs`). Treat them as public contract.

| Item | Value |
| --- | --- |
| Meter | `PersistentCollection` (`PersistentCollectionTelemetry.MeterName`) |
| ActivitySource | `PersistentCollection` (`PersistentCollectionTelemetry.ActivitySourceName`) |
| Meter / source version | Library version, for example `2.2.0` |

## Subscribing from a host

### Radiant

```csharp
using PersistentCollection;
using Radiant;

RadiantSettings settings = new RadiantSettings("my-service");
settings.Otlp.Endpoint = "http://127.0.0.1:4317";
settings.Prometheus.Enable = true;
settings.Sources.AddMeter(PersistentCollectionTelemetry.MeterName);
settings.Sources.AddActivitySource(PersistentCollectionTelemetry.ActivitySourceName);

using (RadiantHost host = RadiantHost.Start(settings))
{
    // run the app
}
```

### OpenTelemetry SDK

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddMeter(PersistentCollectionTelemetry.MeterName).AddOtlpExporter())
    .WithTracing(t => t.AddSource(PersistentCollectionTelemetry.ActivitySourceName).AddOtlpExporter());
```

### Ad hoc

```
dotnet-counters monitor -n MyApp --counters PersistentCollection
```

There is nothing to configure in the library. The only switch is whether a listener subscribes. Histogram bucket boundaries are supplied as instrument *advice* (seconds: 0.0001 to 10; bytes: 256 B to 64 MB), and the host's collector can override them.

## Metrics catalog

The Prometheus column shows the names the OpenTelemetry Prometheus exporter produces. Dots become underscores and the unit and `_total` suffixes are added.

| Instrument | Prometheus | Type | Unit | Labels | Description |
| --- | --- | --- | --- | --- | --- |
| `persistentcollection.operation.duration` | `persistentcollection_operation_duration_seconds` | Histogram | `s` | `collection.type`, `operation`, `outcome`, `error.type`* | Duration of each public operation, including lock wait and any disk persistence. Its `_count` is the operation rate. |
| `persistentcollection.lock.wait.duration` | `persistentcollection_lock_wait_duration_seconds` | Histogram | `s` | `collection.type`, `operation` | Time waiting to acquire the per-instance lock (contention). |
| `persistentcollection.persistence.duration` | `persistentcollection_persistence_duration_seconds` | Histogram | `s` | `collection.type`, `operation` (`read`, `write`, `truncate`), `outcome`, `error.type`* | Duration of a disk step: read and deserialize on open, serialize and rewrite on mutation, truncate on `Clear`. |
| `persistentcollection.persistence.size` | `persistentcollection_persistence_size_bytes` | Histogram | `By` | `collection.type`, `operation` (`read`, `write`) | Bytes read on open or written per rewrite, which equals the file size. |
| `persistentcollection.persistence.last_success` | `persistentcollection_persistence_last_success_seconds` | Gauge (observable) | `s` (Unix time) | `collection.type` | Time of the last successful write or truncate in this process. |
| `persistentcollection.errors` | `persistentcollection_errors_total` | Counter | `{error}` | `collection.type`, `operation`, `error.type` | Failed public operations. This includes failures that `TryDequeue`, `TryPop`, and `TryPeek` swallow. |
| `persistentcollection.items` | `persistentcollection_items` | UpDownCounter | `{item}` | `collection.type` | Items held in memory across all live instances. For queues and stacks this is the depth. |
| `persistentcollection.instances` | `persistentcollection_instances` | UpDownCounter | `{instance}` | `collection.type` | Instances that are open and not yet disposed. |
| `persistentcollection.build.info` | `persistentcollection_build_info` | Gauge (observable) | none | `version` | Always 1. Shows the library version loaded. |

\* `error.type` is present only when `outcome="error"`.

### Label values (all bounded)

- `collection.type`: `list`, `queue`, `stack`, `dictionary`
- `outcome`: `success`; `miss` (completed normally but the target was absent, for example `TryDequeue` on an empty queue, `Remove` of a missing item or key, or `TryGetValue` of a missing key); `error` (the operation threw)
- `error.type`: full exception type name, for example `System.InvalidOperationException`, `System.IO.IOException`, `System.UnauthorizedAccessException`, `System.Text.Json.JsonException`
- `operation`:

| Collection | Operations |
| --- | --- |
| all | `open` (constructor), `count`, `enumerate`, `contains`, `copy_to`, `clear` |
| queue | `enqueue`, `dequeue`, `peek`, `to_array`, `try_peek`, `try_dequeue` |
| stack | `push`, `pop`, `peek`, `to_array`, `try_peek`, `try_pop` |
| list | `add`, `insert`, `get`, `set`, `index_of`, `remove`, `remove_at` |
| dictionary | `add`, `get`, `set`, `remove`, `contains_key`, `try_get_value`, `keys`, `values` |

File paths, keys, values, and item contents **never** appear in metric labels.

### Notes

- `items` and `instances` are only accurate if you dispose your collections. A collection that is never disposed keeps counting. Note that `Dispose` clears in-memory state but leaves the file on disk.
- A miss is not an error. An idle consumer polling `TryDequeue` produces `outcome="miss"` and does not raise the error rate.
- If a persistence write fails during a mutation, the in-memory change has already been applied and `items` reflects it. The error shows on both the operation and the `write` step. This is existing library behavior that the telemetry now makes visible.

## Spans catalog

Spans are emitted only for operations that touch disk. Read-only, in-memory operations such as `count`, `peek`, `contains`, and `get` are metrics-only, so tight read loops don't flood your traces. All spans use `ActivityKind.Internal` and nest under `Activity.Current`. When a host span exists, such as Watson's request span or your own job span, they join its W3C trace automatically.

| Span name | Emitted by | Child spans |
| --- | --- | --- |
| `{Type} open` | constructor | `{Type} persistence.read` (if the file exists) |
| `PersistentQueue enqueue`, `dequeue`, `try_dequeue` | queue mutations | `PersistentQueue persistence.write` |
| `PersistentStack push`, `pop`, `try_pop` | stack mutations | `PersistentStack persistence.write` |
| `PersistentList add`, `insert`, `set`, `remove`, `remove_at` | list mutations | `PersistentList persistence.write` |
| `PersistentDictionary add`, `set`, `remove` | dictionary mutations | `PersistentDictionary persistence.write` |
| `{Type} clear` | `Clear()` | `{Type} persistence.truncate` |

`{Type}` is one of `PersistentList`, `PersistentQueue`, `PersistentStack`, `PersistentDictionary`.

| Attribute | On | Description |
| --- | --- | --- |
| `collection.type` | all | `list`, `queue`, `stack`, `dictionary` |
| `operation` | all | Operation or persistence step name |
| `outcome` | all | `success`, `miss`, `error` |
| `error.type` | failed spans | Exception type full name |
| `persistentcollection.lock_wait_seconds` | operation spans | Seconds spent waiting for the lock |
| `persistentcollection.item_count` | `open`, persistence spans | Item count after the step |
| `persistentcollection.bytes` | `persistence.read`, `persistence.write` | Bytes read or written |
| `file.name` | persistence spans | File name only, never the directory path |

Status is set explicitly: `Ok` on success and miss, `Error` on failure. A failure also carries an OpenTelemetry `exception` event with type, message, and stack trace. Exception messages for I/O errors can include the file path, so keep trace backends on an internal network.

## Recommended PromQL alerts

```promql
# Persistence is failing (disk full, permissions, file locked by another process)
sum by (collection_type, error_type) (rate(persistentcollection_persistence_duration_seconds_count{outcome="error"}[5m])) > 0

# A persistence file is corrupt (open fails to deserialize)
increase(persistentcollection_errors_total{operation="open", error_type="System.Text.Json.JsonException"}[15m]) > 0

# Mutations are slow (p95 over 250 ms): the file has grown too large, or the disk is slow
histogram_quantile(0.95, sum by (le, collection_type) (rate(persistentcollection_persistence_duration_seconds_bucket{operation="write"}[5m]))) > 0.25

# File size exceeds the README guidance (about 10 MB)
histogram_quantile(0.99, sum by (le, collection_type) (rate(persistentcollection_persistence_size_bytes_bucket{operation="write"}[15m]))) > 10e6

# Lock contention: p95 wait over 100 ms
histogram_quantile(0.95, sum by (le, collection_type, operation) (rate(persistentcollection_lock_wait_duration_seconds_bucket[5m]))) > 0.1

# Queue backlog is growing and not draining
persistentcollection_items{collection_type="queue"} > 1000 and deriv(persistentcollection_items{collection_type="queue"}[15m]) > 0
```

## Dashboard map

PersistentCollection is a library. It ships no compose stack and no Grafana dashboards of its own, because those belong to the host service. A host that embeds it should add a **PersistentCollection** row (or a domain dashboard) with these panels:

| Panel | Query |
| --- | --- |
| Operation rate by outcome | `sum by (collection_type, outcome) (rate(persistentcollection_operation_duration_seconds_count[5m]))` |
| Operation p95 by operation | `histogram_quantile(0.95, sum by (le, collection_type, operation) (rate(persistentcollection_operation_duration_seconds_bucket[5m])))` |
| Lock wait vs disk time (p95) | `lock_wait_duration` and `persistence_duration{operation="write"}` quantiles, side by side |
| File size p95 | `histogram_quantile(0.95, sum by (le, collection_type) (rate(persistentcollection_persistence_size_bytes_bucket{operation="write"}[5m])))` |
| Queue / stack depth | `sum by (collection_type) (persistentcollection_items)` |
| Errors by type | `sum by (collection_type, operation, error_type) (rate(persistentcollection_errors_total[5m]))` |
| Seconds since last successful write | `time() - persistentcollection_persistence_last_success_seconds` |
| Version | `persistentcollection_build_info` |

Runtime metrics (GC, thread pool, CPU) are the host's responsibility. Radiant and the OpenTelemetry runtime instrumentation both provide them.

## Verification

`src/Test.Shared/TelemetrySuite.cs` runs in every test host (Touchstone CLI, xUnit, NUnit). It uses an in-memory `MeterListener` and `ActivityListener` to verify:

- every collection type's operations, outcomes, and lock wait
- persistence read, write, and truncate, with sizes
- depth and instance accounting, span nesting, and joining an ambient W3C trace
- failure paths: an empty-collection error, a corrupt file on open, a disk write failure, and a swallowed `TryDequeue` failure
- the gauges, and that labels stay bounded
- that the no-listener path and a listener that throws never affect collection behavior
