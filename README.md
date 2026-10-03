![alt tag](https://github.com/jchristn/PersistentCollection/blob/main/src/PersistentCollection/Assets/icon.png?raw=true)

# PersistentCollection

[![NuGet Version](https://img.shields.io/nuget/v/PersistentCollection.svg?style=flat)](https://www.nuget.org/packages/PersistentCollection/) [![NuGet](https://img.shields.io/nuget/dt/PersistentCollection.svg)](https://www.nuget.org/packages/PersistentCollection) 

Lightweight, persistent, thread-safe, disk-based collection classes written in C# for queue, stack, dictionary, and list.  All classes leverage a file to enable persistence across instantiations of the object or restarts of the software.

**IMPORTANT**:
- To provide persistence, the internal data structure is persisted to disk *in full* any time a change is made
- Thus, this library is NOT APPROPRIATE for large amounts of data or a large number of records
- It is recommended that these classes be used sparingly, when record counts are less than 1000 and total size is less than 10MB

## New in v2.2.x

- Built-in observability: metrics and traces via `System.Diagnostics` `Meter` and `ActivitySource`, both named `PersistentCollection` (no exporter dependency, near-zero cost when nothing is listening)
- Operation latency and outcome, lock wait, disk persistence latency and file size, queue/stack depth, and errors by type
- See [TELEMETRY.md](TELEMETRY.md) for the full metrics and spans catalog, subscription examples, and recommended alerts
- v2.2.1: dependency updates (`SerializationHelper` 2.1.0, `System.Text.Json` 10.0.12); no public API changes

## New in v2.0.x

- Remove expiration
- Migrate existing implementation to `PersistentList`, `PersistentQueue`, `PersistentDictionary`, and `PersistentStack`
- Rename package to `PersistentCollection`

## Getting Started

Refer to the ```Test``` project for a working example.

### PersistentList

`PersistentList` implements the interface of `System.Collections.Generic.List<T>`.

```csharp
using PersistentCollection;

PersistentList<string> myList = new PersistentList<string>("./list.idx"); 
myList.Add("foo");
myList.Add("bar");
string val = myList.Get(1);
myList.RemoveAt(1);
```

### PersistentQueue

`PersistentQueue` mimics the behavior of `System.Collections.Generic.Queue<T>`.

```csharp
using PersistentCollection;

PersistentQueue<string> myQueue = new PersistentQueue<string>("./queue.idx");
myQueue.Enqueue("foo");
myQueue.Enqueue("bar");
string val = myQueue.Dequeue(); // foo
```

### PersistentStack

`PersistentStack` mimics the behavior of `System.Collections.Generic.Stack<T>`.

```csharp
using PersistentCollection;

PersistentStack<string> myStack = new PersistentStack<string>("./stack.idx");
myStack.Push("foo");
myStack.Push("bar");
string val = myStack.Pop(); // bar
```

### PersistentDictionary

`PersistentDictionary` implements the interface of `System.Collections.Generic.IDictionary<TKey, TValue>`.

```csharp
using PersistentCollection;

PersistentDictionary<string, string> myDict = new PersistentDictionary<string, string>("./dict.idx");
myDict.Add("name", "Joel");
myDict.Add("hobbies", "code");
string val = myDict["name"]; // Joel
```

## Observability

PersistentCollection emits OpenTelemetry-compatible metrics and traces through the BCL `System.Diagnostics` APIs. It takes no dependency on any exporter. Subscribe your host's collector to the `PersistentCollection` meter and activity source, for example with Radiant:

```csharp
using PersistentCollection;

settings.Sources.AddMeter(PersistentCollectionTelemetry.MeterName);
settings.Sources.AddActivitySource(PersistentCollectionTelemetry.ActivitySourceName);
```

Or with the OpenTelemetry SDK: `.AddMeter(PersistentCollectionTelemetry.MeterName)` and `.AddSource(PersistentCollectionTelemetry.ActivitySourceName)`.

You then get, for example:

- `persistentcollection_operation_duration_seconds` per collection type, operation, and outcome
- `persistentcollection_persistence_duration_seconds` and `persistentcollection_persistence_size_bytes`, which show the cost of each full-file rewrite
- `persistentcollection_items` (queue and stack depth) and `persistentcollection_errors_total` by `error_type`
- spans such as `PersistentQueue enqueue` → `PersistentQueue persistence.write`, which nest under your request or job span

Refer to [TELEMETRY.md](TELEMETRY.md) for the complete catalog, PromQL alerts, and suggested dashboard panels.

## Version History

Refer to CHANGELOG.md for version history.
