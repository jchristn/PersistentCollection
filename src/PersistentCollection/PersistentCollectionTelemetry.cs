namespace PersistentCollection
{
    /// <summary>
    /// Stable public names for the telemetry emitted by PersistentCollection.
    /// The library emits through the BCL System.Diagnostics.Metrics.Meter and System.Diagnostics.ActivitySource
    /// APIs only, and takes no dependency on any exporter. When no listener is subscribed, emission is effectively free.
    /// A host subscribes by name, for example with Radiant: settings.Sources.AddMeter(PersistentCollectionTelemetry.MeterName)
    /// and settings.Sources.AddActivitySource(PersistentCollectionTelemetry.ActivitySourceName), or with the OpenTelemetry SDK:
    /// AddMeter(PersistentCollectionTelemetry.MeterName) and AddSource(PersistentCollectionTelemetry.ActivitySourceName).
    /// These names, the metric names, and the attribute keys below are public contract. Dashboards and alerts depend on them.
    /// This class is thread-safe; it contains only constants.
    /// </summary>
    public static class PersistentCollectionTelemetry
    {
        #region Source-Names

        /// <summary>
        /// Name of the Meter that emits all PersistentCollection metrics.
        /// </summary>
        public const string MeterName = "PersistentCollection";

        /// <summary>
        /// Name of the ActivitySource that emits all PersistentCollection spans.
        /// </summary>
        public const string ActivitySourceName = "PersistentCollection";

        #endregion

        #region Metric-Names

        /// <summary>
        /// Histogram, unit seconds. Duration of each public collection operation, including lock wait and any disk persistence.
        /// Labels: collection.type, operation, outcome, and error.type (only when outcome is error).
        /// </summary>
        public const string OperationDuration = "persistentcollection.operation.duration";

        /// <summary>
        /// Histogram, unit seconds. Time spent waiting to acquire the per-instance lock before an operation could run.
        /// Labels: collection.type, operation.
        /// </summary>
        public const string LockWaitDuration = "persistentcollection.lock.wait.duration";

        /// <summary>
        /// Histogram, unit seconds. Duration of a disk persistence step (read on open, full rewrite on mutation, truncate on clear).
        /// Labels: collection.type, operation (read, write, truncate), outcome, and error.type (only when outcome is error).
        /// </summary>
        public const string PersistenceDuration = "persistentcollection.persistence.duration";

        /// <summary>
        /// Histogram, unit bytes. Size of the persistence file read on open or written on mutation.
        /// Labels: collection.type, operation (read, write).
        /// </summary>
        public const string PersistenceSize = "persistentcollection.persistence.size";

        /// <summary>
        /// Gauge, unit seconds (Unix time). Time of the most recent successful persistence write or truncate in this process.
        /// Labels: collection.type.
        /// </summary>
        public const string PersistenceLastSuccess = "persistentcollection.persistence.last_success";

        /// <summary>
        /// Counter, unit {error}. Failed public operations.
        /// Labels: collection.type, operation, error.type.
        /// </summary>
        public const string Errors = "persistentcollection.errors";

        /// <summary>
        /// UpDownCounter, unit {item}. Items currently held in memory across all live instances in this process (queue depth for queues).
        /// Labels: collection.type.
        /// </summary>
        public const string Items = "persistentcollection.items";

        /// <summary>
        /// UpDownCounter, unit {instance}. Live (opened and not yet disposed) collection instances in this process.
        /// Labels: collection.type.
        /// </summary>
        public const string Instances = "persistentcollection.instances";

        /// <summary>
        /// Gauge, always 1. Identifies the library version loaded in this process.
        /// Labels: version.
        /// </summary>
        public const string BuildInfo = "persistentcollection.build.info";

        #endregion

        #region Attribute-Keys

        /// <summary>
        /// Attribute key for the collection type. Values: list, queue, stack, dictionary.
        /// </summary>
        public const string CollectionTypeKey = "collection.type";

        /// <summary>
        /// Attribute key for the operation name, for example enqueue, dequeue, add, open, or (for persistence) read, write, truncate.
        /// </summary>
        public const string OperationKey = "operation";

        /// <summary>
        /// Attribute key for the operation outcome. Values: success, miss, error.
        /// </summary>
        public const string OutcomeKey = "outcome";

        /// <summary>
        /// Attribute key for the error type, set to the full name of the exception type when outcome is error.
        /// </summary>
        public const string ErrorTypeKey = "error.type";

        /// <summary>
        /// Attribute key for the library version on the build info gauge.
        /// </summary>
        public const string VersionKey = "version";

        /// <summary>
        /// Span-only attribute key for the persistence file name (file name only, never the full path).
        /// </summary>
        public const string FileNameKey = "file.name";

        /// <summary>
        /// Span-only attribute key for the item count after the operation or persistence step.
        /// </summary>
        public const string ItemCountKey = "persistentcollection.item_count";

        /// <summary>
        /// Span-only attribute key for the number of bytes read or written by a persistence step.
        /// </summary>
        public const string BytesKey = "persistentcollection.bytes";

        /// <summary>
        /// Span-only attribute key for the lock wait time in seconds before the operation could run.
        /// </summary>
        public const string LockWaitKey = "persistentcollection.lock_wait_seconds";

        #endregion

        #region Attribute-Values

        /// <summary>
        /// Collection type value for PersistentList.
        /// </summary>
        public const string CollectionTypeList = "list";

        /// <summary>
        /// Collection type value for PersistentQueue.
        /// </summary>
        public const string CollectionTypeQueue = "queue";

        /// <summary>
        /// Collection type value for PersistentStack.
        /// </summary>
        public const string CollectionTypeStack = "stack";

        /// <summary>
        /// Collection type value for PersistentDictionary.
        /// </summary>
        public const string CollectionTypeDictionary = "dictionary";

        /// <summary>
        /// Outcome value: the operation completed and did what was asked.
        /// </summary>
        public const string OutcomeSuccess = "success";

        /// <summary>
        /// Outcome value: the operation completed normally but its target was absent
        /// (for example TryDequeue on an empty queue, or Remove of a missing key).
        /// </summary>
        public const string OutcomeMiss = "miss";

        /// <summary>
        /// Outcome value: the operation threw an exception.
        /// </summary>
        public const string OutcomeError = "error";

        /// <summary>
        /// Persistence operation value: the persistence file was read and deserialized on open.
        /// </summary>
        public const string PersistenceRead = "read";

        /// <summary>
        /// Persistence operation value: the full collection was serialized and the persistence file rewritten.
        /// </summary>
        public const string PersistenceWrite = "write";

        /// <summary>
        /// Persistence operation value: the persistence file was truncated by Clear.
        /// </summary>
        public const string PersistenceTruncate = "truncate";

        #endregion
    }
}
