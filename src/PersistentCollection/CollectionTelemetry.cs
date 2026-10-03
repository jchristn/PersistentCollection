namespace PersistentCollection
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Metrics;
    using System.Reflection;

    /// <summary>
    /// Process-wide Meter, ActivitySource, and instruments for PersistentCollection.
    /// Every recording method is best-effort: it never throws, and it does nothing when no listener is subscribed.
    /// </summary>
    internal static class CollectionTelemetry
    {
        private static readonly double[] _SecondsBuckets = new double[]
        {
            0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10
        };

        private static readonly long[] _BytesBuckets = new long[]
        {
            256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864
        };

        private static readonly ConcurrentDictionary<string, double> _LastSuccess = new ConcurrentDictionary<string, double>();

        internal static readonly string Version = ResolveVersion();

        internal static readonly ActivitySource Source = new ActivitySource(PersistentCollectionTelemetry.ActivitySourceName, Version);

        internal static readonly Meter Meter = new Meter(PersistentCollectionTelemetry.MeterName, Version);

        internal static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
            PersistentCollectionTelemetry.OperationDuration,
            "s",
            "Duration of each public collection operation, including lock wait and any disk persistence.",
            null,
            new InstrumentAdvice<double> { HistogramBucketBoundaries = _SecondsBuckets });

        internal static readonly Histogram<double> LockWaitDuration = Meter.CreateHistogram<double>(
            PersistentCollectionTelemetry.LockWaitDuration,
            "s",
            "Time spent waiting to acquire the per-instance lock before an operation could run.",
            null,
            new InstrumentAdvice<double> { HistogramBucketBoundaries = _SecondsBuckets });

        internal static readonly Histogram<double> PersistenceDuration = Meter.CreateHistogram<double>(
            PersistentCollectionTelemetry.PersistenceDuration,
            "s",
            "Duration of a disk persistence step (read, write, or truncate).",
            null,
            new InstrumentAdvice<double> { HistogramBucketBoundaries = _SecondsBuckets });

        internal static readonly Histogram<long> PersistenceSize = Meter.CreateHistogram<long>(
            PersistentCollectionTelemetry.PersistenceSize,
            "By",
            "Size of the persistence file read on open or written on mutation.",
            null,
            new InstrumentAdvice<long> { HistogramBucketBoundaries = _BytesBuckets });

        internal static readonly Counter<long> Errors = Meter.CreateCounter<long>(
            PersistentCollectionTelemetry.Errors,
            "{error}",
            "Failed public collection operations.");

        internal static readonly UpDownCounter<long> Items = Meter.CreateUpDownCounter<long>(
            PersistentCollectionTelemetry.Items,
            "{item}",
            "Items currently held in memory across all live collection instances.");

        internal static readonly UpDownCounter<long> Instances = Meter.CreateUpDownCounter<long>(
            PersistentCollectionTelemetry.Instances,
            "{instance}",
            "Live (opened and not yet disposed) collection instances.");

        internal static readonly ObservableGauge<double> PersistenceLastSuccess = Meter.CreateObservableGauge<double>(
            PersistentCollectionTelemetry.PersistenceLastSuccess,
            ObserveLastSuccess,
            "s",
            "Unix time of the most recent successful persistence write or truncate.");

        internal static readonly ObservableGauge<long> BuildInfo = Meter.CreateObservableGauge<long>(
            PersistentCollectionTelemetry.BuildInfo,
            ObserveBuildInfo,
            null,
            "Library version loaded in this process. Always 1.");

        internal static void AddItems(string collectionType, long delta)
        {
            if (delta == 0 || !Items.Enabled) return;

            try
            {
                Items.Add(delta, new KeyValuePair<string, object>(PersistentCollectionTelemetry.CollectionTypeKey, collectionType));
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        internal static void AddInstance(string collectionType, long delta)
        {
            if (!Instances.Enabled) return;

            try
            {
                Instances.Add(delta, new KeyValuePair<string, object>(PersistentCollectionTelemetry.CollectionTypeKey, collectionType));
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        internal static void MarkPersistenceSuccess(string collectionType)
        {
            try
            {
                double now = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
                _LastSuccess[collectionType] = now;
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        private static IEnumerable<Measurement<double>> ObserveLastSuccess()
        {
            List<Measurement<double>> measurements = new List<Measurement<double>>();

            foreach (KeyValuePair<string, double> entry in _LastSuccess)
            {
                measurements.Add(new Measurement<double>(
                    entry.Value,
                    new KeyValuePair<string, object>(PersistentCollectionTelemetry.CollectionTypeKey, entry.Key)));
            }

            return measurements;
        }

        private static Measurement<long> ObserveBuildInfo()
        {
            return new Measurement<long>(1, new KeyValuePair<string, object>(PersistentCollectionTelemetry.VersionKey, Version));
        }

        private static string ResolveVersion()
        {
            try
            {
                Assembly assembly = typeof(CollectionTelemetry).Assembly;
                AssemblyInformationalVersionAttribute info = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
                string version = info?.InformationalVersion;

                if (String.IsNullOrEmpty(version)) version = assembly.GetName().Version?.ToString();
                if (String.IsNullOrEmpty(version)) return "unknown";

                int plus = version.IndexOf('+');
                return plus > 0 ? version.Substring(0, plus) : version;
            }
            catch
            {
                return "unknown";
            }
        }
    }
}
