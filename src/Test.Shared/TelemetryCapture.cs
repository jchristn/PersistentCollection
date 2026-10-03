namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Metrics;
    using System.Linq;
    using PersistentCollection;

    /// <summary>
    /// In-memory listener that captures every metric measurement and completed span emitted by PersistentCollection
    /// while it is alive. Thread-safe for concurrent recording.
    /// </summary>
    internal sealed class TelemetryCapture : IDisposable
    {
        /// <summary>
        /// When true, every measurement callback throws, to prove instrumentation failures never reach the caller.
        /// </summary>
        internal bool ThrowOnMeasurement { get; set; } = false;

        private readonly object _Lock = new object();
        private readonly List<CapturedMeasurement> _Measurements = new List<CapturedMeasurement>();
        private readonly List<Activity> _Activities = new List<Activity>();
        private readonly MeterListener _MeterListener;
        private readonly ActivityListener _ActivityListener;

        /// <summary>
        /// Start capturing.
        /// </summary>
        internal TelemetryCapture()
        {
            _MeterListener = new MeterListener();
            _MeterListener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == PersistentCollectionTelemetry.MeterName) listener.EnableMeasurementEvents(instrument);
            };
            _MeterListener.SetMeasurementEventCallback<double>((instrument, value, tags, state) => Record(instrument, value, tags));
            _MeterListener.SetMeasurementEventCallback<long>((instrument, value, tags, state) => Record(instrument, value, tags));
            _MeterListener.Start();

            _ActivityListener = new ActivityListener();
            _ActivityListener.ShouldListenTo = source => source.Name == PersistentCollectionTelemetry.ActivitySourceName;
            _ActivityListener.Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded;
            _ActivityListener.ActivityStopped = activity =>
            {
                lock (_Lock) _Activities.Add(activity);
            };
            ActivitySource.AddActivityListener(_ActivityListener);
        }

        /// <summary>
        /// Snapshot of captured measurements.
        /// </summary>
        internal List<CapturedMeasurement> Measurements
        {
            get
            {
                lock (_Lock) return new List<CapturedMeasurement>(_Measurements);
            }
        }

        /// <summary>
        /// Snapshot of completed spans.
        /// </summary>
        internal List<Activity> Activities
        {
            get
            {
                lock (_Lock) return new List<Activity>(_Activities);
            }
        }

        /// <summary>
        /// Poll observable instruments (gauges) so their current values are captured.
        /// </summary>
        internal void CollectObservables()
        {
            _MeterListener.RecordObservableInstruments();
        }

        /// <summary>
        /// Measurements for one instrument whose tags contain every supplied key/value pair.
        /// </summary>
        /// <param name="instrument">Instrument name.</param>
        /// <param name="tags">Alternating tag keys and values to match.</param>
        /// <returns>Matching measurements.</returns>
        internal List<CapturedMeasurement> Find(string instrument, params string[] tags)
        {
            return Measurements.Where(m => m.Instrument == instrument && m.Matches(tags)).ToList();
        }

        /// <summary>
        /// Completed spans with a given name.
        /// </summary>
        /// <param name="name">Span display name.</param>
        /// <returns>Matching spans.</returns>
        internal List<Activity> Spans(string name)
        {
            return Activities.Where(a => a.DisplayName == name).ToList();
        }

        /// <summary>
        /// Stop capturing.
        /// </summary>
        public void Dispose()
        {
            _MeterListener.Dispose();
            _ActivityListener.Dispose();
        }

        private void Record<T>(Instrument instrument, T value, ReadOnlySpan<KeyValuePair<string, object>> tags)
        {
            Dictionary<string, string> copy = new Dictionary<string, string>();
            foreach (KeyValuePair<string, object> tag in tags) copy[tag.Key] = tag.Value?.ToString();

            lock (_Lock) _Measurements.Add(new CapturedMeasurement(instrument.Name, Convert.ToDouble(value), copy));

            if (ThrowOnMeasurement) throw new InvalidOperationException("Simulated listener failure.");
        }
    }
}
