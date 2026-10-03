namespace PersistentCollection
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Metrics;

    /// <summary>
    /// Times one collection operation or persistence step, tracks its outcome, and owns its span.
    /// Factories return null when nothing is listening, so callers use the null-conditional operator.
    /// Every member is best-effort and never throws. Not thread-safe; one scope belongs to one call.
    /// </summary>
    internal sealed class TelemetryScope : IDisposable
    {
        private static readonly double _TickToSeconds = 1.0 / Stopwatch.Frequency;

        private readonly Histogram<double> _Duration;
        private readonly string _CollectionType;
        private readonly string _Operation;
        private readonly bool _IsPersistence;
        private readonly long _StartTimestamp;
        private Activity _Activity;
        private string _Outcome = PersistentCollectionTelemetry.OutcomeSuccess;
        private string _ErrorType = null;
        private bool _Disposed = false;

        private TelemetryScope(
            Histogram<double> duration,
            string collectionType,
            string operation,
            bool isPersistence,
            Activity activity)
        {
            _Duration = duration;
            _CollectionType = collectionType;
            _Operation = operation;
            _IsPersistence = isPersistence;
            _Activity = activity;
            _StartTimestamp = Stopwatch.GetTimestamp();
        }

        internal static TelemetryScope Operation(string collectionType, string operation, string spanName)
        {
            try
            {
                bool metrics =
                    CollectionTelemetry.OperationDuration.Enabled
                    || CollectionTelemetry.LockWaitDuration.Enabled
                    || CollectionTelemetry.Errors.Enabled;

                Activity activity = null;
                if (spanName != null && CollectionTelemetry.Source.HasListeners())
                {
                    activity = CollectionTelemetry.Source.StartActivity(spanName, ActivityKind.Internal);
                    if (activity != null)
                    {
                        activity.SetTag(PersistentCollectionTelemetry.CollectionTypeKey, collectionType);
                        activity.SetTag(PersistentCollectionTelemetry.OperationKey, operation);
                    }
                }

                if (!metrics && activity == null) return null;
                return new TelemetryScope(CollectionTelemetry.OperationDuration, collectionType, operation, false, activity);
            }
            catch
            {
                return null;
            }
        }

        internal static TelemetryScope Persistence(string collectionType, string operation, string spanName, string fileName)
        {
            try
            {
                bool metrics =
                    CollectionTelemetry.PersistenceDuration.Enabled
                    || CollectionTelemetry.PersistenceSize.Enabled
                    || CollectionTelemetry.PersistenceLastSuccess.Enabled;

                Activity activity = null;
                if (CollectionTelemetry.Source.HasListeners())
                {
                    activity = CollectionTelemetry.Source.StartActivity(spanName, ActivityKind.Internal);
                    if (activity != null)
                    {
                        activity.SetTag(PersistentCollectionTelemetry.CollectionTypeKey, collectionType);
                        activity.SetTag(PersistentCollectionTelemetry.OperationKey, operation);
                        activity.SetTag(PersistentCollectionTelemetry.FileNameKey, fileName);
                    }
                }

                if (!metrics && activity == null) return null;
                return new TelemetryScope(CollectionTelemetry.PersistenceDuration, collectionType, operation, true, activity);
            }
            catch
            {
                return null;
            }
        }

        internal static bool Fail(TelemetryScope scope, Exception ex)
        {
            // Used as an exception filter; always returns false so the exception propagates unchanged.
            scope?.MarkFailed(ex);
            return false;
        }

        internal void MarkFailed(Exception ex)
        {
            try
            {
                _Outcome = PersistentCollectionTelemetry.OutcomeError;
                _ErrorType = ex != null ? ex.GetType().FullName : "unknown";

                if (_Activity != null)
                {
                    _Activity.SetStatus(ActivityStatusCode.Error, _ErrorType);
                    _Activity.SetTag(PersistentCollectionTelemetry.ErrorTypeKey, _ErrorType);
                    if (ex != null) _Activity.AddException(ex);
                }
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        internal void MarkMiss()
        {
            _Outcome = PersistentCollectionTelemetry.OutcomeMiss;
        }

        internal void LockAcquired()
        {
            try
            {
                double waited = (Stopwatch.GetTimestamp() - _StartTimestamp) * _TickToSeconds;

                if (CollectionTelemetry.LockWaitDuration.Enabled)
                {
                    CollectionTelemetry.LockWaitDuration.Record(
                        waited,
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.CollectionTypeKey, _CollectionType),
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.OperationKey, _Operation));
                }

                _Activity?.SetTag(PersistentCollectionTelemetry.LockWaitKey, waited);
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        internal void SetItemCount(int count)
        {
            try
            {
                _Activity?.SetTag(PersistentCollectionTelemetry.ItemCountKey, count);
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        internal void SetBytes(long bytes)
        {
            try
            {
                if (CollectionTelemetry.PersistenceSize.Enabled)
                {
                    CollectionTelemetry.PersistenceSize.Record(
                        bytes,
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.CollectionTypeKey, _CollectionType),
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.OperationKey, _Operation));
                }

                _Activity?.SetTag(PersistentCollectionTelemetry.BytesKey, bytes);
            }
            catch
            {
                // telemetry is best-effort
            }
        }

        public void Dispose()
        {
            if (_Disposed) return;
            _Disposed = true;

            try
            {
                double elapsed = (Stopwatch.GetTimestamp() - _StartTimestamp) * _TickToSeconds;

                if (_Duration.Enabled)
                {
                    TagList tags = new TagList();
                    tags.Add(PersistentCollectionTelemetry.CollectionTypeKey, _CollectionType);
                    tags.Add(PersistentCollectionTelemetry.OperationKey, _Operation);
                    tags.Add(PersistentCollectionTelemetry.OutcomeKey, _Outcome);
                    if (_ErrorType != null) tags.Add(PersistentCollectionTelemetry.ErrorTypeKey, _ErrorType);
                    _Duration.Record(elapsed, tags);
                }

                if (_ErrorType != null && !_IsPersistence && CollectionTelemetry.Errors.Enabled)
                {
                    CollectionTelemetry.Errors.Add(
                        1,
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.CollectionTypeKey, _CollectionType),
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.OperationKey, _Operation),
                        new KeyValuePair<string, object>(PersistentCollectionTelemetry.ErrorTypeKey, _ErrorType));
                }

                if (_IsPersistence
                    && _ErrorType == null
                    && !String.Equals(_Operation, PersistentCollectionTelemetry.PersistenceRead, StringComparison.Ordinal))
                {
                    CollectionTelemetry.MarkPersistenceSuccess(_CollectionType);
                }

                if (_Activity != null)
                {
                    _Activity.SetTag(PersistentCollectionTelemetry.OutcomeKey, _Outcome);
                    if (_ErrorType == null) _Activity.SetStatus(ActivityStatusCode.Ok);
                    _Activity.Dispose();
                    _Activity = null;
                }
            }
            catch
            {
                // telemetry is best-effort
            }
        }
    }
}
