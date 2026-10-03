namespace Test.Shared
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// One metric measurement captured by <see cref="TelemetryCapture"/>.
    /// </summary>
    internal sealed class CapturedMeasurement
    {
        /// <summary>
        /// Instrument name.
        /// </summary>
        internal string Instrument { get; }

        /// <summary>
        /// Recorded value.
        /// </summary>
        internal double Value { get; }

        /// <summary>
        /// Tags, stringified.
        /// </summary>
        internal Dictionary<string, string> Tags { get; }

        /// <summary>
        /// Instantiate.
        /// </summary>
        /// <param name="instrument">Instrument name.</param>
        /// <param name="value">Value.</param>
        /// <param name="tags">Tags.</param>
        internal CapturedMeasurement(string instrument, double value, Dictionary<string, string> tags)
        {
            Instrument = instrument;
            Value = value;
            Tags = tags ?? new Dictionary<string, string>();
        }

        /// <summary>
        /// True when every supplied key/value pair is present in the tags.
        /// </summary>
        /// <param name="pairs">Alternating keys and values.</param>
        /// <returns>True if all match.</returns>
        internal bool Matches(string[] pairs)
        {
            if (pairs == null) return true;

            for (int i = 0; i + 1 < pairs.Length; i += 2)
            {
                string actual;
                if (!Tags.TryGetValue(pairs[i], out actual)) return false;
                if (!String.Equals(actual, pairs[i + 1], StringComparison.Ordinal)) return false;
            }

            return true;
        }

        /// <summary>
        /// True when the tags contain the key.
        /// </summary>
        /// <param name="key">Tag key.</param>
        /// <returns>True if present.</returns>
        internal bool Has(string key)
        {
            return Tags.ContainsKey(key);
        }
    }
}
