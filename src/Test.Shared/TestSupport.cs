namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Shared helpers for the PersistentCollection Touchstone suites.
    /// </summary>
    internal static class TestSupport
    {
        /// <summary>
        /// Root directory beneath which every suite gets an isolated working directory.
        /// </summary>
        internal static readonly string BaseDirectory =
            Path.Combine(Path.GetTempPath(), "PersistentCollectionTests");

        /// <summary>
        /// Create a unique persistence file path within a suite's working directory.
        /// </summary>
        /// <param name="suite">Suite identifier.</param>
        /// <returns>Unique file path.</returns>
        internal static string NewFile(string suite)
        {
            string dir = Path.Combine(BaseDirectory, suite);
            Directory.CreateDirectory(dir);
            return Path.Combine(dir, Guid.NewGuid().ToString("N") + ".idx");
        }

        /// <summary>
        /// Produce a suite-scoped setup hook that clears the suite's working directory before it runs.
        /// </summary>
        /// <param name="suite">Suite identifier.</param>
        /// <returns>Before-suite delegate.</returns>
        internal static Func<CancellationToken, ValueTask> CleanSuite(string suite)
        {
            return _ =>
            {
                string dir = Path.Combine(BaseDirectory, suite);
                if (Directory.Exists(dir))
                {
                    try { Directory.Delete(dir, true); }
                    catch { /* best-effort cleanup */ }
                }

                Directory.CreateDirectory(dir);
                return default;
            };
        }

        /// <summary>
        /// Assert a condition, throwing a descriptive exception on failure.
        /// </summary>
        /// <param name="condition">Condition that must be true.</param>
        /// <param name="message">Failure message.</param>
        internal static void Assert(bool condition, string message)
        {
            if (!condition) throw new InvalidOperationException(message);
        }

        /// <summary>
        /// Assert two values are equal.
        /// </summary>
        /// <typeparam name="T">Value type.</typeparam>
        /// <param name="expected">Expected value.</param>
        /// <param name="actual">Actual value.</param>
        /// <param name="message">Failure message.</param>
        internal static void AssertEquals<T>(T expected, T actual, string message)
        {
            if (!EqualityComparer<T>.Default.Equals(expected, actual))
            {
                throw new InvalidOperationException(
                    message + " Expected: [" + expected + "], Actual: [" + actual + "].");
            }
        }

        /// <summary>
        /// Assert that two sequences are equal element by element and in order.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="expected">Expected sequence.</param>
        /// <param name="actual">Actual sequence.</param>
        /// <param name="message">Failure message.</param>
        internal static void AssertSequenceEquals<T>(IEnumerable<T> expected, IEnumerable<T> actual, string message)
        {
            List<T> expectedList = new List<T>(expected);
            List<T> actualList = new List<T>(actual);

            bool equal = expectedList.Count == actualList.Count;
            if (equal)
            {
                for (int i = 0; i < expectedList.Count; i++)
                {
                    if (!EqualityComparer<T>.Default.Equals(expectedList[i], actualList[i]))
                    {
                        equal = false;
                        break;
                    }
                }
            }

            if (!equal)
            {
                throw new InvalidOperationException(
                    message +
                    " Expected: [" + string.Join(", ", expectedList) + "]," +
                    " Actual: [" + string.Join(", ", actualList) + "].");
            }
        }

        /// <summary>
        /// Assert that an action throws a specific exception type.
        /// </summary>
        /// <typeparam name="T">Expected exception type.</typeparam>
        /// <param name="action">Action to run.</param>
        /// <param name="message">Failure message.</param>
        internal static void AssertThrows<T>(Action action, string message)
            where T : Exception
        {
            try
            {
                action();
            }
            catch (T)
            {
                return;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    message + " Threw wrong exception type: " + ex.GetType().Name + ".");
            }

            throw new InvalidOperationException(message + " No exception was thrown.");
        }
    }
}
