namespace Test.Automated
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Test.Shared;
    using Touchstone.Cli;
    using Touchstone.Core;

    /// <summary>
    /// Touchstone console runner for the PersistentCollection shared suites.
    /// </summary>
    public static class Program
    {
        /// <summary>
        /// Run the shared Touchstone suites and return a process exit code.
        /// </summary>
        /// <param name="args">Command-line arguments. Optional: --suite &lt;name&gt; --test &lt;name&gt;.</param>
        /// <returns>Process exit code; non-zero if any test failed.</returns>
        public static async Task<int> Main(string[] args)
        {
            if (IsHelpRequested(args))
            {
                ShowHelp();
                return 0;
            }

            string suiteFilter = ReadOption(args, "suite");
            string testFilter = ReadOption(args, "test");

            IReadOnlyList<TestSuiteDescriptor> suites = FilterSuites(PersistentCollectionSuites.All, suiteFilter, testFilter);

            if (!String.IsNullOrWhiteSpace(suiteFilter) || !String.IsNullOrWhiteSpace(testFilter))
            {
                Console.WriteLine("Test filter: suite=" + (suiteFilter ?? "*") + " test=" + (testFilter ?? "*"));
                Console.WriteLine();
            }

            if (suites.Count == 0)
            {
                Console.Error.WriteLine("No suites or test cases matched the supplied filters.");
                return 1;
            }

            return await ConsoleRunner.RunAsync(suites).ConfigureAwait(false);
        }

        private static bool IsHelpRequested(string[] args)
        {
            if (args == null) return false;

            foreach (string argument in args)
            {
                string normalized = (argument ?? String.Empty).Trim().ToLowerInvariant();
                if (normalized == "--help" || normalized == "-?" || normalized == "-h") return true;
            }

            return false;
        }

        private static void ShowHelp()
        {
            Console.WriteLine("PersistentCollection Touchstone runner");
            Console.WriteLine();
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet run --project src/Test.Automated/Test.Automated.csproj");
            Console.WriteLine("  dotnet run --project src/Test.Automated/Test.Automated.csproj -- --suite queue");
            Console.WriteLine("  dotnet run --project src/Test.Automated/Test.Automated.csproj -- --suite list --test remove_at");
            Console.WriteLine();
            Console.WriteLine("Suites: queue, stack, list, dictionary");
        }

        private static string ReadOption(string[] args, string name)
        {
            if (args == null) return null;

            for (int i = 0; i < args.Length; i++)
            {
                string argument = (args[i] ?? String.Empty).Trim();
                string normalized = argument.TrimStart('-');

                int equalsIndex = normalized.IndexOf('=');
                if (equalsIndex >= 0)
                {
                    string key = normalized.Substring(0, equalsIndex);
                    if (String.Equals(key, name, StringComparison.OrdinalIgnoreCase))
                    {
                        return normalized.Substring(equalsIndex + 1);
                    }

                    continue;
                }

                if (String.Equals(normalized, name, StringComparison.OrdinalIgnoreCase))
                {
                    if (i + 1 < args.Length) return args[i + 1];
                    throw new ArgumentException("--" + name + " requires a value.");
                }
            }

            return null;
        }

        private static IReadOnlyList<TestSuiteDescriptor> FilterSuites(
            IReadOnlyList<TestSuiteDescriptor> suites,
            string suiteFilter,
            string testFilter)
        {
            List<TestSuiteDescriptor> filtered = new List<TestSuiteDescriptor>();

            foreach (TestSuiteDescriptor suite in suites)
            {
                if (!Matches(suite.SuiteId, suiteFilter)) continue;

                List<TestCaseDescriptor> cases = suite.Cases
                    .Where(testCase => Matches(testCase.CaseId, testFilter))
                    .ToList();

                if (cases.Count == 0) continue;

                filtered.Add(new TestSuiteDescriptor(
                    suite.SuiteId,
                    suite.DisplayName,
                    cases,
                    suite.BeforeSuiteAsync,
                    suite.AfterSuiteAsync));
            }

            return filtered;
        }

        private static bool Matches(string value, string filter)
        {
            if (String.IsNullOrWhiteSpace(filter)) return true;
            return value != null && value.IndexOf(filter, StringComparison.OrdinalIgnoreCase) >= 0;
        }
    }
}
