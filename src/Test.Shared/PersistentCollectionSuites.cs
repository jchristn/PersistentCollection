namespace Test.Shared
{
    using System.Collections.Generic;
    using Touchstone.Core;

    /// <summary>
    /// Shared Touchstone suites exercising the PersistentCollection library end to end.
    /// These suites are the single source of truth for the automated CLI runner, the xUnit
    /// adapter, and the NUnit adapter.
    /// </summary>
    public static partial class PersistentCollectionSuites
    {
        /// <summary>
        /// All shared suites covering PersistentQueue, PersistentStack, PersistentList, and PersistentDictionary.
        /// </summary>
        public static IReadOnlyList<TestSuiteDescriptor> All
        {
            get
            {
                return new List<TestSuiteDescriptor>
                {
                    QueueSuite(),
                    StackSuite(),
                    ListSuite(),
                    DictionarySuite()
                };
            }
        }
    }
}
